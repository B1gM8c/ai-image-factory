use std::io::Cursor;

use super::{MediaInputError as Error, MediaInputMetadata, duration_ms};

const MAX_BOXES: usize = 1_024;
const MAX_METADATA_BYTES: u64 = 8 * 1024 * 1024;
const MAX_SAMPLES: u32 = 18_000; // Thirty seconds at up to 600 encoded frames per second.

pub(super) fn probe(bytes: &[u8]) -> Result<MediaInputMetadata, Error> {
    let media_ranges = media_ranges(bytes)?;
    let reader = mp4::Mp4Reader::read_header(Cursor::new(bytes), bytes.len() as u64)
        .map_err(|_| Error::Invalid)?;
    if reader.is_fragmented() {
        return Err(Error::Unsupported);
    }
    if reader.tracks().len() > 8 {
        return Err(Error::LimitExceeded);
    }
    let mut videos = reader
        .tracks()
        .values()
        .filter(|track| matches!(track.track_type(), Ok(mp4::TrackType::Video)));
    let track = videos.next().ok_or(Error::Invalid)?;
    if videos.next().is_some() {
        return Err(Error::Unsupported);
    }
    // The existing parser only reads a single hvcC byte for HEVC. Keep the accepted input
    // subset explicit rather than treating a recognized codec label as validated metadata.
    if !matches!(track.media_type(), Ok(mp4::MediaType::H264)) {
        return Err(Error::Unsupported);
    }
    if track.width() == 0 || track.height() == 0 {
        return Err(Error::Invalid);
    }
    let stbl = &track.trak.mdia.minf.stbl;
    let avc = stbl.stsd.avc1.as_ref().ok_or(Error::Unsupported)?;
    if avc.data_reference_index != 1
        || avc.avcc.configuration_version != 1
        || avc.avcc.length_size_minus_one & 3 == 2
        || avc.avcc.sequence_parameter_sets.is_empty()
        || avc.avcc.picture_parameter_sets.is_empty()
        || avc
            .avcc
            .sequence_parameter_sets
            .iter()
            .any(|nal| nal.bytes.is_empty())
        || avc
            .avcc
            .picture_parameter_sets
            .iter()
            .any(|nal| nal.bytes.is_empty())
    {
        return Err(Error::Invalid);
    }
    let movie_scale = reader.moov.mvhd.timescale;
    let movie_duration = duration_ms(reader.moov.mvhd.duration, movie_scale)?;
    let mut duration = movie_duration;
    for related in reader.tracks().values() {
        // Unknown handlers/codecs cannot silently hide a longer presentation.
        match (related.track_type(), related.media_type()) {
            (Ok(mp4::TrackType::Video), Ok(mp4::MediaType::H264))
            | (Ok(mp4::TrackType::Audio), Ok(mp4::MediaType::AAC))
            | (Ok(mp4::TrackType::Subtitle), Ok(mp4::MediaType::TTXT)) => {}
            _ => return Err(Error::Unsupported),
        }
        let track_duration = track_duration_ms(related, movie_scale, &media_ranges)?;
        // Different timescales may round by one millisecond. A shorter forged movie
        // header must never conceal a longer audio, subtitle or video presentation.
        if movie_duration.saturating_add(1) < track_duration {
            return Err(Error::Invalid);
        }
        duration = duration.max(track_duration);
    }
    Ok(MediaInputMetadata {
        media_type: "video/mp4",
        dimensions: Some((u32::from(track.width()), u32::from(track.height()))),
        duration_ms: Some(duration),
    })
}

fn track_duration_ms(
    track: &mp4::Mp4Track,
    movie_scale: u32,
    media_ranges: &[(u64, u64)],
) -> Result<u64, Error> {
    let stbl = &track.trak.mdia.minf.stbl;
    let count = stbl.stsz.sample_count;
    if count == 0 {
        return Err(Error::Invalid);
    }
    if count > MAX_SAMPLES {
        return Err(Error::LimitExceeded);
    }
    if stbl.stsz.sample_size == 0 && stbl.stsz.sample_sizes.len() != count as usize {
        return Err(Error::Invalid);
    }
    let mut timed_samples = 0_u64;
    let mut units = 0_u64;
    for entry in &stbl.stts.entries {
        if entry.sample_count == 0 || entry.sample_delta == 0 {
            return Err(Error::Invalid);
        }
        timed_samples = timed_samples
            .checked_add(u64::from(entry.sample_count))
            .ok_or(Error::Invalid)?;
        units = units
            .checked_add(u64::from(entry.sample_count) * u64::from(entry.sample_delta))
            .ok_or(Error::Invalid)?;
    }
    if timed_samples != u64::from(count) || units != track.trak.mdia.mdhd.duration {
        return Err(Error::Invalid);
    }
    if let Some(ctts) = &stbl.ctts {
        let n: u64 = ctts.entries.iter().map(|e| u64::from(e.sample_count)).sum();
        if ctts.version > 1
            || n != u64::from(count)
            || ctts.entries.iter().any(|e| e.sample_count == 0)
        {
            return Err(Error::Invalid);
        }
        if ctts.entries.iter().any(|entry| {
            let offset = if ctts.version == 0 {
                i64::from(entry.sample_offset as u32)
            } else {
                i64::from(entry.sample_offset)
            };
            offset.unsigned_abs() > units
        }) {
            return Err(Error::Unsupported);
        }
    }
    if let Some(stss) = &stbl.stss {
        if stss.entries.is_empty()
            || stss.entries.iter().any(|n| *n == 0 || *n > count)
            || stss.entries.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(Error::Invalid);
        }
    }
    let offsets: Vec<u64> = match (&stbl.stco, &stbl.co64) {
        (Some(stco), None) => stco.entries.iter().map(|n| u64::from(*n)).collect(),
        (None, Some(co64)) => co64.entries.clone(),
        _ => return Err(Error::Invalid),
    };
    if offsets.is_empty() || offsets.len() > count as usize {
        return Err(Error::Invalid);
    }
    let entries = &stbl.stsc.entries;
    if entries.first().is_none_or(|entry| entry.first_chunk != 1)
        || entries.iter().any(|entry| {
            entry.samples_per_chunk == 0
                || entry.sample_description_index != 1
                || entry.first_chunk as usize > offsets.len()
        })
        || entries
            .windows(2)
            .any(|pair| pair[0].first_chunk >= pair[1].first_chunk)
    {
        return Err(Error::Invalid);
    }
    // Walk each table once. Calling Mp4Reader::read_sample for every sample would repeatedly
    // sum earlier samples within its chunk, allocate a packet copy, and trust malformed tables.
    let mut sample_index = 0_usize;
    let mut entry_index = 0;
    let mut extents = Vec::with_capacity(offsets.len());
    for (chunk_index, offset) in offsets.into_iter().enumerate() {
        while entry_index + 1 < entries.len()
            && entries[entry_index + 1].first_chunk as usize <= chunk_index + 1
        {
            entry_index += 1;
        }
        let chunk_samples = entries[entry_index].samples_per_chunk as usize;
        if sample_index
            .checked_add(chunk_samples)
            .is_none_or(|end| end > count as usize)
        {
            return Err(Error::Invalid);
        }
        let mut end = offset;
        for _ in 0..chunk_samples {
            let size = if stbl.stsz.sample_size > 0 {
                stbl.stsz.sample_size
            } else {
                stbl.stsz.sample_sizes[sample_index]
            };
            if size == 0 {
                return Err(Error::Invalid);
            }
            end = end.checked_add(u64::from(size)).ok_or(Error::Invalid)?;
            sample_index += 1;
        }
        if !media_ranges
            .iter()
            .any(|(start, limit)| offset >= *start && end <= *limit)
        {
            return Err(Error::Invalid);
        }
        extents.push((offset, end));
    }
    if sample_index != count as usize {
        return Err(Error::Invalid);
    }
    extents.sort_unstable();
    if extents.windows(2).any(|pair| pair[0].1 > pair[1].0) {
        return Err(Error::Invalid);
    }
    presentation_duration_ms(track, movie_scale)
}

fn presentation_duration_ms(track: &mp4::Mp4Track, movie_scale: u32) -> Result<u64, Error> {
    let stbl = &track.trak.mdia.minf.stbl;
    let media_scale = track.trak.mdia.mdhd.timescale;
    if media_scale == 0 {
        return Err(Error::Invalid);
    }
    let mut composition_offsets = stbl.ctts.as_ref().map(|ctts| {
        ctts.entries.iter().flat_map(move |entry| {
            let offset = if ctts.version == 0 {
                i64::from(entry.sample_offset as u32)
            } else {
                i64::from(entry.sample_offset)
            };
            std::iter::repeat_n(offset, entry.sample_count as usize)
        })
    });
    let mut dts = 0_i128;
    let mut first_pts = i128::MAX;
    let mut last_end = i128::MIN;
    for entry in &stbl.stts.entries {
        for _ in 0..entry.sample_count {
            let offset = match composition_offsets.as_mut() {
                Some(offsets) => offsets.next().ok_or(Error::Invalid)?,
                None => 0,
            };
            let pts = dts + i128::from(offset);
            first_pts = first_pts.min(pts);
            last_end = last_end.max(pts + i128::from(entry.sample_delta));
            dts += i128::from(entry.sample_delta);
        }
    }
    let playable = if let Some(edits) = track.trak.edts.as_ref().and_then(|edts| edts.elst.as_ref())
    {
        // One normal-rate edit covers common AVC reordering and AAC encoder priming.
        // Preserve its actual playable duration rather than counting discarded padding.
        if edits.version > 1 || edits.entries.len() != 1 {
            return Err(Error::Unsupported);
        }
        let edit = &edits.entries[0];
        if edit.media_rate != 1 || edit.media_rate_fraction != 0 {
            return Err(Error::Unsupported);
        }
        let duration = duration_ms(edit.segment_duration, movie_scale)?;
        let start = i128::from(edit.media_time);
        let span = (u128::from(edit.segment_duration) * u128::from(media_scale))
            .div_ceil(u128::from(movie_scale));
        let rounding = u128::from(media_scale).div_ceil(u128::from(movie_scale));
        if start < first_pts || start + span as i128 > last_end + rounding as i128 {
            return Err(Error::Invalid);
        }
        duration
    } else {
        duration_ms(
            u64::try_from(last_end).map_err(|_| Error::Invalid)?,
            media_scale,
        )?
    };
    let declared = duration_ms(track.trak.tkhd.duration, movie_scale)?;
    if declared.saturating_add(1) < playable {
        return Err(Error::Invalid);
    }
    Ok(declared.max(playable))
}

fn media_ranges(bytes: &[u8]) -> Result<Vec<(u64, u64)>, Error> {
    let mut cursor = Cursor::new(bytes);
    let mut ranges = Vec::new();
    let mut boxes = 0;
    let mut metadata_bytes = 0_u64;
    let mut ftyp = 0;
    let mut moov = 0;
    while cursor.position() < bytes.len() as u64 {
        boxes += 1;
        if boxes > MAX_BOXES {
            return Err(Error::LimitExceeded);
        }
        let start = cursor.position();
        let header = mp4::BoxHeader::read(&mut cursor).map_err(|_| Error::Invalid)?;
        // mp4 0.14 normalizes large-size headers to an eight-byte header in BoxHeader::size.
        // Open-ended boxes are deliberately unsupported for immutable uploaded inputs.
        let payload = cursor.position();
        let end = payload
            .checked_add(header.size.checked_sub(8).ok_or(Error::Invalid)?)
            .filter(|end| *end <= bytes.len() as u64)
            .ok_or(Error::Invalid)?;
        if header.name == mp4::BoxType::MdatBox {
            ranges.push((payload, end));
        } else {
            metadata_bytes = metadata_bytes
                .checked_add(end - start)
                .ok_or(Error::LimitExceeded)?;
            if metadata_bytes > MAX_METADATA_BYTES {
                return Err(Error::LimitExceeded);
            }
            if header.name == mp4::BoxType::FtypBox {
                ftyp += 1;
            }
            if header.name == mp4::BoxType::MoovBox {
                moov += 1;
            }
        }
        cursor.set_position(end);
    }
    if ftyp != 1 || moov != 1 || ranges.is_empty() {
        return Err(Error::Invalid);
    }
    Ok(ranges)
}
