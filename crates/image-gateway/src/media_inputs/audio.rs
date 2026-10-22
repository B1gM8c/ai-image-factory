use std::io::Cursor;

use symphonia::core::{
    codecs::audio::{AudioDecoderOptions, well_known::CODEC_ID_MP3},
    common::Limit,
    formats::{
        FormatOptions, FormatReader,
        probe::Hint,
        well_known::{FORMAT_ID_MP3, FORMAT_ID_WAVE},
    },
    io::{MediaSourceStream, ReadBytes, ReadOnlySource},
    meta::{MetadataOptions, MetadataReader},
};

use super::{
    MAX_TIMED_INPUT_DURATION_MS, MediaInputError as Error, MediaInputMetadata, duration_ms,
};

const MAX_PACKETS: usize = 20_000;
const MAX_PREFIX_BYTES: usize = 1024 * 1024;
const MAX_PACKET_BYTES: usize = 1024 * 1024;

fn metadata_options() -> MetadataOptions {
    MetadataOptions::default()
        .limit_tag_bytes(Limit::Maximum(0))
        .limit_visual_bytes(Limit::Maximum(0))
}

fn stream(bytes: &[u8]) -> MediaSourceStream<'_> {
    // Disabling seek avoids the MP3 reader's bitrate-based duration estimate: VBR without
    // Xing metadata must be measured by actual decoded frames, never estimated and trimmed.
    MediaSourceStream::new(
        Box::new(ReadOnlySource::new(Cursor::new(bytes))),
        Default::default(),
    )
}

fn open(bytes: &[u8]) -> Result<Box<dyn FormatReader + '_>, Error> {
    symphonia::default::get_probe()
        .probe(
            &Hint::new(),
            stream(bytes),
            FormatOptions::default(),
            metadata_options(),
        )
        .map_err(|_| Error::Invalid)
}

pub(super) fn probe(bytes: &[u8]) -> Result<MediaInputMetadata, Error> {
    // Get the parser's first packet boundary, then reopen. This small header-only pass lets
    // us reject MP3 resynchronization/skipped bytes and truncated final frames that its
    // streaming reader would otherwise silently treat as EOF. No custom MPEG parser.
    let mut offset =
        usize::try_from(open(bytes)?.into_inner().pos()).map_err(|_| Error::Invalid)?;
    if offset > MAX_PREFIX_BYTES {
        return Err(Error::LimitExceeded);
    }
    let mut format = open(bytes)?;
    let is_wav = format.format_info().format == FORMAT_ID_WAVE;
    if !is_wav && format.format_info().format != FORMAT_ID_MP3 {
        return Err(Error::Unsupported);
    }
    if format.tracks().len() != 1 {
        return Err(Error::Unsupported);
    }
    let track = &format.tracks()[0];
    let params = track
        .codec_params
        .as_ref()
        .and_then(|p| p.audio())
        .ok_or(Error::Unsupported)?;
    if !is_wav && params.codec != CODEC_ID_MP3 {
        return Err(Error::Unsupported);
    }
    let sample_rate = params
        .sample_rate
        .filter(|rate| (8_000..=192_000).contains(rate))
        .ok_or(Error::Unsupported)?;
    let channels = params
        .channels
        .as_ref()
        .map(|channels| channels.count())
        .filter(|n| (1..=8).contains(n))
        .ok_or(Error::Unsupported)?;
    let track_id = track.id;
    let expected_frames = track.num_frames;
    let mut decoder = symphonia::default::get_codecs()
        .make_audio_decoder(params, &AudioDecoderOptions::default().verify(true))
        .map_err(|_| Error::Unsupported)?;
    let data_end = if is_wav {
        // Use the library's data offset; enforce the finite RIFF/data envelopes which the
        // streaming reader intentionally permits to be open-ended or partly consumed.
        if bytes.get(..4) != Some(b"RIFF")
            || bytes.get(8..12) != Some(b"WAVE")
            || offset < 8
            || bytes.get(offset - 8..offset - 4) != Some(b"data")
        {
            return Err(Error::Invalid);
        }
        let riff_len = u32::from_le_bytes(
            bytes
                .get(4..8)
                .ok_or(Error::Invalid)?
                .try_into()
                .map_err(|_| Error::Invalid)?,
        ) as usize;
        let data_len = u32::from_le_bytes(
            bytes[offset - 4..offset]
                .try_into()
                .map_err(|_| Error::Invalid)?,
        ) as usize;
        let end = offset.checked_add(data_len).ok_or(Error::Invalid)?;
        if riff_len.checked_add(8) != Some(bytes.len())
            || end.checked_add(data_len % 2) != Some(bytes.len())
        {
            return Err(Error::Invalid);
        }
        end
    } else {
        bytes.len()
    };
    let mut frames = 0_u64;
    let mut untrimmed_frames = 0_u64;
    let mut packets = 0;
    while let Some(packet) = format.next_packet().map_err(|_| Error::Invalid)? {
        packets += 1;
        if packets > MAX_PACKETS || packet.data.len() > MAX_PACKET_BYTES {
            return Err(Error::LimitExceeded);
        }
        let end = offset
            .checked_add(packet.data.len())
            .filter(|end| *end <= data_end)
            .ok_or(Error::Invalid)?;
        if packet.track_id != track_id
            || packet.data.is_empty()
            || bytes.get(offset..end) != Some(packet.data.as_ref())
        {
            return Err(Error::Invalid);
        }
        offset = end;
        untrimmed_frames = untrimmed_frames
            .checked_add(packet.block_dur().get())
            .ok_or(Error::LimitExceeded)?;
        // A forged gapless tag must not hide arbitrarily long decode work. One second covers
        // encoder delay/padding; the measured audible duration still has the strict 30s cap.
        if u128::from(untrimmed_frames) * 1_000
            > u128::from(sample_rate) * u128::from(MAX_TIMED_INPUT_DURATION_MS + 1_000)
        {
            return Err(Error::LimitExceeded);
        }
        let decoded = decoder.decode(&packet).map_err(|_| Error::Invalid)?;
        if decoded.spec().rate() != sample_rate || decoded.spec().channels().count() != channels {
            return Err(Error::Invalid);
        }
        frames = frames
            .checked_add(decoded.frames() as u64)
            .ok_or(Error::LimitExceeded)?;
        // A first delay-only MP3 packet may legitimately decode to zero audible frames.
        if frames > 0 {
            duration_ms(frames, sample_rate)?;
        }
    }
    if is_wav {
        if offset != data_end {
            return Err(Error::Invalid);
        }
    } else if offset != bytes.len() {
        let tail = &bytes[offset..];
        if tail.len() != 128 || !tail.starts_with(b"TAG") {
            return Err(Error::Invalid);
        }
        let mut tag =
            symphonia::default::meta::Id3v1Reader::try_new(stream(tail), metadata_options())
                .map_err(|_| Error::Invalid)?;
        tag.read_all().map_err(|_| Error::Invalid)?;
    }
    if expected_frames.is_some_and(|expected| expected != frames)
        || decoder.finalize().verify_ok == Some(false)
    {
        return Err(Error::Invalid);
    }
    Ok(MediaInputMetadata {
        media_type: if is_wav { "audio/wav" } else { "audio/mpeg" },
        dimensions: None,
        duration_ms: Some(duration_ms(frames, sample_rate)?),
    })
}
