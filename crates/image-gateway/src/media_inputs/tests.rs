use super::*;

fn wav(frames: u32, sample_rate: u32) -> Vec<u8> {
    let size = frames * 2;
    let mut bytes = b"RIFF".to_vec();
    bytes.extend((36 + size).to_le_bytes());
    bytes.extend(b"WAVEfmt ");
    bytes.extend(16_u32.to_le_bytes());
    bytes.extend(1_u16.to_le_bytes()); // PCM
    bytes.extend(1_u16.to_le_bytes()); // mono
    bytes.extend(sample_rate.to_le_bytes());
    bytes.extend((sample_rate * 2).to_le_bytes());
    bytes.extend(2_u16.to_le_bytes());
    bytes.extend(16_u16.to_le_bytes());
    bytes.extend(b"data");
    bytes.extend(size.to_le_bytes());
    bytes.resize(bytes.len() + size as usize, 0);
    bytes
}

#[test]
fn wav_is_measured_by_decoded_samples_with_ceil_milliseconds() {
    let metadata = probe_input(&wav(16_001, 16_000), MediaInputKind::Audio).unwrap();
    assert_eq!(metadata.media_type(), "audio/wav");
    assert_eq!(metadata.duration_ms(), Some(1001));
    assert_eq!(metadata.dimensions(), None);
}

#[test]
fn wav_rejects_truncation_partial_frames_and_false_declared_duration() {
    let bytes = wav(16_000, 16_000);
    assert!(probe_input(&bytes[..bytes.len() - 1], MediaInputKind::Audio).is_err());
    let mut partial = bytes.clone();
    partial[40..44].copy_from_slice(&31_999_u32.to_le_bytes());
    assert!(probe_input(&partial, MediaInputKind::Audio).is_err());
    let mut truncated = bytes;
    truncated[4..8].copy_from_slice(&36_036_u32.to_le_bytes());
    truncated[40..44].copy_from_slice(&36_000_u32.to_le_bytes());
    assert!(probe_input(&truncated, MediaInputKind::Audio).is_err());
    assert!(probe_input(&wav(0, 16_000), MediaInputKind::Audio).is_err());
}

#[test]
fn mp3_decodes_both_gapless_and_vbr_without_duration_metadata() {
    let metadata = probe_input(include_bytes!("fixtures/tone.mp3"), MediaInputKind::Audio).unwrap();
    assert_eq!(metadata.media_type(), "audio/mpeg");
    assert_eq!(metadata.duration_ms(), Some(2000));
    let metadata = probe_input(
        include_bytes!("fixtures/tone-vbr-no-xing.mp3"),
        MediaInputKind::Audio,
    )
    .unwrap();
    assert!(matches!(metadata.duration_ms(), Some(2000..=2200)));
}

#[test]
fn mp3_does_not_silently_accept_partial_tail_or_resynchronize_past_damage() {
    let bytes = include_bytes!("fixtures/tone.mp3");
    for missing in [1, 10, 50] {
        assert!(probe_input(&bytes[..bytes.len() - missing], MediaInputKind::Audio).is_err());
    }
    let mut damaged = bytes.to_vec();
    damaged.splice(3000..3000, [0x12, 0x34, 0x56, 0x78]);
    assert!(probe_input(&damaged, MediaInputKind::Audio).is_err());
    let mut tagged = bytes.to_vec();
    tagged.extend(b"TAG");
    tagged.extend([0; 125]);
    assert_eq!(
        probe_input(&tagged, MediaInputKind::Audio)
            .unwrap()
            .duration_ms(),
        Some(2000)
    );
}

#[test]
fn mp4_uses_video_samples_and_rejects_missing_data_or_wrong_track_kind() {
    let bytes = include_bytes!("fixtures/blue.mp4");
    let metadata = probe_input(bytes, MediaInputKind::Video).unwrap();
    assert_eq!(metadata.duration_ms(), Some(2000));
    assert_eq!(metadata.dimensions(), Some((16, 16)));
    assert!(probe_input(&bytes[..bytes.len() - 1], MediaInputKind::Video).is_err());
    let mut wrong_track = bytes.to_vec();
    let vide = wrong_track.windows(4).position(|b| b == b"vide").unwrap();
    wrong_track[vide..vide + 4].copy_from_slice(b"soun");
    assert!(probe_input(&wrong_track, MediaInputKind::Video).is_err());
    let mut wrong_offset = bytes.to_vec();
    let stco = wrong_offset.windows(4).position(|b| b == b"stco").unwrap();
    wrong_offset[stco + 12..stco + 16].copy_from_slice(&8_u32.to_be_bytes());
    assert!(probe_input(&wrong_offset, MediaInputKind::Video).is_err());
    let mut wrong_count = bytes.to_vec();
    let stts = wrong_count.windows(4).position(|b| b == b"stts").unwrap();
    wrong_count[stts + 12..stts + 16].copy_from_slice(&1_u32.to_be_bytes());
    assert!(probe_input(&wrong_count, MediaInputKind::Video).is_err());
    let mut long_edit = bytes.to_vec();
    let elst = long_edit.windows(4).position(|b| b == b"elst").unwrap();
    long_edit[elst + 12..elst + 16].copy_from_slice(&90_000_u32.to_be_bytes());
    assert!(probe_input(&long_edit, MediaInputKind::Video).is_err());
}

#[test]
fn hevc_and_vp9_are_explicitly_unsupported_input_codecs() {
    for track in [
        mp4::TrackConfig::from(mp4::HevcConfig {
            width: 16,
            height: 16,
        }),
        mp4::TrackConfig::from(mp4::Vp9Config {
            width: 16,
            height: 16,
        }),
    ] {
        let config = mp4::Mp4Config {
            major_brand: "isom".parse().unwrap(),
            minor_version: 0,
            compatible_brands: vec!["isom".parse().unwrap()],
            timescale: 1000,
        };
        let mut writer = mp4::Mp4Writer::write_start(Cursor::new(Vec::new()), &config).unwrap();
        writer.add_track(&track).unwrap();
        writer
            .write_sample(
                1,
                &mp4::Mp4Sample {
                    start_time: 0,
                    duration: 1000,
                    rendering_offset: 0,
                    is_sync: true,
                    bytes: vec![1, 2, 3, 4].into(),
                },
            )
            .unwrap();
        writer.write_end().unwrap();
        assert_eq!(
            probe_input(&writer.into_writer().into_inner(), MediaInputKind::Video),
            Err(MediaInputError::Unsupported)
        );
    }
}

#[test]
fn mp4_container_duration_includes_audio_and_preserves_normal_aac_priming() {
    assert_eq!(
        probe_input(
            include_bytes!("fixtures/blue-audio-2s.mp4"),
            MediaInputKind::Video
        )
        .unwrap()
        .duration_ms(),
        Some(2000)
    );
    assert_eq!(
        probe_input(
            include_bytes!("fixtures/blue-audio-5s.mp4"),
            MediaInputKind::Video
        )
        .unwrap()
        .duration_ms(),
        Some(5000)
    );
    assert_eq!(
        probe_input(
            include_bytes!("fixtures/blue-audio-60s.mp4"),
            MediaInputKind::Video
        ),
        Err(MediaInputError::LimitExceeded)
    );
}

#[test]
fn mp4_short_movie_header_cannot_hide_a_longer_track_or_presentation_offset() {
    let mut hidden_audio = include_bytes!("fixtures/blue-audio-60s.mp4").to_vec();
    let mvhd = hidden_audio
        .windows(4)
        .position(|bytes| bytes == b"mvhd")
        .unwrap();
    assert_eq!(hidden_audio[mvhd + 4], 0);
    let scale = u32::from_be_bytes(hidden_audio[mvhd + 16..mvhd + 20].try_into().unwrap());
    hidden_audio[mvhd + 20..mvhd + 24].copy_from_slice(&(2 * scale).to_be_bytes());
    assert!(probe_input(&hidden_audio, MediaInputKind::Video).is_err());

    let mut hidden_offset = include_bytes!("fixtures/blue.mp4").to_vec();
    let ctts = hidden_offset
        .windows(4)
        .position(|bytes| bytes == b"ctts")
        .unwrap();
    hidden_offset[ctts + 16..ctts + 20].copy_from_slice(&i32::MAX.to_be_bytes());
    assert!(probe_input(&hidden_offset, MediaInputKind::Video).is_err());
}

#[test]
fn input_limits_and_wrong_media_kind_fail_closed() {
    assert_eq!(
        probe_input(&wav(480_001, 16_000), MediaInputKind::Audio),
        Err(MediaInputError::LimitExceeded)
    );
    assert!(probe_input(include_bytes!("fixtures/blue.mp4"), MediaInputKind::Audio).is_err());
    assert!(probe_input(include_bytes!("fixtures/tone.mp3"), MediaInputKind::Video).is_err());
    assert!(probe_input(b"ID3", MediaInputKind::Audio).is_err());
    assert!(probe_input(b"", MediaInputKind::Image).is_err());
    assert_eq!(
        probe_input(
            &vec![0; MediaInputKind::Audio.max_bytes() as usize + 1],
            MediaInputKind::Audio
        ),
        Err(MediaInputError::LimitExceeded)
    );
}

#[test]
fn images_require_decodable_pixels_and_report_real_dimensions() {
    let mut bytes = Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(2, 3)
        .write_to(&mut bytes, image::ImageFormat::Png)
        .unwrap();
    let metadata = probe_input(bytes.get_ref(), MediaInputKind::Image).unwrap();
    assert_eq!(metadata.dimensions(), Some((2, 3)));
    let bytes = bytes.into_inner();
    assert!(probe_input(&bytes[..33], MediaInputKind::Image).is_err());
}
