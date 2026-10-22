These are generated test signals, with no third-party media. Runtime and tests do not invoke FFmpeg.

Generated with FFmpeg from `sine=frequency=440:sample_rate=16000:duration=2`:
- `tone.mp3`: mono, libmp3lame, 32k bitrate, standard Xing/LAME metadata.
- `tone-vbr-no-xing.mp3`: mono, libmp3lame, quality 6, `-write_xing 0`.

`blue.mp4`: `color=c=blue:s=16x16:r=10:d=2`, libx264, yuv420p, faststart.

`blue-audio-{2,5,60}s.mp4`: copy the two-second H.264 stream from `blue.mp4`,
add AAC mono silence from `anullsrc=channel_layout=mono:sample_rate=8000`,
`-t {2,5,60} -c:v copy -c:a aac -b:a 8k -movflags +faststart`.
The latter two files deliberately have longer audio than video. No FFmpeg calls run in tests.
