# multipass-video

Schedule one video file on disk to play for every viewer at the same moment —
now or at a set time, once or looping. One binary, no build step for the
front end, no transcoding.

## How it stays in sync

The server does not stream in the broadcast sense. It serves the file with
ordinary HTTP range requests and publishes a *schedule* — file, start time,
loop flag — together with its own clock. Each viewer:

1. estimates the server-clock offset from a few `/api/state` round trips
   (lowest RTT wins, refreshed every 30 s);
2. computes where it should be: `(server_now − start) mod duration`;
3. seeks if it is more than 0.5 s off, otherwise bends `playbackRate` by up to
   ±8 % until the drift is under 50 ms.

Viewers therefore agree with the server, not with each other, so a late
joiner lands in the right place and nobody waits for anybody. Measured on
localhost: two tabs within ~3 ms of each other; drift settles at 15–30 ms.

## Install

Releases carry a tarball per platform — Linux x86_64 and aarch64 (built
against glibc 2.35: Ubuntu 22.04, Debian 12, or anything newer) and macOS
on Apple silicon — each holding the binary and this README. Or build it:

```bash
cargo install --path .
```

## Run

```bash
cargo run --release -- --media-dir /path/to/videos --listen 0.0.0.0:8080
```

- Viewer: `http://host:8080/`
- Admin: `http://host:8080/admin` — paste the contents of `admin-token`
  (created 0600 with 32 random bytes on first run; `--admin-token-file`).
- The current schedule persists in `multipass-state.json` (`--state-file`)
  and is restored on restart.
- Every flag also reads from `MULTIPASS_*` environment variables.

Put it behind TLS if it leaves the machine: the admin token travels as a
bearer header.

## Files that will play

Only what browsers decode natively: H.264/AAC MP4 (`.mp4`, `.m4v`, `.mov`)
or VP8/VP9/AV1 WebM. The `moov` atom must be at the front or viewers cannot
seek into the middle when they join; convert with

```bash
ffmpeg -i input.mkv -c:v libx264 -pix_fmt yuv420p -c:a aac -movflags +faststart out.mp4
```

A 30 s test clip with a running counter, useful for checking sync by eye:

```bash
ffmpeg -f lavfi -i testsrc=size=640x360:rate=30 -f lavfi -i sine=frequency=440:sample_rate=48000 \
  -t 30 -c:v libx264 -pix_fmt yuv420p -c:a aac -movflags +faststart media/clock-30s.mp4
```

## API

| Method | Path | Auth | Purpose |
|---|---|---|---|
| GET | `/api/state` | – | `{server_now_ms, version, schedule}` |
| GET | `/api/events` | – | SSE, the same object on every change |
| GET | `/api/files` | admin | playable files in the media dir |
| POST | `/api/schedule` | admin | `{file, start_ms?, loop?}` — `start_ms` absent means now |
| DELETE | `/api/schedule` | admin | stop |
| GET | `/media/<file>` | – | the file, with range support |

`schedule` is `{file, start_ms, loop}` or `null`. Admin calls send
`Authorization: Bearer <token>`.

## Not done (yet)

- Transcoding / HLS for files browsers cannot play.
- A playlist — one file at a time.
- Chat, viewer count, or anything social.
