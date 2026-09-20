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

## Who can watch

With `--viewer-key-file` (or `MULTIPASS_VIEWER_KEY_FILE`) set, every page,
API call and byte of media needs a cookie that `/login` sets once per
device from the key in that file -- five lowercase characters, minted on
first start, chosen to be typed on a TV remote; failed logins are served
one at a time with a delay, so guessing one takes months. Without it, anyone with
the URL can watch. The deploy unit sets it; read the key with
`ssh HOST cat /var/lib/multipass-video/viewer-key`.

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

## TV browsers

A TV browser's player is not a laptop's. Samsung's (Tizen 9, SamsungBrowser
8 on Chromium 120) was read through the viewer's trace on 2026-09-20:
its seeks land within 0.3 s every time, but **any `playbackRate` change
flushes the decoder and restarts from the next keyframe**, `play()` after
`pause()` does the same, the clock stalls 0.5-1.5 s after a seek, and the
loop wrap lands anywhere within a second. Corrected the way a laptop is
corrected -- nudge the rate, pause to wait -- every correction was a hop.

So the viewer checks each kind of correction for whether the player can
take it, and uses only what it can: a rate change followed at once by
`waiting` turns the rate off for good; a clock that jumps on resume ends
holds; a seek-only player is corrected by seeks alone, aimed ahead by the
measured stall, backing off (3, 6, 12 ... 30 s) until one settles inside
0.75 s. Samsung's browser starts with rate and holds off. On that TV the
result is one seek at join, one per loop wrap or so, and a flat drift
between (2 seeks, 0 holds, 0 rate changes in 67 s; v0.1.5 had 35 + 30 +
13 in 210 s). The HUD names what it found and is legible from a sofa on
a screen 1600 px or wider.

To see what any player is doing, open the viewer with `?trace` (TV
browsers trace without being asked): it reports every seek, landing,
hold, rate verdict and element event to the server's log, readable with
`journalctl -u multipass-video | grep viewer`. Read that before
simulating: the shim written before the trace had the TV's behaviour
backwards.

The other half is the file: `scripts/transcode` writes a keyframe every
2 s, which bounds where a seek can land on a player that seeks to
keyframes. (It was not the cause on the Samsung; a CBR Main-profile
encode behaved the same.)

## Files that will play

Only what browsers decode natively: H.264/AAC MP4 (`.mp4`, `.m4v`, `.mov`)
or VP8/VP9/AV1 WebM. The `moov` atom must be at the front or viewers cannot
seek into the middle when they join; convert with

```bash
ffmpeg -i input.mkv -c:v libx264 -pix_fmt yuv420p -c:a aac -movflags +faststart out.mp4
```

`scripts/transcode input output.mp4` produces the form that streams best
here -- H.264 High 4.1, at most 1080p, a keyframe every 2 s, capped
bitrate, AAC keeping the source's channels up to 5.1 (7.1 folds to 5.1),
`+faststart` -- from anything ffmpeg reads.

A 30 s test clip with a running counter, useful for checking sync by eye:

```bash
ffmpeg -f lavfi -i testsrc=size=640x360:rate=30 -f lavfi -i sine=frequency=440:sample_rate=48000 \
  -t 30 -c:v libx264 -pix_fmt yuv420p -c:a aac /tmp/raw.mp4 && scripts/transcode /tmp/raw.mp4 media/clock-30s.mp4
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

## Deploying

`deploy/` holds a hardened systemd unit and a Caddyfile; Caddy obtains and
renews the Let's Encrypt certificate itself. `scripts/deploy HOST TAG`
installs that release on a Debian/Ubuntu host from the GitHub release,
checked against its `SHA256SUMS`, and is safe to re-run to upgrade:

```bash
scripts/deploy root@multipass.video v0.1.0
```

Videos go in `/srv/multipass/media` on the host; the admin token is minted
on first start at `/var/lib/multipass-video/admin-token` and is never
printed by the script.
