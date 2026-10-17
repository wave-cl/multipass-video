# multipass-video

Schedule a video — or a playlist of them — to play for every viewer at the
same moment, now or at a set time, once or looping, with something else
looping after the list has played out. One binary, no build step for the
front end, no transcoding.

## How it stays in sync

The server does not stream in the broadcast sense. It serves the file with
ordinary HTTP range requests and publishes a *schedule* — file, start time,
loop flag — together with its own clock. Each viewer:

1. estimates the server-clock offset from a few `/api/state` round trips
   (lowest RTT wins, refreshed every 30 s);
2. computes which item of the playlist is on and where in it, by walking
   the items' durations from `start`;
3. seeks if it is more than 0.5 s off, otherwise bends `playbackRate` by up to
   ±8 % until the drift is under 50 ms.

The durations are the **server's**, read once when the playlist is
scheduled — from the MP4 movie header directly, or from ffprobe for the
formats that reader does not know. A viewer that trusted its own decoder
would put the boundary between two items in a slightly different place
from everyone else.

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

## Playlists

The admin page builds a queue: pick a file, **Add**, reorder with ↑ ↓,
drop with ×. The list plays in order from the start time. After it, one
of two things:

- **Loop the whole playlist** — the list repeats forever, or
- **Then loop** — the named file plays on repeat once the list has played
  out. (The two are mutually exclusive; a looping list never reaches what
  follows it, and the server refuses the pair.)

With neither, the stream ends when the list does. A file whose length
cannot be read cannot be queued: without it the boundary is unknown.
Item boundaries are switched on a timer aimed at the exact moment, not
on the next quarter-second tick, so the next item starts about 30 ms
after it is due plus whatever the browser takes to open the file.

## Intermission

The admin page's **Intermission** button pauses the film for everyone: the
viewer shows a "We'll be right back" card and plays music on a loop;
ending it starts a **10 second countdown** rather than resuming at once:
every viewer parks on the exact frame the film will resume at and fills
its buffer there, so they all start together instead of each stuttering
in. The card counts down, the music plays under it and fades out as the
picture returns. It resumes 10 s before where it was paused, so the
audience picks the thread up (the schedule's start moves forward by the
intermission's length plus the rewind plus the countdown, never past the
film's own beginning; a film that had not started yet keeps its own
countdown).

Starting content works the same way: **Start now** means ten seconds from
now, which viewers spend parked on the first frame with the buffer
filling. Measured locally: 14-30 s buffered before anything plays, and
the picture begins within a frame of the moment. An explicit start time
is obeyed exactly, including one in the past. Both switches are
instant: the film stays loaded and merely pauses under the card, the
music is loaded when the page loads and cached, and the card is CSS,
painted in advance. The music is a built-in ambient pad unless the media
directory holds an `intermission.mp3` (or `.m4a`, `.ogg`, `.opus`,
`.wav`, `.flac`), which is used instead after a restart. Viewers are never
shown the file's name -- only the countdown, the picture, and the HUD.

## Browsers and sound

A browser only starts sound on a page the viewer has clicked. The viewer
tries to play, retries muted, and then shows "Tap for sound" with a
button; after one click on the page, everything that follows plays with
sound on its own. Allowing autoplay for the site removes the tap. In
Brave, Shields override that allowance -- put Shields down for the site
(there is nothing third-party on the page for them to block). Samsung's
TV browser needs no tap.

## Who can watch

With `--viewer-key-file` (or `MULTIPASS_VIEWER_KEY_FILE`) set, every page,
API call and byte of media needs a cookie that `/login` sets once per
device from the key in that file -- five lowercase characters, minted on
first start, chosen to be typed on a TV remote; failed logins are served
one at a time with a delay, so guessing one takes months. Without it, anyone with
the URL can watch. The deploy unit leaves it unset; uncomment it there to gate the
stream, and read the key with `ssh HOST cat /var/lib/multipass-video/viewer-key`.

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
| POST | `/api/schedule` | admin | `{files: [..], start_ms?, loop?, then?}` — `start_ms` absent means ten seconds from now, `files` may be a single `file` |
| DELETE | `/api/schedule` | admin | stop |
| POST | `/api/intermission` | admin | pause the film for everyone, play music |
| DELETE | `/api/intermission` | admin | resume where it was paused |
| GET | `/intermission-audio` | – | the music, with range support |
| GET | `/media/<file>` | – | the file, with range support |

`schedule` is `{items: [{file, duration_ms}], start_ms, loop, then}` or
`null`, where `then` is an item or `null`; `intermission_since_ms` is set
while an intermission is on, and `resume_at_ms` is the moment the film
starts again after one. `/api/files` gives each file's `duration_ms`,
or `null` when it could not be read. Admin calls send
`Authorization: Bearer <token>`.

## Not done (yet)

- Transcoding / HLS for files browsers cannot play.
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
