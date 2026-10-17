//! multipass-video: schedule one video file on disk to play for every viewer
//! at the same wall-clock moment.
//!
//! The server never transcodes. It serves the file with HTTP range requests
//! and publishes a schedule (file, start time, loop flag) plus its own clock;
//! each viewer derives the position it should be at from that and corrects
//! its own playback to match.

use std::{
    convert::Infallible,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Request, State},
    http::{header, HeaderMap, Method, StatusCode},
    middleware::{self, Next},
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Redirect, Response,
    },
    routing::{get, post},
    Form, Json, Router,
};
use clap::Parser;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tokio_stream::{wrappers::WatchStream, Stream, StreamExt};
use tower_http::{
    services::{ServeDir, ServeFile},
    set_header::SetResponseHeaderLayer,
    trace::TraceLayer,
};
use tracing::{info, warn};

const VIEWER_HTML: &str = include_str!("../static/viewer.html");
const ADMIN_HTML: &str = include_str!("../static/admin.html");
const LOGIN_HTML: &str = include_str!("../static/login.html");
/// The intermission music when the media dir has no `intermission.*`.
const INTERMISSION_AUDIO: &[u8] = include_bytes!("../static/intermission.m4a");
const VIEWER_COOKIE: &str = "mp_viewer";

/// Extensions browsers can generally play natively.
const PLAYABLE: &[&str] = &["mp4", "m4v", "webm", "mov", "ogv", "ogg"];

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Directory holding the video files offered to the administrator.
    #[arg(long, env = "MULTIPASS_MEDIA_DIR", default_value = "media")]
    media_dir: PathBuf,

    /// Address to listen on.
    #[arg(long, env = "MULTIPASS_LISTEN", default_value = "127.0.0.1:8080")]
    listen: SocketAddr,

    /// File holding the admin bearer token. Created (0600, random) if absent.
    #[arg(
        long,
        env = "MULTIPASS_ADMIN_TOKEN_FILE",
        default_value = "admin-token"
    )]
    admin_token_file: PathBuf,

    /// Where the current schedule is persisted across restarts.
    #[arg(
        long,
        env = "MULTIPASS_STATE_FILE",
        default_value = "multipass-state.json"
    )]
    state_file: PathBuf,

    /// File holding the viewer key. Set, every page and stream needs it
    /// (entered once per device at /login); unset, anyone can watch.
    /// Created (0600, five random lowercase characters) if absent.
    #[arg(long, env = "MULTIPASS_VIEWER_KEY_FILE")]
    viewer_key_file: Option<PathBuf>,
}

/// One entry of a playlist. The duration is the server's, read once when
/// the playlist is scheduled, so every viewer agrees on where the
/// boundaries are rather than each trusting its own decoder.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Item {
    file: String,
    duration_ms: i64,
}

/// What is scheduled: a playlist played in order from `start_ms`, which is
/// unix milliseconds on the server clock. When it has played out, `then`
/// (if set) loops forever; with `loop` the list itself repeats instead.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Schedule {
    /// Defaulted so a pre-0.4 state file, which has no `items`, still
    /// parses and can be migrated rather than thrown away whole.
    #[serde(default)]
    items: Vec<Item>,
    start_ms: i64,
    #[serde(rename = "loop", default)]
    loop_: bool,
    #[serde(default)]
    then: Option<Item>,
    /// Schedules written before 0.4 named one file and no duration. Read
    /// back here and migrated at startup; never written again.
    #[serde(default, skip_serializing)]
    file: Option<String>,
}

impl Schedule {
    fn total_ms(&self) -> i64 {
        self.items.iter().map(|i| i.duration_ms).sum()
    }
}

/// The persisted state. `version` bumps on every change so clients can tell
/// a new schedule from a repeat of the current one.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Persisted {
    version: u64,
    schedule: Option<Schedule>,
    /// When the intermission began, if one is on. The film is paused for
    /// its length: ending it moves the schedule's start forward by that.
    #[serde(default)]
    intermission_since_ms: Option<i64>,
}

/// What clients see: the state plus the server clock for offset estimation.
#[derive(Serialize)]
struct StateView {
    server_now_ms: i64,
    version: u64,
    schedule: Option<Schedule>,
    intermission_since_ms: Option<i64>,
}

#[derive(Serialize)]
struct FileEntry {
    name: String,
    size: u64,
    /// None when neither the MP4 header nor ffprobe could say; such a file
    /// cannot be put in a playlist, because its boundary is unknown.
    duration_ms: Option<i64>,
}

/// A viewer's own account of what its player did, sent only when opened
/// with `?trace`. Written to the log so a TV across the world can be read
/// from here; bounded, since anyone can post it.
#[derive(Deserialize)]
struct TraceBody {
    id: String,
    ua: String,
    lines: Vec<String>,
}

#[derive(Deserialize)]
struct ScheduleRequest {
    /// The playlist, in order.
    #[serde(default)]
    files: Vec<String>,
    /// A single file, as the API took before 0.4: a playlist of one.
    #[serde(default)]
    file: Option<String>,
    /// Absent or null means "now".
    start_ms: Option<i64>,
    /// Repeat the whole list forever.
    #[serde(rename = "loop", default)]
    loop_: bool,
    /// What loops once the list has played out.
    #[serde(default)]
    then: Option<String>,
}

#[derive(Deserialize)]
struct LoginForm {
    key: String,
}

struct Inner {
    media_dir: PathBuf,
    state_file: PathBuf,
    intermission_audio: PathBuf,
    admin_token: String,
    /// The viewer key and the cookie value that proves it (its SHA-256, hex).
    viewer: Option<(String, String)>,
    /// Held across the delay on a failed login: one guess at a time, server-wide.
    login_failures: tokio::sync::Mutex<()>,
    state: Mutex<Persisted>,
    /// Carries the version; SSE subscribers wake on it and re-read the state.
    notify: watch::Sender<u64>,
}

type App = Arc<Inner>;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before 1970")
        .as_millis() as i64
}

/// A bare file name inside the media directory: no separators, no dot-files,
/// no traversal.
fn valid_file_name(name: &str) -> bool {
    !name.is_empty() && !name.starts_with('.') && !name.contains(['/', '\\', '\0']) && name != ".."
}

fn playable(name: &str) -> bool {
    Path::new(name)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| PLAYABLE.contains(&e.to_ascii_lowercase().as_str()))
        .unwrap_or(false)
}

/// The first box of type `want` between `start` and `end`, as the range its
/// contents occupy. ISO-BMFF boxes are [u32 size][4cc type], with size 1
/// meaning a 64-bit size follows and 0 meaning "to the end".
fn find_box(
    f: &mut std::fs::File,
    start: u64,
    end: u64,
    want: &[u8; 4],
) -> std::io::Result<Option<(u64, u64)>> {
    use std::io::{Read, Seek, SeekFrom};
    let mut pos = start;
    while pos + 8 <= end {
        f.seek(SeekFrom::Start(pos))?;
        let mut h = [0u8; 8];
        if f.read_exact(&mut h).is_err() {
            return Ok(None);
        }
        let typ: [u8; 4] = h[4..8].try_into().unwrap();
        let size32 = u32::from_be_bytes(h[0..4].try_into().unwrap()) as u64;
        let (size, header) = match size32 {
            1 => {
                let mut e = [0u8; 8];
                f.read_exact(&mut e)?;
                (u64::from_be_bytes(e), 16)
            }
            0 => (end - pos, 8),
            n => (n, 8),
        };
        if size < header || pos + size > end {
            return Ok(None); // malformed; do not guess
        }
        if &typ == want {
            return Ok(Some((pos + header, pos + size)));
        }
        pos += size;
    }
    Ok(None)
}

/// How long an MP4/M4V/MOV plays, from `moov`/`mvhd`. None if the file is
/// not ISO-BMFF, or is fragmented and says zero.
fn mp4_duration_ms(path: &Path) -> std::io::Result<Option<i64>> {
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    let len = f.metadata()?.len();
    let Some((moov, moov_end)) = find_box(&mut f, 0, len, b"moov")? else {
        return Ok(None);
    };
    let Some((mvhd, _)) = find_box(&mut f, moov, moov_end, b"mvhd")? else {
        return Ok(None);
    };
    std::io::Seek::seek(&mut f, std::io::SeekFrom::Start(mvhd))?;
    let mut version = [0u8; 4]; // version + flags
    f.read_exact(&mut version)?;
    let (timescale, duration) = if version[0] == 1 {
        let mut b = [0u8; 28]; // creation(8) modification(8) timescale(4) duration(8)
        f.read_exact(&mut b)?;
        (
            u32::from_be_bytes(b[16..20].try_into().unwrap()) as u64,
            u64::from_be_bytes(b[20..28].try_into().unwrap()),
        )
    } else {
        let mut b = [0u8; 16]; // creation(4) modification(4) timescale(4) duration(4)
        f.read_exact(&mut b)?;
        (
            u32::from_be_bytes(b[8..12].try_into().unwrap()) as u64,
            u32::from_be_bytes(b[12..16].try_into().unwrap()) as u64,
        )
    };
    if timescale == 0 || duration == 0 {
        return Ok(None);
    }
    Ok(Some((duration as i128 * 1000 / timescale as i128) as i64))
}

/// Ask ffprobe, for the formats the MP4 reader does not know (WebM, Ogg).
/// Absent ffprobe is not an error: the file simply has no known duration.
fn ffprobe_duration_ms(path: &Path) -> Option<i64> {
    let out = std::process::Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=nw=1:nk=1",
        ])
        .arg(path)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let secs: f64 = String::from_utf8_lossy(&out.stdout).trim().parse().ok()?;
    (secs > 0.0).then(|| (secs * 1000.0).round() as i64)
}

/// How long a file plays. Blocking: a few seeks for an MP4, a process for
/// anything else.
fn probe_duration_ms(path: &Path) -> Option<i64> {
    match mp4_duration_ms(path) {
        Ok(Some(ms)) => return Some(ms),
        Ok(None) => {}
        Err(e) => warn!("{}: {e}", path.display()),
    }
    ffprobe_duration_ms(path)
}

/// Check a name and read its duration, or say which of the two failed.
fn item_for(media_dir: &Path, name: &str) -> Result<Item, (StatusCode, String)> {
    if !valid_file_name(name) || !playable(name) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("{name}: not a playable file name"),
        ));
    }
    let path = media_dir.join(name);
    if !path.is_file() {
        return Err((
            StatusCode::NOT_FOUND,
            format!("{name}: no such file in the media directory"),
        ));
    }
    match probe_duration_ms(&path) {
        Some(duration_ms) => Ok(Item {
            file: name.to_string(),
            duration_ms,
        }),
        None => Err((
            StatusCode::BAD_REQUEST,
            format!("{name}: cannot tell how long it is, so it cannot be scheduled"),
        )),
    }
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

impl Inner {
    fn view(&self) -> StateView {
        let s = self.state.lock().unwrap();
        StateView {
            server_now_ms: now_ms(),
            version: s.version,
            schedule: s.schedule.clone(),
            intermission_since_ms: s.intermission_since_ms,
        }
    }

    fn require_admin(&self, headers: &HeaderMap) -> Result<(), StatusCode> {
        let presented = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or("");
        if ct_eq(presented.as_bytes(), self.admin_token.as_bytes()) {
            Ok(())
        } else {
            Err(StatusCode::UNAUTHORIZED)
        }
    }

    /// Replace the schedule, persist it, and wake every SSE subscriber.
    fn set(&self, schedule: Option<Schedule>) -> Result<u64, std::io::Error> {
        self.update(|s| {
            s.schedule = schedule;
            s.intermission_since_ms = None;
        })
    }

    /// Change the state under the lock, bump the version, persist, wake.
    fn update(&self, f: impl FnOnce(&mut Persisted)) -> Result<u64, std::io::Error> {
        let snapshot = {
            let mut s = self.state.lock().unwrap();
            f(&mut s);
            s.version += 1;
            s.clone()
        };
        persist(&self.state_file, &snapshot)?;
        self.notify.send_replace(snapshot.version);
        Ok(snapshot.version)
    }
}

fn persist(path: &Path, state: &Persisted) -> std::io::Result<()> {
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(state)?)?;
    std::fs::rename(&tmp, path)
}

fn load_state(path: &Path, media_dir: &Path) -> Persisted {
    let mut state: Persisted = match std::fs::read(path) {
        Ok(bytes) => match serde_json::from_slice(&bytes) {
            Ok(s) => s,
            Err(e) => {
                warn!("{}: unreadable state, starting empty: {e}", path.display());
                Persisted::default()
            }
        },
        Err(_) => Persisted::default(),
    };
    // A schedule written before 0.4 names one file and carries no duration.
    // Give it one, so what was playing keeps playing across the upgrade.
    if let Some(sch) = &mut state.schedule {
        if sch.items.is_empty() {
            match sch.file.take() {
                Some(file) => match item_for(media_dir, &file) {
                    Ok(item) => {
                        info!("migrated the schedule of {file} to a playlist of one");
                        sch.items = vec![item];
                    }
                    Err((_, why)) => {
                        warn!("dropping the stored schedule: {why}");
                        state.schedule = None;
                    }
                },
                None => {
                    warn!("dropping a stored schedule with no files");
                    state.schedule = None;
                }
            }
        }
    }
    state
}

/// Read a secret from a file, or mint one into it (0600). The admin token
/// is 32 random bytes as hex; the viewer key is five lowercase characters,
/// because it gets typed on a TV remote. Five is about 29 million
/// possibilities; failed logins are served one at a time with a delay, so
/// guessing them takes months.
fn load_or_create_secret(path: &Path, what: &str, short: bool) -> std::io::Result<String> {
    if let Ok(s) = std::fs::read_to_string(path) {
        let s = s.trim().to_string();
        if !s.is_empty() {
            return Ok(s);
        }
    }
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    let token: String = if short {
        const ALPHABET: &[u8] = b"abcdefghjkmnpqrstuvwxyz23456789"; // no 0/o/1/l/i
        bytes[..5]
            .iter()
            .map(|b| ALPHABET[*b as usize % ALPHABET.len()] as char)
            .collect()
    } else {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    };
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(path)?;
    writeln!(f, "{token}")?;
    info!("created {what} file {}", path.display());
    Ok(token)
}

fn cookie_value(key: &str) -> String {
    Sha256::digest(key.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Everything but /login needs the viewer cookie, when a viewer key is set.
/// A browser asking for a page is sent to log in; anything else gets 401.
async fn viewer_gate(State(app): State<App>, req: Request, next: Next) -> Response {
    let Some((_, expect)) = &app.viewer else {
        return next.run(req).await;
    };
    if req.uri().path() == "/login" {
        return next.run(req).await;
    }
    let presented = req
        .headers()
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(';'))
        .filter_map(|c| {
            c.trim()
                .strip_prefix(VIEWER_COOKIE)
                .and_then(|c| c.strip_prefix('='))
        })
        .any(|v| ct_eq(v.as_bytes(), expect.as_bytes()));
    if presented {
        return next.run(req).await;
    }
    let wants_page = req.method() == Method::GET
        && req
            .headers()
            .get(header::ACCEPT)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|a| a.contains("text/html"));
    if wants_page {
        Redirect::to("/login").into_response()
    } else {
        StatusCode::UNAUTHORIZED.into_response()
    }
}

async fn login_page() -> Html<String> {
    Html(LOGIN_HTML.replace("{{error}}", ""))
}

async fn login_post(State(app): State<App>, Form(form): Form<LoginForm>) -> Response {
    let Some((key, cookie)) = &app.viewer else {
        return Redirect::to("/").into_response();
    };
    if ct_eq(form.key.trim().as_bytes(), key.as_bytes()) {
        let set =
            format!("{VIEWER_COOKIE}={cookie}; Path=/; Max-Age=31536000; HttpOnly; SameSite=Lax");
        ([(header::SET_COOKIE, set)], Redirect::to("/")).into_response()
    } else {
        let _one_at_a_time = app.login_failures.lock().await;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        warn!("viewer login refused");
        (
            StatusCode::UNAUTHORIZED,
            Html(LOGIN_HTML.replace("{{error}}", "That key was not accepted.")),
        )
            .into_response()
    }
}

async fn viewer() -> Html<&'static str> {
    Html(VIEWER_HTML)
}

async fn admin() -> Html<&'static str> {
    Html(ADMIN_HTML)
}

async fn get_state(State(app): State<App>) -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-store")], Json(app.view()))
}

async fn events(State(app): State<App>) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // WatchStream yields the current version at once, then each change.
    let stream = WatchStream::new(app.notify.subscribe()).map(move |_| {
        let event = Event::default()
            .event("state")
            .json_data(app.view())
            .expect("state serialises");
        Ok(event)
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn list_files(
    State(app): State<App>,
    headers: HeaderMap,
) -> Result<Json<Vec<FileEntry>>, StatusCode> {
    app.require_admin(&headers)?;
    let mut rd = tokio::fs::read_dir(&app.media_dir)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut out = Vec::new();
    while let Some(entry) = rd
        .next_entry()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !valid_file_name(&name) || !playable(&name) {
            continue;
        }
        let Ok(meta) = entry.metadata().await else {
            continue;
        };
        if !meta.is_file() {
            continue;
        }
        out.push(FileEntry {
            name,
            size: meta.len(),
            duration_ms: None,
        });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    // Probing reads headers and may run ffprobe, so it goes off the runtime.
    let media_dir = app.media_dir.clone();
    let out = tokio::task::spawn_blocking(move || {
        for e in &mut out {
            e.duration_ms = probe_duration_ms(&media_dir.join(&e.name));
        }
        out
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(out))
}

async fn set_schedule(
    State(app): State<App>,
    headers: HeaderMap,
    Json(req): Json<ScheduleRequest>,
) -> Result<Json<StateView>, (StatusCode, String)> {
    app.require_admin(&headers)
        .map_err(|s| (s, "admin token required".into()))?;
    let mut names = req.files;
    names.extend(req.file); // one `file` is a playlist of one
    if names.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "no files given".into()));
    }
    if req.loop_ && req.then.is_some() {
        return Err((
            StatusCode::BAD_REQUEST,
            "a looping playlist never reaches `then`".into(),
        ));
    }
    // Reading durations is blocking work; do it off the runtime.
    let media_dir = app.media_dir.clone();
    let then_name = req.then;
    let (items, then) = tokio::task::spawn_blocking(move || {
        let items = names
            .iter()
            .map(|n| item_for(&media_dir, n))
            .collect::<Result<Vec<_>, _>>()?;
        let then = then_name
            .as_deref()
            .map(|n| item_for(&media_dir, n))
            .transpose()?;
        Ok::<_, (StatusCode, String)>((items, then))
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))??;
    let schedule = Schedule {
        items,
        start_ms: req.start_ms.unwrap_or_else(now_ms),
        loop_: req.loop_,
        then,
        file: None,
    };
    info!(
        files = schedule.items.len(),
        total_s = schedule.total_ms() / 1000,
        then = schedule.then.as_ref().map(|i| i.file.as_str()),
        loop_ = schedule.loop_,
        "scheduled"
    );
    app.set(Some(schedule))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(app.view()))
}

async fn clear_schedule(
    State(app): State<App>,
    headers: HeaderMap,
) -> Result<Json<StateView>, (StatusCode, String)> {
    app.require_admin(&headers)
        .map_err(|s| (s, "admin token required".into()))?;
    info!("schedule cleared");
    app.set(None)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(app.view()))
}

async fn start_intermission(
    State(app): State<App>,
    headers: HeaderMap,
) -> Result<Json<StateView>, (StatusCode, String)> {
    app.require_admin(&headers)
        .map_err(|s| (s, "admin token required".into()))?;
    let now = now_ms();
    app.update(|s| {
        if s.intermission_since_ms.is_none() {
            s.intermission_since_ms = Some(now);
        }
    })
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    info!("intermission started");
    Ok(Json(app.view()))
}

/// End the intermission. A film that had started resumes a little before
/// where it was paused -- `REWIND_MS` back, so the audience picks the thread
/// up -- its start moving forward by the intermission's length plus that.
/// Never before the film's own beginning. One that had not started yet
/// keeps its countdown.
const REWIND_MS: i64 = 10_000;
async fn end_intermission(
    State(app): State<App>,
    headers: HeaderMap,
) -> Result<Json<StateView>, (StatusCode, String)> {
    app.require_admin(&headers)
        .map_err(|s| (s, "admin token required".into()))?;
    let now = now_ms();
    app.update(|s| {
        if let Some(since) = s.intermission_since_ms.take() {
            if let Some(sch) = &mut s.schedule {
                if sch.start_ms <= since {
                    sch.start_ms = (sch.start_ms + (now - since) + REWIND_MS).min(now);
                }
            }
        }
    })
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    info!("intermission ended");
    Ok(Json(app.view()))
}

/// The music: `intermission.*` in the media dir if the operator put one
/// there, else the built-in pad, written next to the state file so it can
/// be served with range support like any other media.
fn intermission_audio_path(media_dir: &Path, state_file: &Path) -> std::io::Result<PathBuf> {
    for ext in ["mp3", "m4a", "aac", "ogg", "opus", "wav", "flac"] {
        let p = media_dir.join(format!("intermission.{ext}"));
        if p.is_file() {
            info!("intermission music: {}", p.display());
            return Ok(p);
        }
    }
    let p = state_file.with_file_name("intermission-default.m4a");
    if std::fs::read(&p).ok().as_deref() != Some(INTERMISSION_AUDIO) {
        std::fs::write(&p, INTERMISSION_AUDIO)?;
    }
    Ok(p)
}

/// `.m4a` guesses to `audio/m4a`, which Safari does not recognise; AAC in
/// an MP4 container is `audio/mp4`.
fn intermission_service(path: &Path) -> ServeFile {
    match path.extension().and_then(|e| e.to_str()) {
        Some("m4a") | Some("aac") => {
            ServeFile::new_with_mime(path, &"audio/mp4".parse::<mime::Mime>().unwrap())
        }
        _ => ServeFile::new(path),
    }
}

async fn post_trace(Json(body): Json<TraceBody>) -> StatusCode {
    if body.lines.len() > 200 {
        return StatusCode::PAYLOAD_TOO_LARGE;
    }
    let id: String = body
        .id
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .take(8)
        .collect();
    let ua: String = body.ua.chars().take(200).collect();
    info!(target: "viewer", "{id} ua {ua}");
    for line in &body.lines {
        let line: String = line.chars().take(300).collect();
        info!(target: "viewer", "{id} {line}");
    }
    StatusCode::NO_CONTENT
}

fn router(app: App) -> Router {
    let media = ServeDir::new(&app.media_dir);
    Router::new()
        .route("/", get(viewer))
        .route("/login", get(login_page).post(login_post))
        .route("/admin", get(admin))
        .route("/api/state", get(get_state))
        .route("/api/events", get(events))
        .route("/api/files", get(list_files))
        .route("/api/schedule", post(set_schedule).delete(clear_schedule))
        .route("/api/trace", post(post_trace))
        .route(
            "/api/intermission",
            post(start_intermission).delete(end_intermission),
        )
        .route_service(
            "/intermission-audio",
            // Cacheable for a day: the viewer preloads it at page load and
            // a later intermission then plays from the browser's cache.
            // ServeFile's Last-Modified lets the browser revalidate.
            tower::ServiceBuilder::new()
                .layer(SetResponseHeaderLayer::overriding(
                    header::CACHE_CONTROL,
                    header::HeaderValue::from_static("public, max-age=86400"),
                ))
                .service(intermission_service(&app.intermission_audio)),
        )
        .nest_service("/media", media)
        .layer(middleware::from_fn_with_state(app.clone(), viewer_gate))
        .layer(TraceLayer::new_for_http())
        .with_state(app)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tower_http=warn".into()),
        )
        .init();
    let args = Args::parse();

    if !args.media_dir.is_dir() {
        return Err(format!("media dir {} is not a directory", args.media_dir.display()).into());
    }
    let admin_token = load_or_create_secret(&args.admin_token_file, "admin token", false)?;
    let viewer = match &args.viewer_key_file {
        Some(p) => {
            let key = load_or_create_secret(p, "viewer key", true)?;
            info!("viewers must log in with the key in {}", p.display());
            Some((key.clone(), cookie_value(&key)))
        }
        None => {
            info!("no viewer key file: anyone can watch");
            None
        }
    };
    let state = load_state(&args.state_file, &args.media_dir);
    if let Some(s) = &state.schedule {
        info!(?s, "restored schedule");
    }
    let (notify, _) = watch::channel(state.version);

    let intermission_audio = intermission_audio_path(&args.media_dir, &args.state_file)?;
    let app: App = Arc::new(Inner {
        media_dir: args.media_dir.clone(),
        state_file: args.state_file,
        intermission_audio,
        admin_token,
        viewer,
        login_failures: Default::default(),
        state: Mutex::new(state),
        notify,
    });

    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    info!(
        "listening on http://{}  (viewer: /  admin: /admin  media: {})",
        args.listen,
        args.media_dir.display()
    );
    axum::serve(listener, router(app)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_names_are_bare() {
        for ok in ["a.mp4", "My Film (2024).webm", "x.MOV"] {
            assert!(valid_file_name(ok) && playable(ok), "{ok}");
        }
        for bad in [
            "",
            "..",
            "../a.mp4",
            "sub/a.mp4",
            "sub\\a.mp4",
            ".hidden.mp4",
            "a\0.mp4",
        ] {
            assert!(!valid_file_name(bad), "{bad:?}");
        }
        for bad in ["a.mkv", "a.txt", "mp4", "a"] {
            assert!(!playable(bad), "{bad}");
        }
    }

    #[test]
    fn token_check_is_exact() {
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"ab"));
        assert!(!ct_eq(b"", b"a"));
    }

    /// A file with nothing in it but a movie header: enough for the
    /// duration reader, and the smallest thing the scheduler accepts.
    fn fake_mp4(duration_ms: u32) -> Vec<u8> {
        let mut mvhd = vec![0u8; 4]; // version 0 + flags
        mvhd.extend_from_slice(&0u32.to_be_bytes()); // creation
        mvhd.extend_from_slice(&0u32.to_be_bytes()); // modification
        mvhd.extend_from_slice(&1000u32.to_be_bytes()); // timescale: ms
        mvhd.extend_from_slice(&duration_ms.to_be_bytes());
        let mut v = Vec::new();
        v.extend_from_slice(&16u32.to_be_bytes());
        v.extend_from_slice(b"ftypisom");
        v.extend_from_slice(&[0, 0, 0, 0]);
        v.extend_from_slice(&((16 + mvhd.len()) as u32).to_be_bytes());
        v.extend_from_slice(b"moov");
        v.extend_from_slice(&((8 + mvhd.len()) as u32).to_be_bytes());
        v.extend_from_slice(b"mvhd");
        v.extend_from_slice(&mvhd);
        v
    }

    #[test]
    fn a_movie_header_gives_the_duration() {
        let dir = test_dir("mp4");

        let p = dir.join("v0.mp4");
        std::fs::write(&p, fake_mp4(94_000)).unwrap();
        assert_eq!(mp4_duration_ms(&p).unwrap(), Some(94_000));

        // The 64-bit header, which long recordings use.
        let mut mvhd = vec![1u8, 0, 0, 0];
        mvhd.extend_from_slice(&[0u8; 16]); // creation + modification
        mvhd.extend_from_slice(&90_000u32.to_be_bytes()); // timescale
        mvhd.extend_from_slice(&(90_000u64 * 42).to_be_bytes()); // 42 s
        let mut v = Vec::new();
        v.extend_from_slice(&((16 + mvhd.len()) as u32).to_be_bytes());
        v.extend_from_slice(b"moov");
        v.extend_from_slice(&((8 + mvhd.len()) as u32).to_be_bytes());
        v.extend_from_slice(b"mvhd");
        v.extend_from_slice(&mvhd);
        let p = dir.join("v1.mp4");
        std::fs::write(&p, v).unwrap();
        assert_eq!(mp4_duration_ms(&p).unwrap(), Some(42_000));

        // Not an MP4 at all, and a truncated one: no answer, no panic.
        let p = dir.join("junk.mp4");
        std::fs::write(&p, b"not an mp4 at all").unwrap();
        assert_eq!(mp4_duration_ms(&p).unwrap(), None);
        let p = dir.join("short.mp4");
        std::fs::write(&p, &fake_mp4(1000)[..20]).unwrap();
        assert_eq!(mp4_duration_ms(&p).unwrap(), None);
    }

    /// A directory of this test's own: tests run at once in one process,
    /// and sharing a state file makes one test's write fail inside another.
    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("multipass-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn test_app(name: &str, viewer_key: Option<&str>) -> Router {
        test_app_in(&test_dir(name), viewer_key)
    }

    fn test_app_in(dir: &Path, viewer_key: Option<&str>) -> Router {
        let dir = dir.to_path_buf();
        let (notify, _) = watch::channel(0);
        let app: App = Arc::new(Inner {
            media_dir: dir.clone(),
            state_file: dir.join("state.json"),
            intermission_audio: dir.join("intermission.m4a"),
            admin_token: "admin".into(),
            viewer: viewer_key.map(|k| (k.to_string(), cookie_value(k))),
            login_failures: Default::default(),
            state: Mutex::new(Persisted::default()),
            notify,
        });
        router(app)
    }

    async fn call(app: &Router, req: Request) -> (StatusCode, HeaderMap) {
        use tower::ServiceExt;
        let res = app.clone().oneshot(req).await.unwrap();
        (res.status(), res.headers().clone())
    }

    fn get(path: &str) -> Request {
        Request::get(path).body(Default::default()).unwrap()
    }

    #[tokio::test]
    async fn without_a_viewer_key_anyone_can_watch() {
        let app = test_app("open", None);
        assert_eq!(call(&app, get("/api/state")).await.0, StatusCode::OK);
    }

    #[tokio::test]
    async fn with_a_viewer_key_everything_needs_the_cookie() {
        let app = test_app("cookie", Some("hunter2"));
        // API and media: 401. A browser asking for a page: sent to /login.
        for path in ["/api/state", "/api/events", "/media/x.mp4", "/api/files"] {
            assert_eq!(
                call(&app, get(path)).await.0,
                StatusCode::UNAUTHORIZED,
                "{path}"
            );
        }
        for path in ["/", "/admin"] {
            let req = Request::get(path)
                .header(header::ACCEPT, "text/html")
                .body(Default::default())
                .unwrap();
            let (s, h) = call(&app, req).await;
            assert_eq!(s, StatusCode::SEE_OTHER, "{path}");
            assert_eq!(h[header::LOCATION], "/login");
        }
        // The login page itself is open.
        assert_eq!(call(&app, get("/login")).await.0, StatusCode::OK);

        // A wrong key is refused, the right one sets the cookie.
        let login = |key: &str| {
            Request::post("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(format!("key={key}").into())
                .unwrap()
        };
        let (s, h) = call(&app, login("hunter3")).await;
        assert_eq!(s, StatusCode::UNAUTHORIZED);
        assert!(h.get(header::SET_COOKIE).is_none());
        let (s, h) = call(&app, login("hunter2")).await;
        assert_eq!(s, StatusCode::SEE_OTHER);
        let cookie = h[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_string();
        assert!(cookie.starts_with("mp_viewer="));

        // With it, the API answers; a forged cookie does not.
        let with = |c: &str| {
            Request::get("/api/state")
                .header(header::COOKIE, c)
                .body(Default::default())
                .unwrap()
        };
        assert_eq!(call(&app, with(&cookie)).await.0, StatusCode::OK);
        assert_eq!(
            call(&app, with("mp_viewer=deadbeef")).await.0,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn failed_logins_are_served_one_at_a_time() {
        let app = test_app("logins", Some("abcde"));
        let login = || {
            Request::post("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body("key=zzzzz".into())
                .unwrap()
        };
        let t0 = std::time::Instant::now();
        let (a, b) = tokio::join!(call(&app, login()), call(&app, login()));
        assert_eq!(
            (a.0, b.0),
            (StatusCode::UNAUTHORIZED, StatusCode::UNAUTHORIZED)
        );
        assert!(
            t0.elapsed() >= std::time::Duration::from_millis(1000),
            "{:?}",
            t0.elapsed()
        );
    }

    #[tokio::test]
    async fn a_playlist_plays_in_order_and_then_loops_the_last_thing() {
        use http_body_util::BodyExt;
        let dir = test_dir("playlist");
        let app = test_app_in(&dir, None);
        std::fs::write(dir.join("a.mp4"), fake_mp4(20_000)).unwrap();
        std::fs::write(dir.join("b.mp4"), fake_mp4(30_000)).unwrap();
        std::fs::write(dir.join("clock.mp4"), fake_mp4(5_000)).unwrap();

        let post = |body: String| {
            Request::post("/api/schedule")
                .header(header::AUTHORIZATION, "Bearer admin")
                .header(header::CONTENT_TYPE, "application/json")
                .body(body.into())
                .unwrap()
        };
        let state = |app: &Router| {
            let app = app.clone();
            async move {
                use tower::ServiceExt;
                let res = app.oneshot(get("/api/state")).await.unwrap();
                let body = res.into_body().collect().await.unwrap().to_bytes();
                serde_json::from_slice::<serde_json::Value>(&body).unwrap()
            }
        };

        let req = post(r#"{"files":["a.mp4","b.mp4"],"then":"clock.mp4"}"#.into());
        assert_eq!(call(&app, req).await.0, StatusCode::OK);
        let s = state(&app).await;
        let items = s["schedule"]["items"].as_array().unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["file"], "a.mp4");
        assert_eq!(items[0]["duration_ms"], 20_000);
        assert_eq!(items[1]["duration_ms"], 30_000);
        assert_eq!(s["schedule"]["then"]["file"], "clock.mp4");
        assert_eq!(s["schedule"]["then"]["duration_ms"], 5_000);
        assert_eq!(s["schedule"]["loop"], false);

        // One `file` is still a playlist of one.
        let req = post(r#"{"file":"a.mp4","loop":true}"#.into());
        assert_eq!(call(&app, req).await.0, StatusCode::OK);
        let s = state(&app).await;
        assert_eq!(s["schedule"]["items"].as_array().unwrap().len(), 1);
        assert_eq!(s["schedule"]["loop"], true);

        // Refusals: nothing to play, a file that is not there, one whose
        // length cannot be read, and a loop that would never reach `then`.
        std::fs::write(dir.join("mystery.mp4"), b"not an mp4").unwrap();
        for (body, want) in [
            (r#"{"files":[]}"#, StatusCode::BAD_REQUEST),
            (r#"{"files":["nope.mp4"]}"#, StatusCode::NOT_FOUND),
            (r#"{"files":["../etc/passwd"]}"#, StatusCode::BAD_REQUEST),
            (r#"{"files":["mystery.mp4"]}"#, StatusCode::BAD_REQUEST),
            (
                r#"{"files":["a.mp4"],"loop":true,"then":"clock.mp4"}"#,
                StatusCode::BAD_REQUEST,
            ),
        ] {
            assert_eq!(call(&app, post(body.into())).await.0, want, "{body}");
        }
        // ... and none of them disturbed what was already scheduled.
        let s = state(&app).await;
        assert_eq!(s["schedule"]["items"][0]["file"], "a.mp4");
    }

    #[tokio::test]
    async fn a_schedule_written_before_playlists_still_plays() {
        let dir = test_dir("legacy");
        std::fs::write(dir.join("old.mp4"), fake_mp4(123_000)).unwrap();
        let state_file = dir.join("state.json");
        std::fs::write(
            &state_file,
            br#"{"version":7,"schedule":{"file":"old.mp4","start_ms":1000,"loop":true}}"#,
        )
        .unwrap();

        let state = load_state(&state_file, &dir);
        let sch = state.schedule.expect("the schedule survives the upgrade");
        assert_eq!(state.version, 7);
        assert_eq!(
            sch.items,
            vec![Item {
                file: "old.mp4".into(),
                duration_ms: 123_000
            }]
        );
        assert_eq!(sch.start_ms, 1000);
        assert!(sch.loop_);

        // A legacy schedule whose file has since gone is dropped, not kept
        // as something unplayable.
        std::fs::remove_file(dir.join("old.mp4")).unwrap();
        assert!(load_state(&state_file, &dir).schedule.is_none());
    }

    #[tokio::test]
    async fn an_intermission_pauses_a_running_film_and_not_a_countdown() {
        use http_body_util::BodyExt;
        let dir = test_dir("intermission");
        let app = test_app_in(&dir, None);
        let admin =
            |req: axum::http::request::Builder| req.header(header::AUTHORIZATION, "Bearer admin");
        let state = |app: &Router| {
            let app = app.clone();
            async move {
                use tower::ServiceExt;
                let res = app.oneshot(get("/api/state")).await.unwrap();
                let body = res.into_body().collect().await.unwrap().to_bytes();
                serde_json::from_slice::<serde_json::Value>(&body).unwrap()
            }
        };
        std::fs::write(dir.join("f.mp4"), fake_mp4(600_000)).unwrap();

        // A film that started 100 s ago.
        let started = now_ms() - 100_000;
        let body = format!(r#"{{"file":"f.mp4","start_ms":{started},"loop":false}}"#);
        let req = admin(Request::post("/api/schedule"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.into())
            .unwrap();
        assert_eq!(call(&app, req).await.0, StatusCode::OK);

        let req = admin(Request::post("/api/intermission"))
            .body(Default::default())
            .unwrap();
        assert_eq!(call(&app, req).await.0, StatusCode::OK);
        let s = state(&app).await;
        assert!(s["intermission_since_ms"].is_i64());
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let req = admin(Request::delete("/api/intermission"))
            .body(Default::default())
            .unwrap();
        assert_eq!(call(&app, req).await.0, StatusCode::OK);
        let s = state(&app).await;
        assert!(s["intermission_since_ms"].is_null());
        // Moved forward by the intermission's length (50 ms+) plus the 10 s rewind.
        let shifted = s["schedule"]["start_ms"].as_i64().unwrap() - started;
        assert!(
            (10_050..12_000).contains(&shifted),
            "start moved by {shifted} ms"
        );

        // A film paused 2 s in cannot rewind 10 s: it resumes from its start.
        let just_started = now_ms() - 2_000;
        let body = format!(r#"{{"file":"f.mp4","start_ms":{just_started},"loop":false}}"#);
        let req = admin(Request::post("/api/schedule"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.into())
            .unwrap();
        assert_eq!(call(&app, req).await.0, StatusCode::OK);
        let req = admin(Request::post("/api/intermission"))
            .body(Default::default())
            .unwrap();
        call(&app, req).await;
        let req = admin(Request::delete("/api/intermission"))
            .body(Default::default())
            .unwrap();
        call(&app, req).await;
        let s = state(&app).await;
        let start = s["schedule"]["start_ms"].as_i64().unwrap();
        assert!(
            start <= now_ms() && start > now_ms() - 500,
            "resumes from the beginning, not before it"
        );

        // A film that has not started keeps its countdown.
        let future = now_ms() + 100_000;
        let body = format!(r#"{{"file":"f.mp4","start_ms":{future},"loop":false}}"#);
        let req = admin(Request::post("/api/schedule"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.into())
            .unwrap();
        assert_eq!(call(&app, req).await.0, StatusCode::OK);
        let req = admin(Request::post("/api/intermission"))
            .body(Default::default())
            .unwrap();
        call(&app, req).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let req = admin(Request::delete("/api/intermission"))
            .body(Default::default())
            .unwrap();
        call(&app, req).await;
        let s = state(&app).await;
        assert_eq!(s["schedule"]["start_ms"].as_i64().unwrap(), future);

        // Without the token: refused.
        let req = Request::post("/api/intermission")
            .body(Default::default())
            .unwrap();
        assert_eq!(call(&app, req).await.0, StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn schedule_round_trips_with_loop_key() {
        let s = Schedule {
            items: vec![Item {
                file: "a.mp4".into(),
                duration_ms: 20_000,
            }],
            start_ms: 5,
            loop_: true,
            then: None,
            file: None,
        };
        let j = serde_json::to_string(&s).unwrap();
        assert!(j.contains("\"loop\":true"), "{j}");
        // The legacy name is read, never written: a state file written by
        // this version must not grow a field that migration would re-read.
        assert!(!j.contains("\"file\":null"), "{j}");
        assert_eq!(serde_json::from_str::<Schedule>(&j).unwrap(), s);
    }
}
