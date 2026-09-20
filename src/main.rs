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
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse,
    },
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio_stream::{wrappers::WatchStream, Stream, StreamExt};
use tower_http::{services::ServeDir, trace::TraceLayer};
use tracing::{info, warn};

const VIEWER_HTML: &str = include_str!("../static/viewer.html");
const ADMIN_HTML: &str = include_str!("../static/admin.html");

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
}

/// What is scheduled. `start_ms` is unix milliseconds on the server clock.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Schedule {
    file: String,
    start_ms: i64,
    #[serde(rename = "loop")]
    loop_: bool,
}

/// The persisted state. `version` bumps on every change so clients can tell
/// a new schedule from a repeat of the current one.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Persisted {
    version: u64,
    schedule: Option<Schedule>,
}

/// What clients see: the state plus the server clock for offset estimation.
#[derive(Serialize)]
struct StateView {
    server_now_ms: i64,
    version: u64,
    schedule: Option<Schedule>,
}

#[derive(Serialize)]
struct FileEntry {
    name: String,
    size: u64,
}

#[derive(Deserialize)]
struct ScheduleRequest {
    file: String,
    /// Absent or null means "now".
    start_ms: Option<i64>,
    #[serde(rename = "loop", default)]
    loop_: bool,
}

struct Inner {
    media_dir: PathBuf,
    state_file: PathBuf,
    admin_token: String,
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
        let snapshot = {
            let mut s = self.state.lock().unwrap();
            s.version += 1;
            s.schedule = schedule;
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

fn load_state(path: &Path) -> Persisted {
    match std::fs::read(path) {
        Ok(bytes) => match serde_json::from_slice(&bytes) {
            Ok(s) => s,
            Err(e) => {
                warn!("{}: unreadable state, starting empty: {e}", path.display());
                Persisted::default()
            }
        },
        Err(_) => Persisted::default(),
    }
}

/// Read the admin token, or mint one: 32 random bytes as hex in a 0600 file.
fn load_or_create_token(path: &Path) -> std::io::Result<String> {
    if let Ok(s) = std::fs::read_to_string(path) {
        let s = s.trim().to_string();
        if !s.is_empty() {
            return Ok(s);
        }
    }
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    let token: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(path)?;
    writeln!(f, "{token}")?;
    info!("created admin token file {}", path.display());
    Ok(token)
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
        });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(out))
}

async fn set_schedule(
    State(app): State<App>,
    headers: HeaderMap,
    Json(req): Json<ScheduleRequest>,
) -> Result<Json<StateView>, (StatusCode, String)> {
    app.require_admin(&headers)
        .map_err(|s| (s, "admin token required".into()))?;
    if !valid_file_name(&req.file) || !playable(&req.file) {
        return Err((StatusCode::BAD_REQUEST, "not a playable file name".into()));
    }
    if !app.media_dir.join(&req.file).is_file() {
        return Err((
            StatusCode::NOT_FOUND,
            "no such file in the media directory".into(),
        ));
    }
    let schedule = Schedule {
        file: req.file,
        start_ms: req.start_ms.unwrap_or_else(now_ms),
        loop_: req.loop_,
    };
    info!(?schedule, "scheduled");
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

fn router(app: App) -> Router {
    let media = ServeDir::new(&app.media_dir);
    Router::new()
        .route("/", get(viewer))
        .route("/admin", get(admin))
        .route("/api/state", get(get_state))
        .route("/api/events", get(events))
        .route("/api/files", get(list_files))
        .route("/api/schedule", post(set_schedule).delete(clear_schedule))
        .nest_service("/media", media)
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
    let admin_token = load_or_create_token(&args.admin_token_file)?;
    let state = load_state(&args.state_file);
    if let Some(s) = &state.schedule {
        info!(?s, "restored schedule");
    }
    let (notify, _) = watch::channel(state.version);

    let app: App = Arc::new(Inner {
        media_dir: args.media_dir.clone(),
        state_file: args.state_file,
        admin_token,
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

    #[test]
    fn schedule_round_trips_with_loop_key() {
        let s = Schedule {
            file: "a.mp4".into(),
            start_ms: 5,
            loop_: true,
        };
        let j = serde_json::to_string(&s).unwrap();
        assert!(j.contains("\"loop\":true"), "{j}");
        assert_eq!(serde_json::from_str::<Schedule>(&j).unwrap(), s);
    }
}
