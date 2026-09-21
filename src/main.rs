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
    file: String,
    /// Absent or null means "now".
    start_ms: Option<i64>,
    #[serde(rename = "loop", default)]
    loop_: bool,
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
    let state = load_state(&args.state_file);
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

    fn test_app(viewer_key: Option<&str>) -> Router {
        let dir = std::env::temp_dir().join(format!("multipass-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
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
        let app = test_app(None);
        assert_eq!(call(&app, get("/api/state")).await.0, StatusCode::OK);
    }

    #[tokio::test]
    async fn with_a_viewer_key_everything_needs_the_cookie() {
        let app = test_app(Some("hunter2"));
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
        let app = test_app(Some("abcde"));
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
    async fn an_intermission_pauses_a_running_film_and_not_a_countdown() {
        use http_body_util::BodyExt;
        let app = test_app(None);
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
        let dir = std::env::temp_dir().join(format!("multipass-test-{}", std::process::id()));
        std::fs::write(dir.join("f.mp4"), b"x").unwrap();

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
        // Moved forward by the intermission's length (50 ms+) plus the 5 s rewind.
        let shifted = s["schedule"]["start_ms"].as_i64().unwrap() - started;
        assert!(
            (5_050..7_000).contains(&shifted),
            "start moved by {shifted} ms"
        );

        // A film paused 2 s in cannot rewind 5 s: it resumes from its start.
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
            file: "a.mp4".into(),
            start_ms: 5,
            loop_: true,
        };
        let j = serde_json::to_string(&s).unwrap();
        assert!(j.contains("\"loop\":true"), "{j}");
        assert_eq!(serde_json::from_str::<Schedule>(&j).unwrap(), s);
    }
}
