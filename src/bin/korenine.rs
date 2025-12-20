use anyhow::{Context, Result};
use bollard::container::{Config as DockerConfig, CreateContainerOptions, StartContainerOptions, StopContainerOptions, RemoveContainerOptions, ListContainersOptions, LogsOptions};
use bollard::models::HostConfig;
use bollard::auth::DockerCredentials;
use bollard::Docker;
use chrono::{DateTime, Utc, Duration as ChronoDuration};
use futures_util::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{SqlitePool, FromRow};
use std::sync::Arc;
use tokio::sync::{Mutex, oneshot};
use tokio::time::{sleep, Duration};
use tokio_util::io::StreamReader;
use tokio::io::{AsyncBufReadExt, BufReader, AsyncWriteExt};
use tracing::{error, info, warn, debug};
use uuid::Uuid;
use std::str::FromStr;
use std::path::Path;
use std::os::unix::fs::PermissionsExt;
use nix::unistd::{getuid, getgid, mkfifo};
use nix::sys::stat::Mode;

// --- Config & Models ---

#[derive(Debug, serde::Deserialize, Clone)]
struct ManagerConfig {
    server_addr: String,
    token: String,
    docker_socket: String,
    global_pipe: String, // Renamed for clarity: this is where throttler writes TO
    position_location: String,
    db_url: String,
    load_time_secs: i64,
    registry_user: String,
    registry_pass: String,
}

#[derive(Debug, Serialize, Deserialize, FromRow, Clone)]
struct Pattern {
    identifier: String,
    name: String,
    docker_image: String,
    duration: i64,
    enabled: bool,
    changed: DateTime<Utc>,
    last_run: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct RemotePattern {
    identifier: String,
    name: String,
    #[serde(default)]
    description: Option<String>,
    docker: String,
    duration: i64,
    author: String,
    #[serde(default)]
    school: Option<String>,
    changed: String,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    visible: bool,
}

#[derive(Debug, Deserialize)]
struct PatternHeader {
    fps: Option<f64>,
}

struct AppState {
    db: SqlitePool,
    interruption: Option<String>,
    config: ManagerConfig,
}

// --- Throttler Task ---

async fn start_throttler(
    pattern_pipe_path: String,
    global_pipe_path: String,
    expected_leds: usize,
    ready_tx: oneshot::Sender<()>,
) -> Result<()> {
    // 1. Open global pipe (Output)
    let file_out_std = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .open(&global_pipe_path)
        .context("Global pipe missing. Driver running?")?;
    let mut writer = tokio::fs::File::from_std(file_out_std);

    // 2. Open pattern pipe (Input)
    let _ = ready_tx.send(()); 
    
    let file_in = tokio::fs::File::open(&pattern_pipe_path).await
        .context("Failed to open pattern FIFO")?;
    let mut reader = BufReader::new(file_in).lines();

    let mut fps = 60.0;
    let mut header_received = false;
    let expected_len = 1 + (expected_leds * 6);

    while let Ok(Some(line)) = reader.next_line().await {
        let trimmed = line.trim();
        if trimmed.is_empty() { continue; }

        if !header_received && trimmed.starts_with("#{") {
            if let Ok(h) = serde_json::from_str::<PatternHeader>(trimmed) {
                if let Some(f) = h.fps {
                    fps = f.clamp(1.0, 144.0);
                    info!("Throttler: Adjusted to {} FPS", fps);
                }
            }
            header_received = true;
            continue;
        }

        if !trimmed.starts_with('#') || trimmed.len() != expected_len {
            continue;
        }

        let start = tokio::time::Instant::now();
        let frame_duration = Duration::from_secs_f64(1.0 / fps);

        let _ = writer.write_all(format!("{}\n", trimmed).as_bytes()).await;

        let elapsed = start.elapsed();
        if elapsed < frame_duration {
            sleep(frame_duration - elapsed).await;
        }
    }
    Ok(())
}

// --- Database & Pattern Logic ---

async fn get_next_pattern(db: &SqlitePool, current: Option<&Pattern>) -> Result<Option<Pattern>> {
    match current {
        None => {
            sqlx::query_as::<_, Pattern>("SELECT * FROM patterns WHERE enabled = 1 ORDER BY last_run ASC LIMIT 1")
                .fetch_optional(db).await.map_err(Into::into)
        }
        Some(curr) => {
            let next = sqlx::query_as::<_, Pattern>(
                "SELECT * FROM patterns WHERE enabled = 1 AND identifier > ? ORDER BY identifier ASC LIMIT 1"
            ).bind(&curr.identifier).fetch_optional(db).await?;

            if let Some(p) = next {
                Ok(Some(p))
            } else {
                sqlx::query_as::<_, Pattern>("SELECT * FROM patterns WHERE enabled = 1 ORDER BY identifier ASC LIMIT 1")
                    .fetch_optional(db).await.map_err(Into::into)
            }
        }
    }
}

async fn process_patterns_update(db: &SqlitePool, docker: &Docker, config: &ManagerConfig, remote_patterns: Vec<RemotePattern>) -> Result<()> {
    info!("Syncing {} patterns from remote...", remote_patterns.len());
    let creds = DockerCredentials {
        username: Some(config.registry_user.clone()),
        password: Some(config.registry_pass.clone()),
        ..Default::default()
    };
    
    let local_patterns = sqlx::query_as::<_, Pattern>("SELECT * FROM patterns").fetch_all(db).await?;

    for rp in &remote_patterns {
        let changed_date = DateTime::parse_from_rfc3339(&rp.changed)?.with_timezone(&Utc);
        let existing = local_patterns.iter().find(|p| p.identifier == rp.identifier);

        if existing.is_none() || changed_date > existing.unwrap().changed {
            info!("Updating image: {}", rp.identifier);
            let mut pull = docker.create_image(
                Some(bollard::image::CreateImageOptions { from_image: rp.docker.clone(), ..Default::default() }),
                None,
                Some(creds.clone())
            );
            while let Some(_) = pull.next().await {}

            sqlx::query("INSERT INTO patterns (identifier, name, docker_image, duration, enabled, changed, last_run) 
                         VALUES (?,?,?,?,?,?,?) ON CONFLICT(identifier) DO UPDATE SET 
                            name=excluded.name, docker_image=excluded.docker_image, 
                            duration=excluded.duration, changed=excluded.changed, enabled=excluded.enabled")
                .bind(&rp.identifier).bind(&rp.name).bind(&rp.docker).bind(rp.duration)
                .bind(rp.enabled).bind(changed_date).bind(Utc::now()).execute(db).await?;
        }
    }

    let remote_ids: std::collections::HashSet<&String> = remote_patterns.iter().map(|rp| &rp.identifier).collect();
    for lp in local_patterns {
        if !remote_ids.contains(&lp.identifier) {
            info!("Removing obsolete pattern: {}", lp.identifier);
            let _ = sqlx::query("DELETE FROM patterns WHERE identifier = ?").bind(lp.identifier).execute(db).await;
        }
    }
    Ok(())
}

// --- Container Helpers ---

async fn prepare_container(docker: &Docker, p: &Pattern, cfg: &ManagerConfig) -> Result<(String, String)> {
    let run_id = Uuid::new_v4().simple().to_string();
    let container_name = format!("jelka-runner-{}", run_id);
    let pattern_pipe = format!("/tmp/jelka_p_{}", run_id);
    
    // FIXED: Using .as_raw() for nix 0.27 wrappers
    let uid = getuid().as_raw().to_string();
    let gid = getgid().as_raw().to_string();

    if !Path::new(&pattern_pipe).exists() {
        mkfifo(pattern_pipe.as_str(), Mode::S_IRWXU | Mode::S_IRWXG | Mode::S_IRWXO)?;
        std::fs::set_permissions(&pattern_pipe, std::fs::Permissions::from_mode(0o777))?;
    }

    let config = DockerConfig {
        image: Some(p.docker_image.clone()),
        user: Some(format!("{}:{}", uid, gid)),
        host_config: Some(HostConfig {
            binds: Some(vec![
                format!("{}:{}", pattern_pipe, cfg.global_pipe),
                format!("{}:/app/positions.csv:ro", cfg.position_location)
            ]),
            network_mode: Some("host".into()), ..Default::default()
        }),
        env: Some(vec![format!("JELKA_POSITIONS=/app/positions.csv")]), ..Default::default()
    };
    docker.create_container(Some(CreateContainerOptions { name: container_name.as_str(), ..Default::default() }), config).await?;
    Ok((container_name, pattern_pipe))
}

async fn get_container_logs(docker: &Docker, container_name: &str) -> String {
    let mut logs = docker.logs(container_name, Some(LogsOptions::<String> { 
        stdout: true, stderr: true, tail: "20".into(), ..Default::default() 
    }));
    let mut output = String::new();
    while let Some(Ok(log)) = logs.next().await { 
        output.push_str(&log.to_string()); 
    }
    output
}

// --- Background Tasks ---

async fn sse_listener(state_lock: Arc<Mutex<AppState>>, docker: Docker) {
    let client = reqwest::Client::new();
    loop {
        let (url, token) = { 
            let s = state_lock.lock().await; 
            (format!("{}/runner/events/control", s.config.server_addr), s.config.token.clone()) 
        };

        if let Ok(res) = client.get(&url).header("Authorization", format!("Token {}", token)).header("Accept", "text/event-stream").send().await {
            info!("SSE Connected.");
            let stream = res.bytes_stream().map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
            let reader = StreamReader::new(stream);
            let mut lines = BufReader::new(reader).lines();
            
            let mut current_event = String::new(); 
            let mut data_buffer = String::new();

            while let Ok(Some(line)) = lines.next_line().await {
                if line.is_empty() {
                    if !data_buffer.is_empty() {
                        match current_event.as_str() {
                            "run" => if let Ok(v) = serde_json::from_str::<serde_json::Value>(&data_buffer) {
                                if let Some(id) = v["identifier"].as_str() { 
                                    state_lock.lock().await.interruption = Some(id.to_string()); 
                                }
                            },
                            "patterns" => if let Ok(rem) = serde_json::from_str::<Vec<RemotePattern>>(&data_buffer) {
                                let (db, d, cfg) = { let s = state_lock.lock().await; (s.db.clone(), docker.clone(), s.config.clone()) };
                                tokio::spawn(async move { let _ = process_patterns_update(&db, &d, &cfg, rem).await; });
                            },
                            _ => {}
                        }
                    }
                    data_buffer.clear(); current_event.clear();
                } else if let Some(e) = line.strip_prefix("event: ") { current_event = e.trim().to_string(); }
                else if let Some(d) = line.strip_prefix("data: ") { data_buffer.push_str(d.trim()); }
            }
        }
        sleep(Duration::from_secs(5)).await;
    }
}

// --- Main Runner ---

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("Jelka Manager Initializing...");

    let settings = config::Config::builder().add_source(config::File::with_name("config")).build()?;
    let config: ManagerConfig = settings.try_deserialize()?;

    std::fs::create_dir_all("data")?;
    let conn = SqliteConnectOptions::from_str(&config.db_url)?.create_if_missing(true).journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
    let pool = SqlitePoolOptions::new().connect_with(conn).await?;
    sqlx::query("CREATE TABLE IF NOT EXISTS patterns (identifier TEXT PRIMARY KEY, name TEXT, docker_image TEXT, duration INTEGER, enabled BOOLEAN, changed DATETIME, last_run DATETIME)").execute(&pool).await?;

    let docker = Docker::connect_with_unix(&config.docker_socket, 120, bollard::API_DEFAULT_VERSION)?;
    let containers = docker.list_containers(Some(ListContainersOptions::<String> { all: true, ..Default::default() })).await?;
    for c in containers {
        if let Some(names) = c.names {
            if names.iter().any(|n| n.contains("jelka-runner-")) {
                let _ = docker.remove_container(c.id.as_ref().unwrap(), Some(RemoveContainerOptions { force: true, ..Default::default() })).await;
            }
        }
    }

    let state = Arc::new(Mutex::new(AppState { db: pool.clone(), interruption: None, config: config.clone() }));
    
    let (db_c, cfg_c, dock_c) = (pool.clone(), config.clone(), docker.clone());
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        if let Ok(res) = client.get(format!("{}/runner/patterns", cfg_c.server_addr)).send().await {
            if let Ok(rem) = res.json::<Vec<RemotePattern>>().await { let _ = process_patterns_update(&db_c, &dock_c, &cfg_c, rem).await; }
        }
    });

    tokio::spawn(sse_listener(state.clone(), docker.clone()));
    let cfg_p = config.clone();
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        loop {
            let _ = client.post(format!("{}/runner/state/ping", cfg_p.server_addr)).header("Authorization", format!("Token {}", cfg_p.token)).send().await;
            sleep(Duration::from_secs(30)).await;
        }
    });

    let mut current_p: Option<Pattern> = None;
    let mut next_pattern_data: Option<(Pattern, String, String)> = None;
    let _ = std::fs::read_dir("/tmp")?.for_each(|entry| {
        if let Ok(entry) = entry {
            let path = entry.path();
            if path.to_str().map_or(false, |s| s.contains("jelka_p_")) {
                let _ = std::fs::remove_file(path);
            }
        }
    });
    loop {
        
        let (p, container_name, p_pipe) = match next_pattern_data.take() {
            Some(data) => data,
            None => {
                let (pat, cfg) = {
                    let mut s = state.lock().await;
                    let id_inter = s.interruption.take();
                    let p_res = if let Some(id) = id_inter {
                        sqlx::query_as::<_, Pattern>("SELECT * FROM patterns WHERE identifier = ?").bind(id).fetch_optional(&s.db).await?
                    } else {
                        get_next_pattern(&s.db, current_p.as_ref()).await?
                    };
                    (p_res, s.config.clone())
                };
                match pat {
                    Some(pat) => { let (name, pipe) = prepare_container(&docker, &pat, &cfg).await?; (pat, name, pipe) },
                    None => { sleep(Duration::from_secs(5)).await; continue; }
                }
            }
        };

        let (ready_tx, ready_rx) = oneshot::channel();
        let t_pipe = p_pipe.clone();
        let g_pipe = config.global_pipe.clone(); // Path to Driver's FIFO
        
        let throttler_handle = tokio::spawn(async move {
            if let Err(e) = start_throttler(t_pipe, g_pipe, 1000, ready_tx).await { 
                error!("Throttler error: {}", e); 
            }
        });

        let _ = ready_rx.await; 
        info!(">>> Starting: {} ({})", p.name, p.identifier);
        
        if let Err(e) = docker.start_container(&container_name, None::<StartContainerOptions<String>>).await {
            error!("Start failed: {}", e); continue;
        }

        let _ = reqwest::Client::new().post(format!("{}/runner/state/started", config.server_addr))
            .header("Authorization", format!("Token {}", config.token))
            .json(&serde_json::json!({"pattern": p.identifier, "started": Utc::now()})).send().await;

        current_p = Some(p.clone());
        let _ = sqlx::query("UPDATE patterns SET last_run = ? WHERE identifier = ?").bind(Utc::now()).bind(&p.identifier).execute(&pool).await;

        let end = Utc::now() + ChronoDuration::seconds(p.duration);
        let preload_time = end - ChronoDuration::seconds(config.load_time_secs);
        let mut preloaded = false;

        while Utc::now() < end {
            if state.lock().await.interruption.is_some() { break; }
            match docker.inspect_container(&container_name, None).await {
                Ok(resp) => if let Some(st) = resp.state { 
                    if st.running == Some(false) { 
                        warn!("Container exited code {:?}. Logs:\n{}", st.exit_code, get_container_logs(&docker, &container_name).await);
                        break; 
                    } 
                },
                Err(_) => break,
            }
            if !preloaded && Utc::now() >= preload_time {
                let s = state.lock().await;
                if let Ok(Some(nxt)) = get_next_pattern(&s.db, Some(&p)).await {
                    info!("Look-ahead: Pre-loading {}", nxt.identifier);
                    if let Ok((name, pipe)) = prepare_container(&docker, &nxt, &s.config).await {
                        next_pattern_data = Some((nxt, name, pipe)); preloaded = true;
                    }
                }
            }
            sleep(Duration::from_millis(500)).await;
        }

        let _ = docker.stop_container(&container_name, Some(StopContainerOptions { t: 2 })).await;
        let _ = docker.remove_container(&container_name, Some(RemoveContainerOptions { force: true, ..Default::default() })).await;
        
        throttler_handle.abort();
        let _ = throttler_handle.await; // Ensure file handles are dropped before deletion

        if let Err(e) = tokio::fs::remove_file(&p_pipe).await {
            warn!("Failed to delete pattern pipe {}: {}", p_pipe, e);
        }
        info!("<<< Finished: {}", p.identifier); 
    }
}
