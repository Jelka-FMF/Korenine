use futures_util::{SinkExt, StreamExt};
use http::HeaderValue;
use std::fs::OpenOptions;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use nix::sys::stat::Mode;
use nix::unistd;
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader as AsyncBufReader};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{info, warn, error, debug, instrument};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
struct DriverConfig {
    ws_url: String,
    auth_token: String,
    led_count: usize,
    leds_per_device: usize,
    global_pipe: String,
    devices: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
struct Rgb(u8, u8, u8);

impl Rgb {
    fn to_led_bytes(self) -> [u8; 4] {
        [self.1, self.0, self.2, 0x00]
    }
}

fn setup_fifo(path: &str) -> anyhow::Result<()> {
    if Path::new(path).exists() {
        fs::remove_file(path)?;
    }
    unistd::mkfifo(path, Mode::S_IRWXU | Mode::S_IRWXG | Mode::S_IRWXO)?;
    let mut perms = fs::metadata(path)?.permissions();
    perms.set_mode(0o777);
    fs::set_permissions(path, perms)?;
    Ok(())
}

fn decode_frame(frame: &str, led_count: usize) -> Result<Vec<Rgb>, String> {
    let expected_len = led_count * 6;
    if frame.len() != expected_len {
        return Err(format!("Size mismatch: expected {}, got {}", expected_len, frame.len()));
    }
    let mut pixels = Vec::with_capacity(led_count);
    for i in (0..frame.len()).step_by(6) {
        let r = u8::from_str_radix(&frame[i..i+2], 16).map_err(|e| e.to_string())?;
        let g = u8::from_str_radix(&frame[i+2..i+4], 16).map_err(|e| e.to_string())?;
        let b = u8::from_str_radix(&frame[i+4..i+6], 16).map_err(|e| e.to_string())?;
        pixels.push(Rgb(r, g, b));
    }
    Ok(pixels)
}

fn device_writer(dev_path: String, mut rx: mpsc::Receiver<Vec<Rgb>>) {
    while let Some(pixels) = rx.blocking_recv() {
        let mut buffer = Vec::with_capacity(pixels.len() * 4);
        for p in pixels { buffer.extend_from_slice(&p.to_led_bytes()); }
        for _ in 1..=5 {
            if let Ok(mut file) = fs::OpenOptions::new().write(true).open(&dev_path) {
                if file.write_all(&buffer).is_ok() { let _ = file.flush(); break; }
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let settings = config::Config::builder().add_source(config::File::with_name("config")).build()?;
    let cfg: DriverConfig = settings.try_deserialize()?;

    setup_fifo(&cfg.global_pipe)?;

    let mut channels = Vec::new();
    for dev in cfg.devices {
        let (tx, rx) = mpsc::channel::<Vec<Rgb>>(1);
        channels.push(tx);
        std::thread::spawn(move || device_writer(dev, rx));
    }

    let mut request = cfg.ws_url.into_client_request()?;
    request.headers_mut().insert("Sec-WebSocket-Protocol", HeaderValue::from_str(&format!("auth-{}", cfg.auth_token))?);

    info!("Connecting to WebSocket...");
    let (ws_stream, _) = connect_async(request).await?;
    let (mut ws_write, _) = ws_stream.split();

    let file = OpenOptions::new().read(true).write(true).open(&cfg.global_pipe)?;
    let mut reader = AsyncBufReader::new(File::from_std(file)).lines();

    while let Ok(Some(line)) = reader.next_line().await {
        let trimmed = line.trim();
        if trimmed.is_empty() || !trimmed.starts_with('#') || trimmed.starts_with("#{") { continue; }

        let _ = ws_write.send(Message::Text(trimmed.to_string())).await;

        if let Ok(full_frame) = decode_frame(&trimmed[1..], cfg.led_count) {
            for (i, tx) in channels.iter().enumerate() {
                let start = i * cfg.leds_per_device;
                let end = (start + cfg.leds_per_device).min(full_frame.len());
                let _ = tx.try_send(full_frame[start..end].to_vec());
            }
        }
    }
    Ok(())
}
