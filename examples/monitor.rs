//! Print every event from an ExPutt RG.
//!
//! ```text
//! monitor [--ip IP] [--left] [--frames DIR]
//! ```
//!
//! Without `--ip`, waits for the device's discovery beacon (the device must
//! be in mobile mode). `--frames DIR` streams the camera while the device
//! locates the mat and saves each frame as a JPEG. Ctrl-C ends the session
//! cleanly.
//!
//! A plain loop: poll the client, sleep a little when nothing came.

use std::net::{IpAddr, Shutdown, SocketAddr, TcpStream, UdpSocket};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use threeputt::{Beacon, Client, DEVICE_PORT, DISCOVERY_PORT, Event, Hand, PuttResult};

/// The pause between two polls that brought nothing.
const IDLE: Duration = Duration::from_millis(5);

const MPS_TO_MPH: f64 = 2.236_936;

struct Args {
    ip: Option<IpAddr>,
    hand: Hand,
    frames: Option<PathBuf>,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        ip: None,
        hand: Hand::Right,
        frames: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--ip" => {
                let v = it.next().ok_or("--ip needs an address")?;
                args.ip = Some(v.parse().map_err(|e| format!("--ip {v}: {e}"))?);
            }
            "--left" => args.hand = Hand::Left,
            "--frames" => args.frames = Some(it.next().ok_or("--frames needs a directory")?.into()),
            "-h" | "--help" => {
                println!("usage: monitor [--ip IP] [--left] [--frames DIR]");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(args)
}

/// Wall-clock time of day, UTC, `HH:MM:SS.mmmZ`.
fn timestamp() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs() % 86_400;
    format!(
        "{:02}:{:02}:{:02}.{:03}Z",
        secs / 3600,
        secs / 60 % 60,
        secs % 60,
        now.subsec_millis()
    )
}

/// `L 3.4°` / `R 0.6°` for a signed angle, `-` if not measured.
fn side(deg: Option<f64>) -> String {
    match deg {
        Some(d) if d < 0.0 => format!("L {:.1}°", -d),
        Some(d) => format!("R {d:.1}°"),
        None => "-".into(),
    }
}

fn describe_putt(p: &PuttResult) -> String {
    format!(
        "PUTT direction {}, speed {:.2} m/s ({:.1} mph), path {}, face {}",
        side(Some(p.launch_direction_deg)),
        p.ball_speed_mps,
        p.ball_speed_mps * MPS_TO_MPH,
        side(p.path_deg),
        side(p.face_deg),
    )
}

/// Wait up to `timeout` for the device's beacon on UDP port 9999.
fn discover(timeout: Duration) -> std::io::Result<Beacon> {
    let socket = UdpSocket::bind(("0.0.0.0", DISCOVERY_PORT))?;
    let give_up = Instant::now() + timeout;
    let mut buf = [0u8; 2048];
    loop {
        let left = give_up.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(std::io::ErrorKind::TimedOut.into());
        }
        socket.set_read_timeout(Some(left))?;
        let (n, from) = socket.recv_from(&mut buf)?;
        if let Ok(beacon) = Beacon::parse(&buf[..n], from.ip()) {
            return Ok(beacon);
        }
    }
}

/// Say goodbye, then end the connection: shut down the sending direction
/// and read until the device closes its end (at most half a second), so it
/// ends rather than resets.
fn close(mut client: Client<TcpStream>) {
    if let Err(e) = client.close() {
        eprintln!("monitor: close: {e}");
    }
    let stream = client.get_ref();
    let _ = stream.shutdown(Shutdown::Write);
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
    let _ = std::io::copy(&mut &*stream, &mut std::io::sink());
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("monitor: {e}");
            return ExitCode::from(2);
        }
    };

    let ip = if let Some(ip) = args.ip {
        ip
    } else {
        println!("{} waiting for a device beacon on UDP 9999...", timestamp());
        match discover(Duration::from_secs(60)) {
            Ok(b) => {
                println!(
                    "{} found device at {} (version {})",
                    timestamp(),
                    b.ip,
                    b.app_version.as_deref().unwrap_or("?")
                );
                b.ip
            }
            Err(e) => {
                eprintln!("monitor: discovery failed: {e}");
                return ExitCode::FAILURE;
            }
        }
    };

    if let Some(dir) = &args.frames
        && let Err(e) = std::fs::create_dir_all(dir)
    {
        eprintln!("monitor: cannot create {}: {e}", dir.display());
        return ExitCode::FAILURE;
    }

    let running = Arc::new(AtomicBool::new(true));
    {
        let running = Arc::clone(&running);
        if let Err(e) = ctrlc::set_handler(move || running.store(false, Ordering::SeqCst)) {
            eprintln!("monitor: cannot install Ctrl-C handler: {e}");
            return ExitCode::FAILURE;
        }
    }

    println!("{} connecting to {ip}...", timestamp());
    let addr = SocketAddr::new(ip, DEVICE_PORT);
    let stream = match TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .and_then(|s| s.set_nonblocking(true).map(|()| s))
    {
        Ok(s) => s,
        Err(e) => {
            eprintln!("monitor: could not connect to {ip}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut client = Client::new(stream, args.hand);
    client.set_camera(args.frames.is_some());

    let mut frame_no = 0u32;
    while running.load(Ordering::SeqCst) {
        let event = match client.poll() {
            Ok(Some(e)) => e,
            Ok(None) => {
                std::thread::sleep(IDLE);
                continue;
            }
            Err(e) => {
                eprintln!("{} monitor: session ended: {e}", timestamp());
                return ExitCode::FAILURE;
            }
        };
        let ts = timestamp();
        match &event {
            Event::PuttResult(p) => {
                println!("{ts} {event:?}");
                println!("{ts} {}", describe_putt(p));
            }
            Event::CameraFrame(jpeg) => {
                frame_no += 1;
                let saved = args.frames.as_ref().map(|dir| {
                    let path = dir.join(format!("frame-{frame_no:04}.jpg"));
                    match std::fs::write(&path, jpeg) {
                        Ok(()) => format!(" -> {}", path.display()),
                        Err(e) => format!(" (save failed: {e})"),
                    }
                });
                println!(
                    "{ts} CameraFrame #{frame_no} ({} bytes){}",
                    jpeg.len(),
                    saved.unwrap_or_default()
                );
            }
            other => println!("{ts} {other:?}"),
        }
    }

    println!("{} closing session", timestamp());
    close(client);
    ExitCode::SUCCESS
}
