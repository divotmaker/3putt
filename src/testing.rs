//! Test support (`testing` feature): an in-process mock device and sample
//! data.
//!
//! [`MockDevice`] listens on a local TCP port and speaks the device side of
//! the protocol (ZMTP `ROUTER`, `N_OK` replies, `N_FAIL`-carried events), so
//! [`Session`](crate::Session) and [`Connection`](crate::Connection) can be
//! exercised without hardware. Like the device, it streams camera frames from
//! `resyncToMat` until it receives `stopSendImage`.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use base64::Engine as _;
use serde_json::Value;

use crate::error::Error;
use crate::zmtp::{self, FrameReader};

/// A putt with putter data (direction R1.4°, 2.50 m/s, path R0.6°, face R1.5°).
pub const PUTT_RESULT_FULL: &str = "PuttResult,1.43502296839333,2.50345148852703,1531.8,1.50199074589741,0.55270159796981,1.48146093525773,105,101,101,201,400,599,2.15371070126487,1.48146093525773,1.3619127672204,51";

/// A putt without putter data (direction R0.6°, 1.81 m/s).
pub const PUTT_RESULT_BALL_ONLY: &str = "PuttResult,0.561243876918884,1.80518847921913,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999";

/// A pulled putt (direction L10.3°, 3.79 m/s, path L5.2°, face L10.5°).
pub const PUTT_RESULT_PULLED: &str = "PuttResult,-10.2790290384019,3.789423622279,965.7,2.04021538616595,-5.16989038221816,-10.5479310729379,90,105,123,166,365,563,-12.8369117616901,-10.5479310729379,-13.4288407664039,51";

/// The device is ready for a putt ([`Event::PuttReady`](crate::Event::PuttReady)).
pub const PUTT_READY: &str = "PuttReady,106";

/// The ball was struck ([`Event::BallStruck`](crate::Event::BallStruck)).
pub const BALL_STRUCK: &str = "PuttShot,107";

/// JSON body of a device event carrying `comment`.
#[must_use]
pub fn device_event(comment: &str) -> Vec<u8> {
    serde_json::json!({
        "PacketID": "N_FAIL",
        "RequestPacketID": "N_REQ_CONNECT",
        "Comment": comment,
    })
    .to_string()
    .into_bytes()
}

/// JSON body of the device's `N_OK` reply to `request_packet_id`.
#[must_use]
pub fn device_ack(request_packet_id: &str) -> Vec<u8> {
    serde_json::json!({
        "PacketID": "N_OK",
        "RequestPacketID": request_packet_id,
        "Comment": "_INVANT_EXPUTT",
    })
    .to_string()
    .into_bytes()
}

/// A fake JPEG of `len` bytes (starts with the JPEG SOI marker), as the
/// device would send it: the bytes and their base64 text.
#[must_use]
pub fn fake_jpeg(len: usize) -> (Vec<u8>, String) {
    let mut jpeg = vec![0xFF, 0xD8, 0xFF, 0xE0];
    jpeg.extend((0..len.saturating_sub(4)).map(|i| u8::try_from(i % 251).unwrap_or(0)));
    jpeg.truncate(len.max(4));
    let text = base64::engine::general_purpose::STANDARD.encode(&jpeg);
    (jpeg, text)
}

/// Behaviour of a [`MockDevice`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MockConfig {
    /// Reply `N_OK` to `N_VERIFY_ID` and `N_REQ_CONNECT`. Default on.
    pub answer_handshake: bool,
    /// Answer `resyncToMat` with `FindMat` then `MatOK`. Default on; see
    /// also [`MockDevice::set_find_mat`].
    pub find_mat: bool,
    /// Period of the camera frames streamed from `resyncToMat` until
    /// `stopSendImage` (the first frame goes out immediately). Default 20 ms.
    pub frame_interval: Duration,
    /// Size of each camera frame in bytes. Default 60 000 (a long ZMTP frame).
    pub camera_frame_len: usize,
    /// Period of the `Test,yy` heartbeat once the client has connected.
    /// Default 250 ms (the device sends one about every second).
    pub heartbeat: Option<Duration>,
}

impl Default for MockConfig {
    fn default() -> Self {
        Self {
            answer_handshake: true,
            find_mat: true,
            frame_interval: Duration::from_millis(20),
            camera_frame_len: 60_000,
            heartbeat: Some(Duration::from_millis(250)),
        }
    }
}

/// A packet the mock device received from the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedPacket {
    /// `PacketID`.
    pub packet_id: String,
    /// `Comment`.
    pub comment: Option<String>,
    /// The body exactly as received.
    pub json: String,
}

#[derive(Debug, Default)]
struct Log {
    greeting: Vec<u8>,
    packets: Vec<ReceivedPacket>,
    frames_sent: usize,
    closed: bool,
}

/// An in-process mock device accepting one client connection.
#[derive(Debug)]
pub struct MockDevice {
    addr: SocketAddr,
    log: Arc<Mutex<Log>>,
    outbox: Sender<Vec<u8>>,
    find_mat: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl MockDevice {
    /// Listen on `127.0.0.1` (ephemeral port) and serve one client.
    ///
    /// # Errors
    ///
    /// Socket errors.
    pub fn spawn(config: MockConfig) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        let log = Arc::new(Mutex::new(Log::default()));
        let stop = Arc::new(AtomicBool::new(false));
        let find_mat = Arc::new(AtomicBool::new(config.find_mat));
        let (outbox, inbox) = mpsc::channel();
        let thread = {
            let log = Arc::clone(&log);
            let find_mat = Arc::clone(&find_mat);
            let stop = Arc::clone(&stop);
            thread::Builder::new()
                .name("threeputt-mock".into())
                .spawn(move || serve(&listener, &config, &find_mat, &log, &inbox, &stop))?
        };
        Ok(Self {
            addr,
            log,
            outbox,
            find_mat,
            stop,
            thread: Some(thread),
        })
    }

    /// Address the mock listens on.
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Port the mock listens on.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// Send a device event (`N_FAIL` carrier) with this comment, e.g.
    /// [`PUTT_RESULT_FULL`].
    pub fn push_event(&self, comment: &str) {
        self.push_raw(device_event(comment));
    }

    /// Send an arbitrary message body.
    pub fn push_raw(&self, body: Vec<u8>) {
        let _ = self.outbox.send(body);
    }

    /// Whether to answer `resyncToMat` with `FindMat` then `MatOK` from now
    /// on ([`MockConfig::find_mat`]).
    pub fn set_find_mat(&self, on: bool) {
        self.find_mat.store(on, Ordering::SeqCst);
    }

    /// Close the client connection from the device side.
    pub fn disconnect(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    /// Raw greeting bytes the client sent (signature through identity
    /// frame).
    #[must_use]
    pub fn client_greeting(&self) -> Vec<u8> {
        lock(&self.log).greeting.clone()
    }

    /// Every packet received so far, in order.
    #[must_use]
    pub fn received(&self) -> Vec<ReceivedPacket> {
        lock(&self.log).packets.clone()
    }

    /// Number of camera frames the mock has streamed so far.
    #[must_use]
    pub fn camera_frames_sent(&self) -> usize {
        lock(&self.log).frames_sent
    }

    /// Whether the client has closed the connection.
    #[must_use]
    pub fn client_closed(&self) -> bool {
        lock(&self.log).closed
    }

    /// Poll until `done(received packets)` holds, for at most `timeout`.
    /// Returns whether it held.
    #[must_use]
    pub fn wait_until(&self, timeout: Duration, done: impl Fn(&[ReceivedPacket]) -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if done(&lock(&self.log).packets) {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(5));
        }
    }

    /// Poll until the client closes the connection, for at most `timeout`.
    /// Returns whether it closed.
    #[must_use]
    pub fn wait_closed(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.client_closed() {
                return true;
            }
            thread::sleep(Duration::from_millis(5));
        }
        self.client_closed()
    }
}

impl Drop for MockDevice {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn lock(log: &Mutex<Log>) -> MutexGuard<'_, Log> {
    log.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn serve(
    listener: &TcpListener,
    config: &MockConfig,
    find_mat: &AtomicBool,
    log: &Mutex<Log>,
    inbox: &Receiver<Vec<u8>>,
    stop: &AtomicBool,
) {
    let stream = loop {
        if stop.load(Ordering::SeqCst) {
            return;
        }
        match listener.accept() {
            Ok((s, _)) => break s,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(_) => return,
        }
    };
    let _ = run_connection(stream, config, find_mat, log, inbox, stop);
    lock(log).closed = true;
}

fn run_connection(
    stream: TcpStream,
    config: &MockConfig,
    find_mat: &AtomicBool,
    log: &Mutex<Log>,
    inbox: &Receiver<Vec<u8>>,
    stop: &AtomicBool,
) -> io::Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_nodelay(true)?;
    let mut out = stream.try_clone()?;

    // Client greeting: 12 bytes, then a short identity frame (flags, length,
    // body).
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let mut greeting = vec![0u8; 14];
    (&stream).read_exact(&mut greeting)?;
    let mut identity = vec![0u8; usize::from(greeting[13])];
    (&stream).read_exact(&mut identity)?;
    greeting.extend_from_slice(&identity);
    lock(log).greeting = greeting;
    out.write_all(&zmtp::greeting(zmtp::SOCKET_ROUTER))?;

    stream.set_read_timeout(Some(Duration::from_millis(10)))?;
    let mut frames = FrameReader::new(stream);
    let mut connected = false;
    let mut next_heartbeat = Instant::now();
    let mut streaming = false;
    let mut next_frame = Instant::now();
    loop {
        if stop.load(Ordering::SeqCst) {
            return frames.get_ref().shutdown(std::net::Shutdown::Both);
        }
        while let Ok(body) = inbox.try_recv() {
            zmtp::write_frame(&mut out, &body)?;
        }
        if let Some(period) = config.heartbeat
            && connected
            && Instant::now() >= next_heartbeat
        {
            zmtp::write_frame(&mut out, &device_event("Test,yy"))?;
            next_heartbeat = Instant::now() + period;
        }
        if streaming && Instant::now() >= next_frame {
            send_frame(&mut out, config, log)?;
            next_frame = Instant::now() + config.frame_interval;
        }
        match frames.read_message() {
            Ok(body) => {
                let packet = record(log, &body);
                connected |= packet.packet_id == "N_REQ_CONNECT";
                match (packet.packet_id.as_str(), packet.comment.as_deref()) {
                    ("N_TEST", Some("resyncToMat")) => {
                        streaming = true;
                        send_frame(&mut out, config, log)?;
                        next_frame = Instant::now() + config.frame_interval;
                    }
                    ("N_TEST", Some("stopSendImage")) => streaming = false,
                    _ => {}
                }
                for reply in replies(config, find_mat.load(Ordering::SeqCst), &packet) {
                    zmtp::write_frame(&mut out, &reply)?;
                }
            }
            Err(Error::Io(e))
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(_) => return Ok(()),
        }
    }
}

fn send_frame(out: &mut TcpStream, config: &MockConfig, log: &Mutex<Log>) -> io::Result<()> {
    zmtp::write_frame(out, &device_event(&fake_jpeg(config.camera_frame_len).1))?;
    lock(log).frames_sent += 1;
    Ok(())
}

fn record(log: &Mutex<Log>, body: &[u8]) -> ReceivedPacket {
    let json = String::from_utf8_lossy(body).into_owned();
    let value: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let packet = ReceivedPacket {
        packet_id: value["PacketID"].as_str().unwrap_or_default().to_owned(),
        comment: value["Comment"].as_str().map(str::to_owned),
        json,
    };
    lock(log).packets.push(packet.clone());
    packet
}

fn replies(config: &MockConfig, find_mat: bool, packet: &ReceivedPacket) -> Vec<Vec<u8>> {
    match (packet.packet_id.as_str(), packet.comment.as_deref()) {
        (id @ ("N_VERIFY_ID" | "N_REQ_CONNECT"), _) if config.answer_handshake => {
            // A heartbeat first: the client must skip it while waiting.
            vec![device_event("Test,yy"), device_ack(id)]
        }
        ("N_TEST", Some("resyncToMat")) if find_mat => {
            vec![device_event("FindMat,101"), device_event("MatOK,102")]
        }
        ("N_TEST", Some("PM_API_MatchBegin")) => {
            vec![device_event("IndiCatorOut,110"), device_event(PUTT_READY)]
        }
        ("N_TEST", Some("PM_API_MatchEnded")) => vec![device_event("IndiCatorOut,110")],
        _ => Vec::new(),
    }
}
