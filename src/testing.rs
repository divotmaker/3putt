//! Test support (`testing` feature): an in-memory device a test steps
//! itself, and sample putts.
//!
//! [`FakeDevice`] speaks the device side of the protocol (ZMTP `ROUTER`,
//! `N_OK` replies, `N_FAIL`-carried events) on an in-memory stream, so a
//! [`Client`](crate::Client) or the driver can run without hardware, on one
//! thread and in the test's own time.

use std::io::{self, Read, Write};
use std::sync::{Arc, Mutex, MutexGuard};

use base64::Engine as _;
use serde_json::Value;

use crate::error::Error;
use crate::zmtp::{self, FrameReader};

/// A putt with putter data (direction R1.4°, 2.50 m/s, path R0.6°, face R1.5°).
pub const PUTT_RESULT_FULL: &str = "PuttResult,1.43502296839333,2.50345148852703,1531.8,1.50199074589741,0.55270159796981,1.48146093525773,105,101,101,201,400,599,2.15371070126487,1.48146093525773,1.3619127672204,51";

/// A pulled putt (direction L10.3°, 3.79 m/s, path L5.2°, face L10.5°).
pub const PUTT_RESULT_PULLED: &str = "PuttResult,-10.2790290384019,3.789423622279,965.7,2.04021538616595,-5.16989038221816,-10.5479310729379,90,105,123,166,365,563,-12.8369117616901,-10.5479310729379,-13.4288407664039,51";

/// The device is ready for a putt.
const PUTT_READY: &str = "PuttReady,106";

/// JSON body of a device event carrying `comment`.
pub(crate) fn device_event(comment: &str) -> Vec<u8> {
    serde_json::json!({
        "PacketID": "N_FAIL",
        "RequestPacketID": "N_REQ_CONNECT",
        "Comment": comment,
    })
    .to_string()
    .into_bytes()
}

/// JSON body of the device's `N_OK` reply to `request_packet_id`.
fn device_ack(request_packet_id: &str) -> Vec<u8> {
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
pub(crate) fn fake_jpeg(len: usize) -> (Vec<u8>, String) {
    let mut jpeg = vec![0xFF, 0xD8, 0xFF, 0xE0];
    jpeg.extend((0..len.saturating_sub(4)).map(|i| u8::try_from(i % 251).unwrap_or(0)));
    jpeg.truncate(len.max(4));
    let text = base64::engine::general_purpose::STANDARD.encode(&jpeg);
    (jpeg, text)
}

/// A packet the [`FakeDevice`] received from the client.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReceivedPacket {
    /// `PacketID`.
    pub packet_id: String,
    /// `Comment`.
    pub comment: Option<String>,
    /// The body exactly as received.
    pub json: String,
}

/// A scripted ExPutt RG on an in-memory stream: a `Read + Write` to hand
/// to [`Client::new`](crate::Client::new) or to the driver's
/// `connect_with`. It speaks the device side of the protocol and answers
/// at once, inside the client's write:
///
/// - the greeting is waiting to be read from the start;
/// - `N_VERIFY_ID` and `N_REQ_CONNECT` with a heartbeat, then `N_OK`
///   (unless [`unanswering`](Self::unanswering));
/// - `resyncToMat` with one camera frame, then `FindMat` and `MatOK`
///   (unless [`set_find_mat`](Self::set_find_mat) is off);
/// - match begin with `IndiCatorOut` and `PuttReady`; match end with
///   `IndiCatorOut`.
///
/// Nothing happens on its own: the test pushes heartbeats and putts with
/// [`push_event`](Self::push_event). Reads return `WouldBlock` when nothing
/// is waiting. Clones share the device: keep one to push events and read
/// what the client sent while the client owns another.
///
/// ```
/// use threeputt::testing::{FakeDevice, PUTT_RESULT_FULL};
/// use threeputt::{Client, Event, Hand};
///
/// let device = FakeDevice::new();
/// let mut client = Client::new(device.clone(), Hand::Right);
/// // Time moves only when the test moves it.
/// let now = std::time::Instant::now();
/// while client.poll_at(now).unwrap() != Some(Event::PuttReady) {}
/// device.push_event(PUTT_RESULT_FULL);
/// let Some(Event::PuttResult(putt)) = client.poll_at(now).unwrap() else {
///     panic!("a putt");
/// };
/// assert!(putt.ball_speed_mps > 2.5);
/// ```
#[derive(Debug, Clone)]
pub struct FakeDevice(Arc<Mutex<Fake>>);

// Independent switches of a scripted device.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct Fake {
    /// What the client wrote, framed as it arrives.
    inbox: FrameReader<std::collections::VecDeque<u8>>,
    greeted: bool,
    /// Wire bytes waiting to be read by the client.
    outbox: std::collections::VecDeque<u8>,
    received: Vec<ReceivedPacket>,
    answer_handshake: bool,
    find_mat: bool,
    disconnected: bool,
}

impl Default for FakeDevice {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeDevice {
    /// A device that answers the whole session.
    #[must_use]
    pub fn new() -> Self {
        let device = Fake {
            inbox: FrameReader::new(std::collections::VecDeque::new()),
            greeted: false,
            outbox: zmtp::greeting(zmtp::SOCKET_ROUTER).into(),
            received: Vec::new(),
            answer_handshake: true,
            find_mat: true,
            disconnected: false,
        };
        Self(Arc::new(Mutex::new(device)))
    }

    /// A device that greets and then never answers the handshake.
    #[must_use]
    pub fn unanswering() -> Self {
        let device = Self::new();
        device.state().answer_handshake = false;
        device
    }

    /// Whether to answer `resyncToMat` with `FindMat` then `MatOK` from now
    /// on. On by default.
    pub fn set_find_mat(&self, on: bool) {
        self.state().find_mat = on;
    }

    /// Send a device event with this comment, e.g. [`PUTT_RESULT_FULL`] or
    /// `Test,yy` (a heartbeat).
    pub fn push_event(&self, comment: &str) {
        self.state().push(&device_event(comment));
    }

    /// Close the connection from the device's side: the client reads what
    /// was sent before, then the end of the stream; its writes fail.
    pub fn disconnect(&self) {
        self.state().disconnected = true;
    }

    /// Every packet the client has sent, in order.
    #[must_use]
    pub fn received(&self) -> Vec<ReceivedPacket> {
        self.state().received.clone()
    }

    fn state(&self) -> MutexGuard<'_, Fake> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Fake {
    fn push(&mut self, body: &[u8]) {
        self.outbox.extend(zmtp::encode_frame(body));
    }

    /// Answer what the client sent so far.
    fn answer(&mut self) -> io::Result<()> {
        if !self.greeted {
            match self.inbox.read_greeting() {
                Ok(_) => self.greeted = true,
                Err(Error::Closed) => return Ok(()),
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            }
        }
        loop {
            let body = match self.inbox.read_message() {
                Ok(body) => body,
                Err(Error::Closed) => return Ok(()),
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            };
            let packet = parse_packet(&body);
            match (packet.packet_id.as_str(), packet.comment.as_deref()) {
                (id @ ("N_VERIFY_ID" | "N_REQ_CONNECT"), _) if self.answer_handshake => {
                    self.push(&device_event("Test,yy"));
                    self.push(&device_ack(id));
                }
                ("N_TEST", Some("resyncToMat")) => {
                    self.push(&device_event(&fake_jpeg(60_000).1));
                    if self.find_mat {
                        self.push(&device_event("FindMat,101"));
                        self.push(&device_event("MatOK,102"));
                    }
                }
                ("N_TEST", Some("PM_API_MatchBegin")) => {
                    self.push(&device_event("IndiCatorOut,110"));
                    self.push(&device_event(PUTT_READY));
                }
                ("N_TEST", Some("PM_API_MatchEnded")) => {
                    self.push(&device_event("IndiCatorOut,110"));
                }
                _ => {}
            }
            self.received.push(packet);
        }
    }
}

fn parse_packet(body: &[u8]) -> ReceivedPacket {
    let value: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    ReceivedPacket {
        packet_id: value["PacketID"].as_str().unwrap_or_default().to_owned(),
        comment: value["Comment"].as_str().map(str::to_owned),
        json: String::from_utf8_lossy(body).into_owned(),
    }
}

impl Read for FakeDevice {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut st = self.state();
        if st.outbox.is_empty() {
            return if st.disconnected {
                Ok(0)
            } else {
                Err(io::ErrorKind::WouldBlock.into())
            };
        }
        let n = buf.len().min(st.outbox.len());
        for (b, v) in buf.iter_mut().zip(st.outbox.drain(..n)) {
            *b = v;
        }
        Ok(n)
    }
}

impl Write for FakeDevice {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut st = self.state();
        if st.disconnected {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        st.inbox.get_mut().extend(buf);
        st.answer()?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
