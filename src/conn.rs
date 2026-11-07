//! Low-level connection: ZMTP greeting, then send [`Request`]s and receive
//! [`Event`]s.

use std::io::Write as _;
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::error::{Error, Result, from_read_error};
use crate::event::Event;
use crate::packet::Request;
use crate::zmtp::{self, FrameReader};

/// Which way a message crossed the socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    /// From the device.
    Rx,
    /// To the device.
    Tx,
}

/// Sees every message body as it crosses the socket, on the thread that
/// moves it: received bodies before they are parsed (camera frames
/// included), sent bodies once written.
pub(crate) type Tap = Arc<dyn Fn(Direction, &[u8]) + Send + Sync>;

/// An optional [`Tap`], printable.
#[derive(Clone, Default)]
struct TapSlot(Option<Tap>);

impl TapSlot {
    fn see(&self, dir: Direction, body: &[u8]) {
        if let Some(tap) = &self.0 {
            tap(dir, body);
        }
    }
}

impl std::fmt::Debug for TapSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.is_some() { "Tap" } else { "None" })
    }
}

/// Write timeout on the protocol socket, so a dead peer cannot block a send
/// forever.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// A connection to the device after the ZMTP greeting.
///
/// Nothing is sent beyond the greeting; call [`handshake`](Self::handshake)
/// (or drive the session yourself with [`send`](Self::send)). Use
/// [`Session`](crate::Session) for the full flow.
#[derive(Debug)]
pub struct Connection {
    reader: EventReader,
    writer: PacketWriter,
}

impl Connection {
    /// Connect to `addr` (the device listens on port
    /// [`DEVICE_PORT`](crate::DEVICE_PORT)) and exchange ZMTP greetings.
    ///
    /// `timeout` bounds the TCP connect and the wait for the device's
    /// greeting.
    ///
    /// # Errors
    ///
    /// Socket errors, [`Error::Timeout`], [`Error::Greeting`].
    pub fn connect(addr: SocketAddr, timeout: Duration) -> Result<Self> {
        let stream = TcpStream::connect_timeout(&addr, timeout)?;
        stream.set_read_timeout(Some(timeout))?;
        let conn = Self::from_stream(stream)?;
        conn.set_read_timeout(None)?;
        Ok(conn)
    }

    /// Exchange ZMTP greetings over an already connected stream.
    ///
    /// # Errors
    ///
    /// Socket errors, [`Error::Timeout`] if the stream has a read timeout and
    /// the device does not greet in time, [`Error::Greeting`].
    pub fn from_stream(stream: TcpStream) -> Result<Self> {
        stream.set_nodelay(true)?;
        stream.set_write_timeout(Some(WRITE_TIMEOUT))?;
        let mut write_half = stream.try_clone()?;
        write_half.write_all(&zmtp::client_greeting())?;
        write_half.flush()?;
        let mut frames = FrameReader::new(stream);
        frames
            .read_greeting()
            .map_err(|e| timeout_from(e, "the device's greeting"))?;
        Ok(Self {
            reader: EventReader {
                frames,
                tap: TapSlot::default(),
            },
            writer: PacketWriter {
                stream: Arc::new(Mutex::new(write_half)),
                tap: TapSlot::default(),
            },
        })
    }

    /// Send one packet.
    ///
    /// # Errors
    ///
    /// Socket errors.
    pub fn send(&self, request: Request) -> Result<()> {
        self.writer.send(request)
    }

    /// Block until the next event arrives (or the read timeout expires).
    ///
    /// # Errors
    ///
    /// [`Error::Closed`] when the device disconnects, [`Error::Timeout`] when
    /// a read timeout is set and expires (the call can be retried without
    /// losing data), socket errors.
    pub fn recv(&mut self) -> Result<Event> {
        self.reader.recv()
    }

    /// Wait up to `timeout` for the next event.
    ///
    /// # Errors
    ///
    /// As [`recv`](Self::recv).
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Event> {
        self.set_read_timeout(Some(timeout.max(Duration::from_millis(1))))?;
        let result = self.recv();
        self.set_read_timeout(None)?;
        result
    }

    /// Set (or clear) the socket read timeout used by [`recv`](Self::recv).
    ///
    /// # Errors
    ///
    /// Socket errors.
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> Result<()> {
        self.reader.frames.get_ref().set_read_timeout(timeout)?;
        Ok(())
    }

    /// Send `request` and wait up to `timeout` for the device's `N_OK` to it.
    /// Other events received meanwhile are discarded.
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`] if no matching `N_OK` arrives in time, socket
    /// errors, [`Error::Closed`].
    pub fn request(&mut self, request: Request, timeout: Duration) -> Result<()> {
        self.send(request)?;
        let deadline = Instant::now() + timeout;
        let waiting_for = match request {
            Request::VerifyId => "the device's handshake reply",
            Request::Connect => "the device's connection reply",
            _ => "the device's reply",
        };
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Timeout(waiting_for));
            }
            match self.recv_timeout(remaining) {
                Ok(Event::Ack { request: id, .. }) if id == request.packet_id() => return Ok(()),
                Ok(_) => {}
                Err(Error::Timeout(_)) => return Err(Error::Timeout(waiting_for)),
                Err(e) => return Err(e),
            }
        }
    }

    /// Identify and join: [`Request::VerifyId`], then [`Request::Connect`],
    /// each waiting up to `timeout` for its `N_OK`.
    ///
    /// # Errors
    ///
    /// As [`request`](Self::request).
    pub fn handshake(&mut self, timeout: Duration) -> Result<()> {
        self.request(Request::VerifyId, timeout)?;
        self.request(Request::Connect, timeout)
    }

    /// Split into a reader (for a receive thread) and a cloneable writer.
    #[must_use]
    pub fn split(self) -> (EventReader, PacketWriter) {
        (self.reader, self.writer)
    }
}

/// Receiving half of a [`Connection`].
#[derive(Debug)]
pub struct EventReader {
    frames: FrameReader<TcpStream>,
    tap: TapSlot,
}

impl EventReader {
    /// Block until the next event arrives.
    ///
    /// # Errors
    ///
    /// As [`Connection::recv`].
    pub fn recv(&mut self) -> Result<Event> {
        self.recv_filtered(true)
    }

    /// As [`recv`](Self::recv); camera frames are skipped, without decoding
    /// them, unless `frames` is set.
    pub(crate) fn recv_filtered(&mut self, frames: bool) -> Result<Event> {
        loop {
            let body = self
                .frames
                .read_message()
                .map_err(|e| timeout_from(e, "an event"))?;
            self.tap.see(Direction::Rx, &body);
            if let Some(event) = Event::parse_filtered(&body, frames) {
                return Ok(event);
            }
        }
    }

    /// Show every message read from now on to `tap`.
    pub(crate) fn set_tap(&mut self, tap: Tap) {
        self.tap = TapSlot(Some(tap));
    }
}

/// Sending half of a [`Connection`]. Clones share the socket; sends are
/// serialized so frames never interleave.
#[derive(Debug, Clone)]
pub struct PacketWriter {
    stream: Arc<Mutex<TcpStream>>,
    tap: TapSlot,
}

impl PacketWriter {
    /// Send one packet.
    ///
    /// # Errors
    ///
    /// Socket errors, [`Error::Poisoned`].
    pub fn send(&self, request: Request) -> Result<()> {
        let body = request.to_json();
        let frame = zmtp::encode_frame(body.as_bytes());
        {
            let mut stream = self.stream.lock().map_err(|_| Error::Poisoned)?;
            stream.write_all(&frame)?;
            stream.flush()?;
        }
        self.tap.see(Direction::Tx, body.as_bytes());
        Ok(())
    }

    /// Show every message sent from now on, through this writer and the
    /// clones made after this call, to `tap`.
    pub(crate) fn set_tap(&mut self, tap: Tap) {
        self.tap = TapSlot(Some(tap));
    }

    /// Shut down both directions of the socket. A thread blocked in
    /// [`EventReader::recv`] then returns.
    ///
    /// # Errors
    ///
    /// Socket errors, [`Error::Poisoned`].
    pub fn shutdown(&self) -> Result<()> {
        let stream = self.stream.lock().map_err(|_| Error::Poisoned)?;
        match stream.shutdown(Shutdown::Both) {
            Err(e) if e.kind() != std::io::ErrorKind::NotConnected => Err(Error::Io(e)),
            _ => Ok(()),
        }
    }
}

/// Turn a read-timeout I/O error into [`Error::Timeout`].
fn timeout_from(err: Error, waiting_for: &'static str) -> Error {
    match err {
        Error::Io(e) => from_read_error(e, waiting_for),
        other => other,
    }
}
