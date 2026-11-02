//! ZMTP 2.0 framing, as spoken on the device's TCP port.
//!
//! The device is a ZeroMQ `ROUTER` socket; this client is a `DEALER`. After a
//! fixed greeting, every message is a single frame whose body is one UTF-8
//! JSON object:
//!
//! ```text
//! greeting:    ff 00 00 00 00 00 00 00 01 7f   signature
//!              01                              revision (ZMTP 2.0)
//!              05 | 06                         socket type (DEALER | ROUTER)
//!              00 00                           empty identity frame
//! short frame: flags (0x00) | len: u8         | body
//! long frame:  flags (0x02) | len: u64 BE     | body
//! ```
//!
//! Flag bit `0x01` (MORE) marks a multipart message. This client never sends
//! one; on input the parts are concatenated into one body.

use std::io::{self, Read, Write};

use crate::error::{Error, Result};

/// Greeting signature: `0xFF`, eight padding bytes, `0x7F`.
pub const SIGNATURE: [u8; 10] = [0xFF, 0, 0, 0, 0, 0, 0, 0, 0x01, 0x7F];

/// ZMTP revision spoken by the device (ZMTP 2.0).
pub const REVISION: u8 = 0x01;

/// Socket type of this client (`DEALER`).
pub const SOCKET_DEALER: u8 = 0x05;

/// Socket type of the device (`ROUTER`).
pub const SOCKET_ROUTER: u8 = 0x06;

/// Largest frame body accepted. Camera frames are around 100 KB; anything
/// beyond this is treated as a corrupt stream.
pub const MAX_FRAME_LEN: u64 = 16 * 1024 * 1024;

const FLAG_MORE: u8 = 0x01;
const FLAG_LONG: u8 = 0x02;

/// The full greeting this client sends: signature, revision, `DEALER`, and an
/// empty identity frame.
#[must_use]
pub fn client_greeting() -> Vec<u8> {
    greeting(SOCKET_DEALER)
}

/// A greeting for the given socket type, with an empty identity frame.
#[must_use]
pub fn greeting(socket_type: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(14);
    out.extend_from_slice(&SIGNATURE);
    out.push(REVISION);
    out.push(socket_type);
    out.extend_from_slice(&[0x00, 0x00]);
    out
}

/// Encode one single-part frame: short form for bodies under 256 bytes,
/// long form otherwise.
#[must_use]
pub fn encode_frame(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 9);
    if let Ok(len) = u8::try_from(body.len()) {
        out.push(0x00);
        out.push(len);
    } else {
        out.push(FLAG_LONG);
        out.extend_from_slice(&(body.len() as u64).to_be_bytes());
    }
    out.extend_from_slice(body);
    out
}

/// The peer's greeting, as parsed by [`FrameReader::read_greeting`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerGreeting {
    /// Socket type byte (`0x06` for the device).
    pub socket_type: u8,
    /// Identity sent by the peer (empty for the device).
    pub identity: Vec<u8>,
}

/// Buffered frame reader.
///
/// Partial input stays buffered across calls, so a read timeout in the middle
/// of a frame does not lose data: the next call picks up where it stopped.
#[derive(Debug)]
pub struct FrameReader<R> {
    inner: R,
    buf: Vec<u8>,
    /// Parts of a multipart message read so far.
    pending: Vec<u8>,
}

impl<R: Read> FrameReader<R> {
    /// Wrap a byte stream.
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            buf: Vec::new(),
            pending: Vec::new(),
        }
    }

    /// The wrapped stream.
    pub fn get_ref(&self) -> &R {
        &self.inner
    }

    /// Read the peer's greeting: signature, revision, socket type, identity
    /// frame.
    ///
    /// # Errors
    ///
    /// [`Error::Greeting`] if the signature or revision is wrong; I/O errors
    /// from the stream.
    pub fn read_greeting(&mut self) -> Result<PeerGreeting> {
        self.fill_to(12)?;
        if self.buf[0] != 0xFF || self.buf[9] & 0x01 == 0 {
            return Err(Error::Greeting("bad signature"));
        }
        if self.buf[10] != REVISION {
            return Err(Error::Greeting("unsupported revision"));
        }
        let socket_type = self.buf[11];
        self.buf.drain(..12);
        let (_, identity) = self.read_frame()?;
        Ok(PeerGreeting {
            socket_type,
            identity,
        })
    }

    /// Read one message (all parts joined).
    ///
    /// # Errors
    ///
    /// I/O errors from the stream (including read timeouts, after which the
    /// call can be retried), [`Error::FrameTooLarge`].
    pub fn read_message(&mut self) -> Result<Vec<u8>> {
        loop {
            let (flags, body) = self.read_frame()?;
            if flags & FLAG_MORE == 0 {
                if self.pending.is_empty() {
                    return Ok(body);
                }
                self.pending.extend_from_slice(&body);
                return Ok(std::mem::take(&mut self.pending));
            }
            self.pending.extend_from_slice(&body);
        }
    }

    /// Read one frame, returning its flags and body.
    fn read_frame(&mut self) -> Result<(u8, Vec<u8>)> {
        self.fill_to(2)?;
        let flags = self.buf[0];
        let (header, len) = if flags & FLAG_LONG == 0 {
            (2, u64::from(self.buf[1]))
        } else {
            self.fill_to(9)?;
            let mut be = [0u8; 8];
            be.copy_from_slice(&self.buf[1..9]);
            (9, u64::from_be_bytes(be))
        };
        if len > MAX_FRAME_LEN {
            return Err(Error::FrameTooLarge(len));
        }
        // Bounded by MAX_FRAME_LEN above, so this fits in usize.
        let len = usize::try_from(len).map_err(|_| Error::FrameTooLarge(len))?;
        self.fill_to(header + len)?;
        let body = self.buf[header..header + len].to_vec();
        self.buf.drain(..header + len);
        Ok((flags, body))
    }

    /// Read until at least `n` bytes are buffered.
    fn fill_to(&mut self, n: usize) -> Result<()> {
        let mut chunk = [0u8; 16 * 1024];
        while self.buf.len() < n {
            match self.inner.read(&mut chunk) {
                Ok(0) => return Err(Error::Closed),
                Ok(k) => self.buf.extend_from_slice(&chunk[..k]),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(Error::Io(e)),
            }
        }
        Ok(())
    }
}

/// Write one single-part frame.
///
/// # Errors
///
/// I/O errors from the stream.
pub fn write_frame<W: Write>(out: &mut W, body: &[u8]) -> io::Result<()> {
    out.write_all(&encode_frame(body))?;
    out.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A reader that yields its data a few bytes at a time.
    struct Trickle {
        data: Vec<u8>,
        pos: usize,
        step: usize,
    }

    impl Read for Trickle {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            let n = self.step.min(out.len()).min(self.data.len() - self.pos);
            out[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
    }

    /// Fails with `WouldBlock` once, then delivers the rest.
    struct Stall {
        first: Vec<u8>,
        rest: Vec<u8>,
        stalled: bool,
    }

    impl Read for Stall {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            if !self.first.is_empty() {
                let n = self.first.len().min(out.len());
                out[..n].copy_from_slice(&self.first[..n]);
                self.first.drain(..n);
                return Ok(n);
            }
            if !self.stalled {
                self.stalled = true;
                return Err(io::ErrorKind::WouldBlock.into());
            }
            let n = self.rest.len().min(out.len());
            out[..n].copy_from_slice(&self.rest[..n]);
            self.rest.drain(..n);
            Ok(n)
        }
    }

    #[test]
    fn client_greeting_bytes() {
        assert_eq!(
            client_greeting(),
            [
                0xFF, 0, 0, 0, 0, 0, 0, 0, 0x01, 0x7F, 0x01, 0x05, 0x00, 0x00
            ]
        );
    }

    #[test]
    fn parses_device_greeting() {
        let wire = [
            0xFF, 0, 0, 0, 0, 0, 0, 0, 0x01, 0x7F, 0x01, 0x06, 0x00, 0x00,
        ];
        let mut r = FrameReader::new(&wire[..]);
        let g = r.read_greeting().unwrap();
        assert_eq!(g.socket_type, SOCKET_ROUTER);
        assert!(g.identity.is_empty());
    }

    #[test]
    fn greeting_with_identity_then_message() {
        let mut wire = Vec::from(SIGNATURE);
        wire.extend_from_slice(&[0x01, 0x06, 0x00, 0x03, b'a', b'b', b'c']);
        wire.extend_from_slice(&encode_frame(b"{}"));
        let mut r = FrameReader::new(Trickle {
            data: wire,
            pos: 0,
            step: 1,
        });
        assert_eq!(r.read_greeting().unwrap().identity, b"abc");
        assert_eq!(r.read_message().unwrap(), b"{}");
    }

    #[test]
    fn rejects_bad_signature_and_revision() {
        let mut bad_sig = greeting(SOCKET_ROUTER);
        bad_sig[0] = 0x00;
        assert!(matches!(
            FrameReader::new(&bad_sig[..]).read_greeting(),
            Err(Error::Greeting(_))
        ));
        let mut bad_rev = greeting(SOCKET_ROUTER);
        bad_rev[10] = 0x03;
        assert!(matches!(
            FrameReader::new(&bad_rev[..]).read_greeting(),
            Err(Error::Greeting(_))
        ));
    }

    #[test]
    fn short_frame_encoding() {
        assert_eq!(encode_frame(b"abc"), [0x00, 0x03, b'a', b'b', b'c']);
        assert_eq!(encode_frame(b""), [0x00, 0x00]);
        let f = encode_frame(&[7u8; 255]);
        assert_eq!(&f[..2], &[0x00, 0xFF]);
        assert_eq!(f.len(), 257);
    }

    #[test]
    fn long_frame_encoding() {
        let body = vec![b'x'; 256];
        let f = encode_frame(&body);
        assert_eq!(&f[..9], &[0x02, 0, 0, 0, 0, 0, 0, 0x01, 0x00]);
        assert_eq!(f.len(), 9 + 256);
    }

    #[test]
    fn round_trips_short_and_long() {
        let big = vec![b'j'; 100_000];
        let mut wire = encode_frame(b"small");
        wire.extend_from_slice(&encode_frame(&big));
        let mut r = FrameReader::new(Trickle {
            data: wire,
            pos: 0,
            step: 4093,
        });
        assert_eq!(r.read_message().unwrap(), b"small");
        assert_eq!(r.read_message().unwrap(), big);
        assert!(matches!(r.read_message(), Err(Error::Closed)));
    }

    #[test]
    fn joins_multipart() {
        let wire = [
            0x01, 0x00, // empty part, MORE
            0x01, 0x02, b'{', b'"', // short part, MORE
            0x02, 0, 0, 0, 0, 0, 0, 0, 0x02, b'a', b'"', // long final part
            0x00, 0x02, b'{', b'}', // next message
        ];
        let mut r = FrameReader::new(&wire[..]);
        assert_eq!(r.read_message().unwrap(), b"{\"a\"");
        assert_eq!(r.read_message().unwrap(), b"{}");
    }

    #[test]
    fn resumes_after_timeout_mid_frame() {
        let wire = encode_frame(b"hello world");
        let mut r = FrameReader::new(Stall {
            first: wire[..5].to_vec(),
            rest: wire[5..].to_vec(),
            stalled: false,
        });
        assert!(
            matches!(r.read_message(), Err(Error::Io(e)) if e.kind() == io::ErrorKind::WouldBlock)
        );
        assert_eq!(r.read_message().unwrap(), b"hello world");
    }

    #[test]
    fn rejects_oversized_frame() {
        let wire = [0x02, 0xFF, 0, 0, 0, 0, 0, 0, 0];
        assert!(matches!(
            FrameReader::new(&wire[..]).read_message(),
            Err(Error::FrameTooLarge(_))
        ));
    }
}
