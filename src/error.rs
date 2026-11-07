//! Error type for the whole crate.

use std::io;

/// Everything that can go wrong talking to the device.
///
/// Unrecognized or malformed packets from the device are never errors: they
/// arrive as [`Event::Unknown`](crate::Event::Unknown).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Socket error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// The device did not answer in time. The payload names what was
    /// awaited, in plain words.
    #[error("timed out waiting for {0}")]
    Timeout(&'static str),

    /// The device closed the TCP connection.
    #[error("connection closed by the device")]
    Closed,

    /// The device's ZMTP greeting was not one this client understands.
    #[error("unexpected greeting from the device: {0}")]
    Greeting(&'static str),

    /// A frame header announced a body larger than [`MAX_FRAME_LEN`](crate::zmtp::MAX_FRAME_LEN).
    #[error("the device sent a {0}-byte message, over the size limit")]
    FrameTooLarge(u64),

    /// A discovery datagram was not a valid beacon.
    #[error("invalid discovery broadcast from the device: {0}")]
    Beacon(String),

    /// A worker thread panicked or a lock was poisoned.
    #[error("internal state poisoned")]
    Poisoned,
}

/// Crate result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Map a socket read timeout to [`Error::Timeout`], keeping other I/O errors.
pub(crate) fn from_read_error(err: io::Error, waiting_for: &'static str) -> Error {
    match err.kind() {
        // Unix reports SO_RCVTIMEO expiry as WouldBlock, Windows as TimedOut.
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => Error::Timeout(waiting_for),
        io::ErrorKind::UnexpectedEof => Error::Closed,
        _ => Error::Io(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// User-facing text is plain English: no packet, protocol or field names.
    #[test]
    fn display_is_plain_english() {
        let errors = [
            Error::Io(io::Error::other("boom")),
            Error::Timeout("the device's handshake reply"),
            Error::Closed,
            Error::Greeting("bad signature"),
            Error::FrameTooLarge(1 << 20),
            Error::Beacon("no device address or id".into()),
            Error::Poisoned,
        ];
        for err in errors {
            let text = err.to_string();
            for wire in ["N_", "ZMTP", "_server", "_ip", "PuttResult"] {
                assert!(!text.contains(wire), "{text:?} contains {wire:?}");
            }
        }
    }
}
