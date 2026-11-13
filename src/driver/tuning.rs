//! The device's ports and session-start timings, which tests replace.

use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use super::{CONNECT_TIMEOUT, MAT_TIMEOUT, REPLY_TIMEOUT};
use crate::beacon::{DEVICE_PORT, DISCOVERY_PORT};
use crate::packet::Hand;
use crate::session::SessionOptions;

/// The device's ports and the timings of the TCP session (`testing`
/// feature), for tests that run the driver against `testing::MockDevice`.
/// [`Default`] is what the driver uses with a real device. None of it is a
/// setting: a real device needs these values. Start from [`Default`] and
/// change the fields a test needs. The driver's own timers (silence, mat
/// search spacing, reconnect waits, alerts) are not here: they run on the
/// `now` a host steps the session with.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Tuning {
    /// The device's TCP port, read at each connect, so a test can move the
    /// "device" between attempts. Default 8889.
    pub port: Arc<AtomicU16>,
    /// The UDP port discovery listens on. Default 9999.
    pub discovery_port: u16,
    /// TCP connect and greeting timeout. Default 3 s.
    pub connect_timeout: Duration,
    /// Wait for each handshake reply. Default 3 s.
    pub reply_timeout: Duration,
    /// Longest wait for the mat in a mat search. Default 5 s.
    pub mat_timeout: Duration,
    /// Keepalive period. Default 2 s.
    pub keepalive_interval: Duration,
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            port: Arc::new(AtomicU16::new(DEVICE_PORT)),
            discovery_port: DISCOVERY_PORT,
            connect_timeout: CONNECT_TIMEOUT,
            reply_timeout: REPLY_TIMEOUT,
            mat_timeout: MAT_TIMEOUT,
            keepalive_interval: SessionOptions::default().keepalive_interval,
        }
    }
}

impl Tuning {
    /// Session options for the next attempt, with `hand`.
    pub(super) fn session_options(&self, hand: Hand) -> SessionOptions {
        SessionOptions {
            hand,
            port: self.port.load(Ordering::SeqCst),
            connect_timeout: self.connect_timeout,
            reply_timeout: self.reply_timeout,
            mat_timeout: self.mat_timeout,
            keepalive_interval: self.keepalive_interval,
            ..SessionOptions::default()
        }
    }
}
