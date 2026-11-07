//! Timings and ports the driver uses, which tests replace.

use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use super::{
    CONNECT_TIMEOUT, DISCOVERY_HINT_AFTER, MAT_TIMEOUT, NOT_ANSWERING_AFTER, REPLY_TIMEOUT,
    RESYNC_GAP, SILENCE, STALL_RECONNECT_WAIT, SilenceLimits,
};
use crate::beacon::{DEVICE_PORT, DISCOVERY_PORT};
use crate::packet::Hand;
use crate::session::SessionOptions;

/// The driver's timings and ports (`testing` feature), for tests that run
/// the driver against `testing::MockDevice` in seconds instead of minutes.
/// [`Default`] is what the driver uses with a real device. None of it is a
/// setting: a real device needs these values. Start from [`Default`] and
/// change the fields a test needs.
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
    /// Discovery time without a device before the hint alert. Default 30 s.
    pub discovery_hint_after: Duration,
    /// Longest silence before a session is lost. Default 90 s.
    pub silence_baseline: Duration,
    /// Longest silence after a strike. Default 120 s.
    pub silence_after_strike: Duration,
    /// Silence after which `ready` turns false. Default 10 s.
    pub silence_warn_after: Duration,
    /// Shortest reconnect delay after a session lost to silence. Default 15 s.
    pub stall_wait: Duration,
    /// Shortest gap between mat searches. Default 10 s.
    pub resync_gap: Duration,
    /// Unanswered attempts this long before the "not answering" alert.
    /// Default 120 s.
    pub not_answering_after: Duration,
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
            discovery_hint_after: DISCOVERY_HINT_AFTER,
            silence_baseline: SILENCE.baseline,
            silence_after_strike: SILENCE.after_strike,
            silence_warn_after: SILENCE.warn_after,
            stall_wait: STALL_RECONNECT_WAIT,
            resync_gap: RESYNC_GAP,
            not_answering_after: NOT_ANSWERING_AFTER,
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

    pub(super) fn silence(&self) -> SilenceLimits {
        SilenceLimits {
            baseline: self.silence_baseline,
            after_strike: self.silence_after_strike,
            warn_after: self.silence_warn_after,
        }
    }
}
