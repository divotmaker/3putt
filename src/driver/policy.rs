//! The driver's pure policies: reconnect delays, the unanswered-device
//! outage, mat search spacing and the silence watchdog. Each takes `now`
//! from its caller.

use std::sync::atomic::AtomicU32;
use std::time::{Duration, Instant};

use flightrelay::driver::Backoff;

use crate::event::Event;
use crate::packet::Hand;

/// Reconnect delay cap for the first [`QUICK_RETRIES`] attempts after a
/// session that was measuring, so a device that went quiet for a moment is
/// back quickly. Later attempts use the host's backoff.
const QUICK_RETRY: Duration = Duration::from_secs(1);
pub(super) const QUICK_RETRIES: u32 = 3;

/// Counts since the driver was built, kept across reconnects, for the logs.
#[derive(Debug, Default)]
pub(super) struct Totals {
    pub(super) connects: AtomicU32,
    pub(super) resyncs: AtomicU32,
}

/// Reconnect delays: the host's backoff, shortened for the first
/// [`QUICK_RETRIES`] attempts after a session that was measuring, and
/// lengthened after a session lost to silence.
#[derive(Debug, Default)]
pub(super) struct Retry {
    /// Attempts left whose delay is capped at [`QUICK_RETRY`].
    pub(super) quick_left: u32,
    /// The last session that ended was lost to silence and none has
    /// succeeded since.
    pub(super) calm: bool,
}

impl Retry {
    /// Delay before the next attempt. `session` is how long the attempt that
    /// just ended was measuring, `None` if it never got there; `silent` is
    /// whether it ended because the device went silent. After a silent
    /// session every delay is at least `stall_wait`, until a session
    /// succeeds again.
    pub(super) fn delay(
        &mut self,
        backoff: &mut Backoff,
        session: Option<Duration>,
        silent: bool,
        stall_wait: Duration,
    ) -> Duration {
        if session.is_some() {
            self.quick_left = QUICK_RETRIES;
            self.calm = false;
        }
        self.calm |= silent;
        let mut delay = backoff.delay(session);
        if self.quick_left > 0 {
            self.quick_left -= 1;
            delay = delay.min(QUICK_RETRY);
        }
        if self.calm {
            delay = delay.max(stall_wait);
        }
        delay
    }
}

/// Tracks an outage: connection attempts that reach a device which accepts
/// the connection but never answers. The clock starts at the first such
/// attempt and runs until a session succeeds; attempts that fail some other
/// way neither stop nor restart it, so a flapping outage is still reported.
#[derive(Debug)]
pub(super) struct Unanswered {
    /// How long the outage may go on before the user is told.
    pub(super) after: Duration,
    /// When the first unanswered attempt of the outage happened.
    since: Option<Instant>,
    /// The user has been told about this outage.
    pub(super) told: bool,
}

impl Unanswered {
    pub(super) fn new(after: Duration) -> Self {
        Self {
            after,
            since: None,
            told: false,
        }
    }

    /// An attempt got no answer at `now`. Whether the user is to be told
    /// now: once per outage.
    pub(super) fn no_answer(&mut self, now: Instant) -> bool {
        let since = *self.since.get_or_insert(now);
        if !self.told && now.saturating_duration_since(since) >= self.after {
            self.told = true;
            return true;
        }
        false
    }

    /// A session succeeded: the outage is over.
    pub(super) fn recovered(&mut self) {
        self.since = None;
        self.told = false;
    }
}

/// What a mat search is asked for.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Resync {
    /// A new round with this hand.
    Restart(Hand),
    /// Locate the mat again, same hand.
    Find,
}

/// Spaces the mat searches of a session. A search is never started within
/// `gap` of the previous one (the session's own at its start included);
/// requests made inside the gap are folded into the latest hand and one Find
/// mat, and run once when the gap ends.
#[derive(Debug)]
pub(super) struct ResyncGate {
    gap: Duration,
    /// When the last search started.
    last: Instant,
    /// The hand the device was last told.
    hand: Hand,
    /// The latest hand asked for.
    want: Hand,
    /// A Find mat is waiting.
    find: bool,
}

impl ResyncGate {
    pub(super) fn new(gap: Duration, hand: Hand, now: Instant) -> Self {
        Self {
            gap,
            last: now,
            hand,
            want: hand,
            find: false,
        }
    }

    pub(super) fn want_hand(&mut self, hand: Hand) {
        self.want = hand;
    }

    pub(super) fn want_find(&mut self) {
        self.find = true;
    }

    /// Something is asked for that has not run.
    pub(super) fn pending(&self) -> bool {
        self.find || self.want != self.hand
    }

    /// Time left until a pending search may run, `None` if none is pending or
    /// it may run now.
    pub(super) fn hold(&self, now: Instant) -> Option<Duration> {
        if !self.pending() {
            return None;
        }
        self.gap
            .checked_sub(now.saturating_duration_since(self.last))
            .filter(|left| !left.is_zero())
    }

    /// The search to start at `now`, if one is pending and the gap is over.
    pub(super) fn due(&mut self, now: Instant) -> Option<Resync> {
        if !self.pending() || self.hold(now).is_some() {
            return None;
        }
        self.last = now;
        self.find = false;
        if self.want == self.hand {
            Some(Resync::Find)
        } else {
            self.hand = self.want;
            Some(Resync::Restart(self.hand))
        }
    }
}

/// How long a measuring session may go without a device event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SilenceLimits {
    /// Longest silence before the session is treated as lost.
    pub(super) baseline: Duration,
    /// Longest silence after a strike, until the putt's result or the device
    /// is ready again.
    pub(super) after_strike: Duration,
    /// Silence after which one warning is logged and `ready` turns false.
    pub(super) warn_after: Duration,
}

/// What a quiet spell amounts to so far.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Quiet {
    /// Within the limits.
    Fine,
    /// Past the warning threshold for the first time in this spell.
    Warn(Duration),
    /// Past the limit: the session is lost.
    Lost(Duration),
}

/// Tracks silence from the device against [`SilenceLimits`].
#[derive(Debug)]
pub(super) struct Watchdog {
    limits: SilenceLimits,
    last: Instant,
    /// A strike was seen and its result has not arrived yet.
    awaiting_result: bool,
    /// The current quiet spell has been warned about.
    warned: bool,
}

impl Watchdog {
    pub(super) fn new(limits: SilenceLimits, now: Instant) -> Self {
        Self {
            limits,
            last: now,
            awaiting_result: false,
            warned: false,
        }
    }

    /// A device event arrived at `now`. Returns the length of the quiet spell
    /// it ends when that spell was warned about.
    pub(super) fn event(&mut self, event: &Event, now: Instant) -> Option<Duration> {
        match event {
            Event::BallStruck => self.awaiting_result = true,
            Event::PuttResult(_) | Event::PuttReady => self.awaiting_result = false,
            _ => {}
        }
        let spell = now.saturating_duration_since(self.last);
        self.last = now;
        std::mem::take(&mut self.warned).then_some(spell)
    }

    /// The silence allowed right now.
    pub(super) fn limit(&self) -> Duration {
        if self.awaiting_result {
            self.limits.after_strike
        } else {
            self.limits.baseline
        }
    }

    pub(super) fn check(&mut self, now: Instant) -> Quiet {
        let quiet = now.saturating_duration_since(self.last);
        if quiet >= self.limit() {
            Quiet::Lost(quiet)
        } else if quiet >= self.limits.warn_after && !self.warned {
            self.warned = true;
            Quiet::Warn(quiet)
        } else {
            Quiet::Fine
        }
    }
}
