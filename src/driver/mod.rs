//! The flightrelay `FrpDriver` for the ExPutt RG (`driver` feature).
//!
//! [`ExPutt`] finds the device on the network (or uses
//! [`Config::address`]), starts a [`Session`] with the player's hand, and
//! turns every putt into an FRP shot: `shot.trigger`, `shot.launch` (ball
//! speed and launch direction), `shot.club` (putter path and face angle, when
//! the putter was tracked) and `shot.finished`. The device measures no
//! vertical launch and no spin, so the launch carries neither. Its status is
//! the `not_armed` blocker whenever the device is not ready for a putt (none
//! when it is), and the `device_state` telemetry key (`finding mat`,
//! `mat found`, `ready`, `ball struck`).
//!
//! A host runs it as a launch monitor actor with flightrelay's `FrpServer`
//! (feature `frp` builds the `threeputt-frp` binary on `flightrelay::run`),
//! or drives it itself through the `FrpDriver` contract: each
//! `step(now)` handles at most one device message, and every timer below
//! runs on the `now` the host steps the session with.
//!
//! # Hints and actions
//!
//! - The handedness of a `golfer.turn` (`TurnHints::handedness`), when it
//!   differs from the hand the device has, starts a new round with that hand
//!   ([`Session::restart_match`]). A turn with no handedness keeps the hand
//!   the device has; a session starts right-handed when there is none. The
//!   mode and the club do not apply: the device only measures putts
//!   (`DriverInfo::fixed_mode` is putting).
//! - [`Action::FindMat`] has the device locate the mat again
//!   ([`Session::find_mat`]).
//!
//! Both run a mat search, and mat searches are spaced: none starts within
//! 10 s of the previous one (the session's own at its start included). A
//! request inside that gap waits for its end; requests made meanwhile fold
//! into the latest hand and one mat search. The `not_armed` blocker is
//! reported while one waits.
//!
//! # A device that stalls
//!
//! The device's app can stop answering for half a minute or more and then
//! carry on, so the driver gives it time:
//!
//! - A session may stay silent for 90 s, and 120 s after a strike until the
//!   putt's result or the device is ready again. The `not_armed` blocker is
//!   reported from 10 s of silence until the device sends again. Past the limit the
//!   session ends with [`Error::Silent`].
//! - After a session lost to silence, the next attempt waits at least 15 s,
//!   and attempts stay at least 15 s apart until a session succeeds.
//! - After a session that was measuring, the first three attempts wait at
//!   most 1 s.
//! - Each failed attempt raises an "ExPutt error" `bus.alert`, until two minutes
//!   of attempts the device accepted but never answered have passed. Then one
//!   alert says the device is not answering, and the attempts go on quietly
//!   until a session succeeds.
//!
//! Starting a session cannot be cut short, so [`ExPutt::STOP_WAIT`] covers
//! its timeouts: 3 s each for the connect, the greeting and the two handshake
//! replies, 5 s for the mat search, and a second to close.
//!
//! # Raw frames
//!
//! While the host's configuration turns raw frames on, every message the
//! session sends and every known message it receives becomes a `shot.raw`: the JSON text as it
//! crossed the socket, labeled with its name (`PuttResult`, `Test`,
//! `resyncToMat`, …). A putt result's frame carries the shot's key. Camera
//! images and messages the crate does not recognize are never sent as raw
//! frames; they reach only the host's trace log, as hex. While raw frames are
//! off, a trace log gets every message that way.

mod policy;
mod raw;
mod tuning;

use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::time::{Duration, Instant};

use flightrelay::config::{ConfigError, DriverConfig};
use flightrelay::driver::{
    Activity, Backoff, ConnectCtx, DeviceIdentity, DeviceStatus, DriverInfo, Events, FrpDriver,
    FrpSession, Level, RawProfile, SessionEnd, ShotIds, Stopped, TurnHints,
};
use flightrelay::{
    ClubData, FrpBody, Handedness, Launch, Mode, RawDir, RawFrame, Severity, ShotKey, Source,
    Velocity, utc_ms,
};

use crate::beacon::{Beacon, Listener};
use crate::config::Config;
use crate::conn::{Direction, Tap};
use crate::event::{Event, PuttResult};
use crate::packet::Hand;
use crate::session::Session;
use policy::{Quiet, Resync, ResyncGate, Retry, SilenceLimits, Totals, Unanswered, Watchdog};
use raw::{Frame, Message, PUTT_RESULT_LABEL, RawTap, classify};
#[cfg(feature = "testing")]
pub use tuning::Tuning;
#[cfg(not(feature = "testing"))]
use tuning::Tuning;

/// Session start timeouts. Starting a session cannot be interrupted, so these
/// bound how long a stop or a reconnect can wait on a device that is away.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const REPLY_TIMEOUT: Duration = Duration::from_secs(3);
const MAT_TIMEOUT: Duration = Duration::from_secs(5);

/// Longest single wait for the device to show up on the network, between
/// ticks of the host.
const DISCOVERY_SLICE: Duration = Duration::from_millis(250);

/// How long discovery runs without finding the device before the user is
/// told how to reach it another way.
const DISCOVERY_HINT_AFTER: Duration = Duration::from_secs(30);

/// The alert raised once when discovery has not found the device.
pub const DISCOVERY_HINT: &str = "ExPutt not found on this network. If it is on another network, \
     or a firewall blocks discovery, enter its IP address in the driver's address setting.";

/// How long the device may stay silent. It sends a heartbeat about once a
/// second but can pause for a while, longer while it works out a putt, and its
/// app can stall for half a minute or more and then carry on, so a stalled
/// device is given time. A connection the device closes or resets ends the
/// session at once; a device that just disappears is found by this limit
/// alone. The `not_armed` blocker is reported from the warning on.
const SILENCE: SilenceLimits = SilenceLimits {
    baseline: Duration::from_secs(90),
    after_strike: Duration::from_secs(120),
    warn_after: Duration::from_secs(10),
};

/// Shortest wait before reconnecting to a device that went silent, and the
/// shortest gap between attempts until a session succeeds again. A stalled
/// device is left alone rather than hit with new connections.
const STALL_RECONNECT_WAIT: Duration = Duration::from_secs(15);

/// Shortest gap between two mat searches (the session's own, counted from the
/// end of session start; a hand change; Find mat). A request inside the gap
/// waits for its end; only the latest one is run.
const RESYNC_GAP: Duration = Duration::from_secs(10);

// A search that is still running when the gap ends would swallow the next
// request: the session ignores a Find mat while the previous search waits for
// its mat.
const _: () = assert!(RESYNC_GAP.as_millis() > MAT_TIMEOUT.as_millis());

/// How long connection attempts may fail to get an answer from a device that
/// accepts the connection before the user is told.
const NOT_ANSWERING_AFTER: Duration = Duration::from_secs(120);

/// The alert raised once per outage of a device that accepts connections and
/// never answers.
pub const NOT_ANSWERING: &str =
    "The ExPutt is not answering. If this lasts, turn it off and on again.";

/// A mat search in a running session that takes longer than this is logged as
/// a warning.
const SLOW_MAT_SEARCH: Duration = Duration::from_secs(3);

/// Names the transport in a host's raw capture.
const RAW_TRANSPORT: &str = "exputt-wifi";

/// Product name: the model, and the start of the device label.
const MODEL: &str = "ExPutt RG";

/// The longest a stop waits for a connect. A session start in progress cannot
/// be cut short: the TCP connect and the device's greeting (up to
/// [`CONNECT_TIMEOUT`] each), two handshake replies ([`REPLY_TIMEOUT`] each)
/// and the mat search ([`MAT_TIMEOUT`]), then a second to close the session.
const STOP_WAIT: Duration = Duration::from_secs(
    2 * CONNECT_TIMEOUT.as_secs() + 2 * REPLY_TIMEOUT.as_secs() + MAT_TIMEOUT.as_secs() + 1,
);

/// A device-specific operation of the ExPutt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Action {
    /// Locate the mat again, e.g. when the device locked onto the wrong spot
    /// because someone stood in its view as the session started.
    FindMat,
}

/// Why a connect or a session ended.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The device could not be found, reached or set up, or a send failed.
    #[error(transparent)]
    Device(#[from] crate::Error),
    /// The device sent nothing for this long.
    #[error("no data from the device for {:.0}s", .0.as_secs_f32())]
    Silent(Duration),
    /// The device closed the connection.
    #[error("connection to the device lost")]
    Lost,
    /// The host stopped.
    #[error("stopped")]
    Stopped,
}

impl From<Stopped> for Error {
    fn from(_: Stopped) -> Self {
        Self::Stopped
    }
}

/// The ExPutt RG as a flightrelay `FrpDriver`. See the [module docs](self).
#[derive(Debug)]
pub struct ExPutt {
    config: Config,
    /// The configured address, `None` to find the device on the network.
    ip: Option<IpAddr>,
    ids: ShotIds,
    tuning: Tuning,
    retry: Retry,
    unanswered: Unanswered,
    totals: Arc<Totals>,
}

#[cfg(feature = "testing")]
impl ExPutt {
    /// A driver with test ports and session timings instead of the
    /// device's ([`Tuning`]).
    ///
    /// # Errors
    ///
    /// The [`ConfigError`] of a config that is not usable.
    pub fn with_tuning(config: Config, tuning: Tuning) -> Result<Self, ConfigError> {
        let mut driver = <Self as FrpDriver>::new(config)?;
        driver.tuning = tuning;
        Ok(driver)
    }
}

impl ExPutt {
    /// Wait for the device to announce itself on the network, on one socket,
    /// ticking the host between waits. Raises one hint alert when the device
    /// has not shown up after a while.
    fn discover(&self, ctx: &mut ConnectCtx<'_>) -> Result<Beacon, Error> {
        ctx.out
            .log(Level::Info, "looking for the ExPutt on the network...");
        ctx.out.phase("discovering");
        let listener = Listener::bind(self.tuning.discovery_port)?;
        let started = ctx.now();
        let mut hinted = false;
        loop {
            ctx.tick()?;
            match listener.recv(DISCOVERY_SLICE) {
                Ok(beacon) => {
                    ctx.out.log(
                        Level::Info,
                        format!(
                            "found the ExPutt at {} (software {})",
                            beacon.ip,
                            beacon.app_version.as_deref().unwrap_or("unknown"),
                        ),
                    );
                    return Ok(beacon);
                }
                Err(crate::Error::Timeout(_)) => {}
                Err(e) => return Err(e.into()),
            }
            let searched = ctx.now().saturating_duration_since(started);
            if !hinted && searched >= DISCOVERY_HINT_AFTER {
                hinted = true;
                ctx.out.log(
                    Level::Info,
                    format!(
                        "no ExPutt found on the network after {}s",
                        searched.as_secs()
                    ),
                );
                // Warn is the lowest alert severity.
                ctx.out.alert(Severity::Warn, DISCOVERY_HINT);
            }
        }
    }

    /// Start a measuring session with the device at `ip` over the link
    /// `start` opens with the hand and the raw tap (the TCP [`Session`], or a
    /// test's channel).
    fn open<L: Link>(
        &mut self,
        ctx: &mut ConnectCtx<'_>,
        ip: IpAddr,
        firmware: Option<String>,
        start: impl FnOnce(&Tuning, Hand, Tap) -> crate::Result<L>,
    ) -> Result<Measuring<L>, Error> {
        ctx.out.phase("connecting");
        let hand = hand_of(ctx.hints().handedness);
        ctx.out.log(
            Level::Info,
            format!("connecting to the ExPutt at {ip} ({hand:?} hand)..."),
        );
        ctx.tick()?;
        let (raw, raw_rx) = RawTap::new();
        raw.want(ctx.out.wants_raw() || ctx.out.enabled(Level::Trace));
        // The session's own mat search runs inside `start`.
        let resync_started = ctx.now();
        let link = start(&self.tuning, hand, raw.tap())?;
        self.totals.connects.fetch_add(1, Ordering::Relaxed);
        self.totals.resyncs.fetch_add(1, Ordering::Relaxed);

        let name = format!("{MODEL} ({ip})");
        ctx.out
            .log(Level::Info, format!("connected to the {name}, measuring"));
        let mut identity = DeviceIdentity::new(name.clone())
            .with_model(MODEL)
            .with_label(name);
        if let Some(firmware) = firmware {
            identity = identity.with_firmware(firmware);
        }
        let now = ctx.now();
        let mut session = Measuring {
            link,
            identity,
            ids: self.ids.clone(),
            raw,
            raw_rx,
            watchdog: Watchdog::new(SILENCE, now),
            gate: ResyncGate::new(RESYNC_GAP, hand, now),
            device_ready: false,
            device_state: None,
            silent: false,
            mat_search: Some((resync_started, true)),
            struck_at: None,
            hold_to_log: false,
            totals: Arc::clone(&self.totals),
        };
        // A handedness hint that arrived while the session started is a
        // request like any other. After a stop, dropping the session closes
        // it.
        ctx.tick()?;
        if let Some(handed) = ctx.hints().handedness {
            session.gate.want_hand(hand_of(Some(handed)));
            session.hold_to_log = true;
        }
        ctx.out.phase("measuring");
        Ok(session)
    }

    /// [`FrpDriver::retry_delay`] at `now`, which dates an attempt the
    /// device did not answer.
    fn retry_delay_at(
        &mut self,
        ended: &SessionEnd<'_, Error>,
        backoff: &mut Backoff,
        out: &mut Events<'_>,
        now: Instant,
    ) -> Duration {
        let Some(error) = ended.error else {
            return backoff.delay(ended.connected_for);
        };
        // A stop is no failure: no alert, and the reconnect policy stays.
        if matches!(error, Error::Stopped) {
            return backoff.delay(ended.connected_for);
        }

        // A device that accepts the connection and never answers is not
        // hammered (see `Retry`); the user is told once, and the per-attempt
        // alerts stop until a session succeeds.
        let mut tell_not_answering = false;
        if ended.connected_for.is_some() {
            self.unanswered.recovered();
        } else if matches!(error, Error::Device(crate::Error::Timeout(_))) {
            tell_not_answering = self.unanswered.no_answer(now);
        }
        if !self.unanswered.told {
            out.alert(Severity::Warn, format!("ExPutt error: {error}"));
        }
        if tell_not_answering {
            out.log(
                Level::Warn,
                format!(
                    "no answer from the device to {} s of connection attempts",
                    self.unanswered.after.as_secs()
                ),
            );
            out.alert(Severity::Warn, NOT_ANSWERING);
        }

        let silent = matches!(error, Error::Silent(_));
        self.retry
            .delay(backoff, ended.connected_for, silent, STALL_RECONNECT_WAIT)
    }
}

impl FrpDriver for ExPutt {
    type Config = Config;
    type Error = Error;
    type Action = Action;
    type Session = ExPuttSession;

    const STOP_WAIT: Duration = STOP_WAIT;

    fn info(_: &Config) -> DriverInfo {
        DriverInfo::new("exputt", MODEL, Source::Measured)
            .exclusive()
            .with_fixed_mode(Mode::Putting)
            .with_raw(RawProfile::new(RAW_TRANSPORT, 64, 64, 0, |_| false, true))
    }

    fn new(config: Config) -> Result<Self, ConfigError> {
        config.validate()?;
        let ip = config.ip()?;
        let tuning = Tuning::default();
        Ok(Self {
            config,
            ip,
            ids: ShotIds::new(),
            unanswered: Unanswered::new(NOT_ANSWERING_AFTER),
            tuning,
            retry: Retry::default(),
            totals: Arc::new(Totals::default()),
        })
    }

    fn config(&self) -> &Config {
        &self.config
    }

    fn apply_config(&mut self, new: &Config) {
        self.config = new.clone();
        // A host applies only validated configs; an address that does not
        // parse leaves the one in use.
        if let Ok(ip) = new.ip() {
            self.ip = ip;
        }
    }

    fn connect(&mut self, ctx: &mut ConnectCtx<'_>) -> Result<ExPuttSession, Error> {
        ctx.tick()?;
        let (ip, firmware) = if let Some(ip) = self.ip {
            (ip, None)
        } else {
            let beacon = self.discover(ctx)?;
            (beacon.ip, beacon.app_version)
        };
        self.open(ctx, ip, firmware, |tuning, hand, tap| {
            Session::start_tapped(ip, &tuning.session_options(hand), tap)
        })
        .map(ExPuttSession)
    }

    /// The device's own reconnect policy: the host's backoff, shortened for
    /// the first attempts after a session that was measuring and lengthened
    /// after a session lost to silence; an "ExPutt error" alert per attempt
    /// until the "not answering" alert has gone out, then none until a
    /// session succeeds.
    fn retry_delay(
        &mut self,
        ended: &SessionEnd<'_, Error>,
        backoff: &mut Backoff,
        out: &mut Events<'_>,
    ) -> Option<Duration> {
        Some(self.retry_delay_at(ended, backoff, out, Instant::now()))
    }
}

/// A measuring session with the ExPutt. See the [module docs](self).
#[derive(Debug)]
pub struct ExPuttSession(Measuring<Session>);

/// What the driver uses of a session with the device: its events, the mat
/// searches, and the close. The TCP [`Session`]; a channel in tests.
trait Link {
    /// The device's events; disconnected once the device is gone.
    fn events(&self) -> &Receiver<Event>;
    fn find_mat(&self) -> crate::Result<()>;
    fn restart_match(&self, hand: Hand) -> crate::Result<()>;
    fn close(self) -> crate::Result<()>;
}

impl Link for Session {
    fn events(&self) -> &Receiver<Event> {
        Session::events(self)
    }

    fn find_mat(&self) -> crate::Result<()> {
        Session::find_mat(self)
    }

    fn restart_match(&self, hand: Hand) -> crate::Result<()> {
        Session::restart_match(self, hand)
    }

    fn close(self) -> crate::Result<()> {
        Session::close(self)
    }
}

/// The session behind [`ExPuttSession`], over any [`Link`].
#[derive(Debug)]
struct Measuring<L> {
    link: L,
    identity: DeviceIdentity,
    ids: ShotIds,
    raw: RawTap,
    raw_rx: Receiver<Message>,
    watchdog: Watchdog,
    gate: ResyncGate,
    /// The last `ready` the device itself reported; the status also depends
    /// on a waiting mat search and on silence.
    device_ready: bool,
    /// The last state the device reported, the `device_state` telemetry.
    device_state: Option<&'static str>,
    /// The device has been silent past the warning threshold; not ready as
    /// published until it sends something again.
    silent: bool,
    /// The mat search in progress, when it started, and whether it is the
    /// session's own (whose time includes connect and handshake).
    mat_search: Option<(Instant, bool)>,
    /// When the ball was struck (UTC ms), until the putt's result or the
    /// device is ready again.
    struck_at: Option<u64>,
    /// A hint or action asked for a mat search since the last step, which
    /// logs whether it is held back.
    hold_to_log: bool,
    totals: Arc<Totals>,
}

impl<L: Link> Measuring<L> {
    /// Report the status: the `not_armed` blocker unless the device is ready
    /// (it is not while a requested mat search waits or past the silence
    /// warning), and `device_state`. That is the whole status: the device
    /// has no battery or attitude. The host publishes only what changed.
    fn report(&self, out: &mut Events<'_>) {
        let ready = self.device_ready && !self.silent && !self.gate.pending();
        let mut status = if ready {
            DeviceStatus::new()
        } else {
            DeviceStatus::not_armed()
        };
        if let Some(state) = self.device_state {
            status = status.with_telemetry("device_state", state);
        }
        out.status(status);
    }

    /// Logs how long a requested mat search is held back, if it is.
    fn log_hold(&self, out: &mut Events<'_>, now: Instant) {
        if let Some(left) = self.gate.hold(now) {
            out.log(
                Level::Info,
                format!(
                    "mat search held for {:.1}s, after the last one",
                    left.as_secs_f32()
                ),
            );
        }
    }

    /// Emit the messages the tap queued: shareable ones as raw frames (a putt
    /// result's with `putt`, its shot) while the host wants them, everything
    /// else as audit lines (which reach only a trace log).
    fn drain_raw(&mut self, out: &mut Events<'_>, putt: Option<&ShotKey>) {
        while let Ok(message) = self.raw_rx.try_recv() {
            let dir = match message.dir {
                Direction::Rx => RawDir::Rx,
                Direction::Tx => RawDir::Tx,
            };
            let (label, private) = match classify(message.dir, &message.body) {
                Frame::Shared(label) => (label, false),
                Frame::Private(label) => (label, true),
            };
            let raw = out.wants_raw() && !private;
            match putt.filter(|_| raw && label == PUTT_RESULT_LABEL) {
                Some(key) => out.frp(FrpBody::ShotRaw(RawFrame {
                    timestamp: message.at,
                    label: Some(label.to_owned()),
                    key: Some(key.clone()),
                    ..RawFrame::new(dir, 0, message.body)
                })),
                None => out.wire(dir, label, message.body, message.at, private),
            }
        }
    }

    /// Emit one putt as a putter shot: the putt's raw frame (with the shot's
    /// key, before the shot events built from it), then trigger, ball,
    /// putter data when tracked, finish.
    fn emit_putt(&mut self, out: &mut Events<'_>, putt: &PuttResult) {
        let key = self.ids.next_key();
        self.drain_raw(out, Some(&key));
        out.log(
            Level::Info,
            format!(
                "putt #{}: {:.2}mph HLA={:+.2} path={} face={} values={:?}",
                key.shot_number,
                Velocity::MetersPerSecond(putt.ball_speed_mps).as_mph(),
                putt.launch_direction_deg,
                putt.path_deg.map_or("-".into(), |v| format!("{v:+.2}")),
                putt.face_deg.map_or("-".into(), |v| format!("{v:+.2}")),
                putt.raw,
            ),
        );
        match self.struck_at.take() {
            Some(at) => out.shot_trigger_at(key.clone(), at),
            None => out.shot_trigger(key.clone()),
        }
        out.launch(key.clone(), launch_of(putt));
        if let Some(club) = club_of(putt) {
            out.club(key.clone(), club);
        } else {
            out.log(Level::Debug, "  no putter data (putter not tracked)");
        }
        out.shot_finished(key);
    }

    /// A device event at `now`: the watchdog, then readiness and the device
    /// state, reported when the event or the end of a silence moved either.
    fn handle(&mut self, out: &mut Events<'_>, event: Event, now: Instant) {
        let spell = self.watchdog.event(&event, now);
        if let Some(spell) = spell {
            self.silent = false;
            out.log(
                Level::Info,
                format!(
                    "device sent data again after {:.1}s of silence",
                    spell.as_secs_f32()
                ),
            );
        }
        // (ready, device_state) the event sets; `None` leaves a value alone.
        let (ready, device_state) = match event {
            Event::FindingMat => (None, Some("finding mat")),
            Event::MatFound => {
                self.log_mat_found(out, now);
                (None, Some("mat found"))
            }
            Event::PuttReady => {
                out.log(Level::Info, "ready for a putt");
                self.struck_at = None;
                (Some(true), Some("ready"))
            }
            Event::BallStruck => {
                out.log(Level::Info, "ball struck");
                self.struck_at = utc_ms();
                (Some(false), Some("ball struck"))
            }
            Event::Heartbeat => (None, None),
            Event::PuttResult(putt) => {
                self.emit_putt(out, &putt);
                (None, None)
            }
            Event::Unknown { packet_id, comment } => {
                out.log(
                    Level::Debug,
                    format!("unrecognized event {packet_id}: {comment:?}"),
                );
                (None, None)
            }
            other => {
                out.log(Level::Debug, format!("event {other:?}"));
                (None, None)
            }
        };
        if ready.is_none() && device_state.is_none() && spell.is_none() {
            return;
        }
        self.device_ready = ready.unwrap_or(self.device_ready);
        self.device_state = device_state.or(self.device_state);
        self.report(out);
    }

    fn log_mat_found(&mut self, out: &mut Events<'_>, now: Instant) {
        let Some((started, at_start)) = self.mat_search.take() else {
            return;
        };
        let took = now.saturating_duration_since(started);
        if at_start {
            out.log(
                Level::Info,
                format!(
                    "mat found {:.1}s into session start (connect and handshake included)",
                    took.as_secs_f32()
                ),
            );
        } else {
            out.log(
                Level::Info,
                format!("mat found after {:.1}s", took.as_secs_f32()),
            );
            if took > SLOW_MAT_SEARCH {
                out.log(
                    Level::Warn,
                    format!(
                        "mat search took {:.1}s, over {}s",
                        took.as_secs_f32(),
                        SLOW_MAT_SEARCH.as_secs()
                    ),
                );
            }
        }
    }

    /// No event waiting at `now`: the watchdog decides.
    fn quiet(&mut self, out: &mut Events<'_>, now: Instant) -> Result<(), Error> {
        match self.watchdog.check(now) {
            Quiet::Fine => Ok(()),
            Quiet::Warn(quiet) => {
                out.log(
                    Level::Warn,
                    format!(
                        "no data from the device for {:.1}s, waiting up to {:.1}s \
                         ({} connects, {} mat searches since start)",
                        quiet.as_secs_f32(),
                        self.watchdog.limit().as_secs_f32(),
                        self.totals.connects.load(Ordering::Relaxed),
                        self.totals.resyncs.load(Ordering::Relaxed),
                    ),
                );
                self.silent = true;
                self.report(out);
                Ok(())
            }
            Quiet::Lost(quiet) => {
                out.log(
                    Level::Warn,
                    format!(
                        "giving up on the device after {:.0}s without data \
                         ({} connects, {} mat searches since start)",
                        quiet.as_secs_f32(),
                        self.totals.connects.load(Ordering::Relaxed),
                        self.totals.resyncs.load(Ordering::Relaxed),
                    ),
                );
                Err(Error::Silent(quiet))
            }
        }
    }
}

impl<L: Link> FrpSession for Measuring<L> {
    type Error = Error;
    type Action = Action;

    /// One pass: a mat search that is due, then at most one device event
    /// (`Activity::Active`), or the watchdog when none is waiting (`Activity::Idle`).
    fn step(&mut self, now: Instant, out: &mut Events<'_>) -> Result<Activity, Error> {
        self.raw.want(out.wants_raw() || out.enabled(Level::Trace));
        if std::mem::take(&mut self.hold_to_log) {
            self.log_hold(out, now);
        }

        // A mat search runs at most once per gap, with the latest hand.
        if let Some(search) = self.gate.due(now) {
            match search {
                Resync::Restart(hand) => {
                    out.log(Level::Info, format!("hand -> {hand:?}, finding the mat"));
                    self.link.restart_match(hand)?;
                }
                Resync::Find => {
                    out.log(Level::Info, "finding the mat");
                    self.link.find_mat()?;
                }
            }
            self.totals.resyncs.fetch_add(1, Ordering::Relaxed);
            self.mat_search = Some((now, false));
            self.device_ready = false;
            self.report(out);
        }

        let event = match self.link.events().try_recv() {
            Ok(event) => event,
            Err(TryRecvError::Empty) => {
                self.drain_raw(out, None);
                self.quiet(out, now)?;
                return Ok(Activity::Idle);
            }
            Err(TryRecvError::Disconnected) => {
                self.drain_raw(out, None);
                out.log(Level::Info, "device closed the connection");
                return Err(Error::Lost);
            }
        };
        // A putt's own frame goes out with its shot's key, which the putt
        // assigns.
        if !matches!(event, Event::PuttResult(_)) {
            self.drain_raw(out, None);
        }
        self.handle(out, event, now);
        Ok(Activity::Active)
    }

    /// Follow the golfer's handedness: a new round when it differs from the
    /// device's hand, which finds the mat first, so the device is not ready
    /// until it reports ready again. No handedness hint keeps the hand. The
    /// mode and the club do not apply: the device only measures putts.
    fn hints(&mut self, hints: &TurnHints, out: &mut Events<'_>) -> Result<(), Error> {
        if let Some(handed) = hints.handedness {
            self.gate.want_hand(hand_of(Some(handed)));
            self.hold_to_log = true;
            self.report(out);
        }
        Ok(())
    }

    fn action(&mut self, action: &Action, out: &mut Events<'_>) -> Result<(), Error> {
        match action {
            Action::FindMat => {
                self.gate.want_find();
                self.hold_to_log = true;
            }
        }
        self.report(out);
        Ok(())
    }

    fn identity(&self) -> &DeviceIdentity {
        &self.identity
    }

    fn close(self, out: &mut Events<'_>) -> Result<(), Error> {
        if self.gate.pending() {
            out.log(Level::Debug, "mat search request dropped at shutdown");
        }
        out.log(Level::Info, "closing the session");
        if let Err(e) = self.link.close() {
            out.log(Level::Debug, format!("session close: {e}"));
        }
        Ok(())
    }
}

impl FrpSession for ExPuttSession {
    type Error = Error;
    type Action = Action;

    fn step(&mut self, now: Instant, out: &mut Events<'_>) -> Result<Activity, Error> {
        self.0.step(now, out)
    }

    fn hints(&mut self, hints: &TurnHints, out: &mut Events<'_>) -> Result<(), Error> {
        self.0.hints(hints, out)
    }

    fn action(&mut self, action: &Action, out: &mut Events<'_>) -> Result<(), Error> {
        self.0.action(action, out)
    }

    fn identity(&self) -> &DeviceIdentity {
        self.0.identity()
    }

    fn close(self, out: &mut Events<'_>) -> Result<(), Error> {
        self.0.close(out)
    }
}

/// The putting hand for the player's handedness (right when unknown).
fn hand_of(handed: Option<Handedness>) -> Hand {
    if handed == Some(Handedness::Left) {
        Hand::Left
    } else {
        Hand::Right
    }
}

/// The launch of a putt: ball speed and launch direction. The device measures
/// neither a vertical launch nor spin, so `launch` carries neither.
fn launch_of(putt: &PuttResult) -> Launch {
    Launch {
        launch_speed: Some(Velocity::MetersPerSecond(putt.ball_speed_mps)),
        // Both are positive to the right of the target line.
        launch_azimuth: Some(putt.launch_direction_deg),
        ..Launch::default()
    }
}

/// Putter data of a putt: path and face angle, each when the device tracked
/// it. `None` when it tracked neither (the putter was not seen); the ball
/// data still makes a shot.
fn club_of(putt: &PuttResult) -> Option<ClubData> {
    if putt.path_deg.is_none() && putt.face_deg.is_none() {
        return None;
    }
    Some(ClubData {
        // Both are positive to the right of the target line.
        path: putt.path_deg,
        face_angle: putt.face_deg,
        ..ClubData::default()
    })
}

#[cfg(test)]
mod tests;
