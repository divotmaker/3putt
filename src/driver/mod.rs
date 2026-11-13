//! The flightrelay `DeviceDriver` for the ExPutt RG (`driver` feature).
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
//! or drives it itself through the `DeviceDriver` contract.
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

mod tuning;

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use flightrelay::config::{ConfigError, DriverConfig};
use flightrelay::driver::{
    Backoff, ConnectCtx, DeviceDriver, DeviceIdentity, DeviceSession, DeviceStatus, DriverInfo,
    Events, Level, RawProfile, SessionEnd, ShotIds, Stopped, TurnHints,
};
use flightrelay::{
    ClubData, FrpBody, Handedness, Launch, Mode, RawDir, RawFrame, Severity, ShotKey, Source,
    Velocity,
};
use serde::Deserialize;

use crate::beacon::{Beacon, Listener};
pub use crate::config::Config;
use crate::conn::{Direction, Tap};
use crate::event::{Event, PuttResult};
use crate::packet::{Hand, Request};
use crate::session::Session;
#[cfg(feature = "testing")]
pub use tuning::Tuning;
#[cfg(not(feature = "testing"))]
use tuning::Tuning;

/// Reconnect delay cap for the first [`QUICK_RETRIES`] attempts after a
/// session that was measuring, so a device that went quiet for a moment is
/// back quickly. Later attempts use the host's backoff.
const QUICK_RETRY: Duration = Duration::from_secs(1);
const QUICK_RETRIES: u32 = 3;

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

/// Longest wait for a device event in one poll.
const EVENT_POLL: Duration = Duration::from_millis(100);

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

/// The ExPutt RG as a flightrelay `DeviceDriver`. See the [module docs](self).
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
    /// A driver with test timings instead of the device's ([`Tuning`]).
    ///
    /// # Errors
    ///
    /// The [`ConfigError`] of a config that is not usable.
    pub fn with_tuning(config: Config, tuning: Tuning) -> Result<Self, ConfigError> {
        let mut driver = <Self as DeviceDriver>::new(config)?;
        driver.unanswered = Unanswered::new(tuning.not_answering_after);
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
        let started = Instant::now();
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
            if !hinted && started.elapsed() >= self.tuning.discovery_hint_after {
                hinted = true;
                ctx.out.log(
                    Level::Info,
                    format!(
                        "no ExPutt found on the network after {}s",
                        started.elapsed().as_secs()
                    ),
                );
                // Warn is the lowest alert severity.
                ctx.out.alert(Severity::Warn, DISCOVERY_HINT);
            }
        }
    }
}

impl DeviceDriver for ExPutt {
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
            unanswered: Unanswered::new(tuning.not_answering_after),
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
        let resync_started = Instant::now();
        let session = Session::start_tapped(ip, &self.tuning.session_options(hand), raw.tap())?;
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
        let now = Instant::now();
        let mut session = ExPuttSession {
            session,
            identity,
            ids: self.ids.clone(),
            raw,
            raw_rx,
            watchdog: Watchdog::new(self.tuning.silence(), now),
            gate: ResyncGate::new(self.tuning.resync_gap, hand, now),
            device_ready: "false",
            silent: false,
            mat_search: Some((resync_started, true)),
            published: HashMap::new(),
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
        let Some(error) = ended.error else {
            return Some(backoff.delay(ended.connected_for));
        };
        // A stop is no failure: no alert, and the reconnect policy stays.
        if matches!(error, Error::Stopped) {
            return Some(backoff.delay(ended.connected_for));
        }

        // A device that accepts the connection and never answers is not
        // hammered (see `Retry`); the user is told once, and the per-attempt
        // alerts stop until a session succeeds.
        let mut tell_not_answering = false;
        if ended.connected_for.is_some() {
            self.unanswered.recovered();
        } else if matches!(error, Error::Device(crate::Error::Timeout(_))) {
            tell_not_answering = self.unanswered.no_answer(Instant::now());
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
        Some(
            self.retry
                .delay(backoff, ended.connected_for, silent, self.tuning.stall_wait),
        )
    }
}

/// A measuring session with the ExPutt. See the [module docs](self).
#[derive(Debug)]
pub struct ExPuttSession {
    session: Session,
    identity: DeviceIdentity,
    ids: ShotIds,
    raw: RawTap,
    raw_rx: Receiver<Message>,
    watchdog: Watchdog,
    gate: ResyncGate,
    /// The last `ready` the device itself reported; what is published also
    /// depends on a waiting mat search and on silence.
    device_ready: &'static str,
    /// The device has been silent past the warning threshold; not ready as
    /// published until it sends something again.
    silent: bool,
    /// The mat search in progress, when it started, and whether it is the
    /// session's own (whose time includes connect and handshake).
    mat_search: Option<(Instant, bool)>,
    /// Status as last reported: `ready` and `device_state`.
    published: HashMap<&'static str, &'static str>,
    /// When the ball was struck (UTC ms), until the putt's result or the
    /// device is ready again.
    struck_at: Option<u64>,
    /// A hint or action asked for a mat search since the last poll, which
    /// logs whether it is held back.
    hold_to_log: bool,
    totals: Arc<Totals>,
}

impl ExPuttSession {
    /// Set `key` to `value`; whether that changed anything.
    fn set(&mut self, key: &'static str, value: &'static str) -> bool {
        self.published.insert(key, value) != Some(value)
    }

    /// `ready` as published: not ready while a requested mat search is waiting
    /// or the device has been silent past the warning threshold.
    fn published_ready(&self) -> &'static str {
        if self.silent || self.gate.pending() {
            "false"
        } else {
            self.device_ready
        }
    }

    /// Report `ready` (as the blockers: none, or `not_armed`) and
    /// `device_state` (telemetry, when given), each only if it changed. What
    /// goes out is the whole status: the device has no battery or attitude,
    /// and `device_state` is the last state it reported.
    fn report(&mut self, out: &mut Events<'_>, device_state: Option<&'static str>) {
        let ready = self.published_ready();
        let mut changed = self.set("ready", ready);
        if let Some(state) = device_state {
            changed |= self.set("device_state", state);
        }
        if !changed {
            return;
        }
        let mut status = if ready == "true" {
            DeviceStatus::new()
        } else {
            DeviceStatus::not_armed()
        };
        if let Some(state) = self.published.get("device_state") {
            status = status.with_telemetry("device_state", *state);
        }
        out.status(status);
    }

    /// Logs how long a requested mat search is held back, if it is.
    fn log_hold(&self, out: &mut Events<'_>) {
        if let Some(left) = self.gate.hold(Instant::now()) {
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
            match classify(message.dir, &message.body) {
                Frame::Shared(label) if out.wants_raw() => {
                    let key = putt.filter(|_| label == PUTT_RESULT_LABEL).cloned();
                    out.frp(FrpBody::ShotRaw(RawFrame {
                        timestamp: message.at,
                        label: Some(label.to_owned()),
                        key,
                        ..RawFrame::new(dir, 0, message.body)
                    }));
                }
                Frame::Shared(label) | Frame::Private(label) => {
                    out.audit(dir, label, &message.body);
                }
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

    /// A device event: the watchdog, then readiness and the device state.
    fn handle(&mut self, out: &mut Events<'_>, event: Event) {
        if let Some(spell) = self.watchdog.event(&event, Instant::now()) {
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
                self.log_mat_found(out);
                (None, Some("mat found"))
            }
            Event::PuttReady => {
                out.log(Level::Info, "ready for a putt");
                self.struck_at = None;
                (Some("true"), Some("ready"))
            }
            Event::BallStruck => {
                out.log(Level::Info, "ball struck");
                self.struck_at = now_ms();
                (Some("false"), Some("ball struck"))
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
        if let Some(ready) = ready {
            self.device_ready = ready;
        }
        self.report(out, device_state);
    }

    fn log_mat_found(&mut self, out: &mut Events<'_>) {
        let Some((started, at_start)) = self.mat_search.take() else {
            return;
        };
        let took = started.elapsed();
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

    /// No event within the poll: the watchdog decides.
    fn quiet(&mut self, out: &mut Events<'_>) -> Result<(), Error> {
        match self.watchdog.check(Instant::now()) {
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
                self.report(out, None);
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

impl DeviceSession for ExPuttSession {
    type Driver = ExPutt;

    fn poll(&mut self, timeout: Duration, out: &mut Events<'_>) -> Result<(), Error> {
        self.raw.want(out.wants_raw() || out.enabled(Level::Trace));
        if std::mem::take(&mut self.hold_to_log) {
            self.log_hold(out);
        }

        // A mat search runs at most once per gap, with the latest hand.
        let now = Instant::now();
        if let Some(search) = self.gate.due(now) {
            match search {
                Resync::Restart(hand) => {
                    out.log(Level::Info, format!("hand -> {hand:?}, finding the mat"));
                    self.session.restart_match(hand)?;
                }
                Resync::Find => {
                    out.log(Level::Info, "finding the mat");
                    self.session.find_mat()?;
                }
            }
            self.totals.resyncs.fetch_add(1, Ordering::Relaxed);
            self.mat_search = Some((now, false));
            self.device_ready = "false";
        }
        self.report(out, None);

        let event = match self.session.events().recv_timeout(timeout.min(EVENT_POLL)) {
            Ok(event) => event,
            Err(RecvTimeoutError::Timeout) => {
                self.drain_raw(out, None);
                return self.quiet(out);
            }
            Err(RecvTimeoutError::Disconnected) => {
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
        self.handle(out, event);
        Ok(())
    }

    /// Follow the golfer's handedness: a new round when it differs from the
    /// device's hand, which finds the mat first, so the device is not ready
    /// until it reports ready again. No handedness hint keeps the hand. The
    /// mode and the club do not apply: the device only measures putts.
    fn hints(&mut self, hints: &TurnHints) -> Result<(), Error> {
        if let Some(handed) = hints.handedness {
            self.gate.want_hand(hand_of(Some(handed)));
            self.hold_to_log = true;
        }
        Ok(())
    }

    fn action(&mut self, action: &Action) -> Result<(), Error> {
        match action {
            Action::FindMat => {
                self.gate.want_find();
                self.hold_to_log = true;
            }
        }
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
        out.status(DeviceStatus::not_armed());
        if let Err(e) = self.session.close() {
            out.log(Level::Debug, format!("session close: {e}"));
        }
        Ok(())
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

/// The current UTC time in milliseconds since the Unix epoch.
fn now_ms() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_millis()).ok())
}

// ---------------------------------------------------------------------------
// Raw frames
// ---------------------------------------------------------------------------

/// One message the tap saw.
#[derive(Debug)]
struct Message {
    dir: Direction,
    /// When it crossed the socket (UTC ms).
    at: Option<u64>,
    body: Vec<u8>,
}

/// Queues the session's messages for the device thread while a host wants
/// raw frames or trace lines.
#[derive(Debug, Clone)]
struct RawTap {
    wanted: Arc<AtomicBool>,
    queue: Sender<Message>,
}

impl RawTap {
    fn new() -> (Self, Receiver<Message>) {
        let (queue, rx) = mpsc::channel();
        (
            Self {
                wanted: Arc::new(AtomicBool::new(false)),
                queue,
            },
            rx,
        )
    }

    fn want(&self, wanted: bool) {
        self.wanted.store(wanted, Ordering::Relaxed);
    }

    /// The tap the session calls on its reader and writer threads.
    fn tap(&self) -> Tap {
        let this = self.clone();
        Arc::new(move |dir, body: &[u8]| {
            if this.wanted.load(Ordering::Relaxed) {
                let _ = this.queue.send(Message {
                    dir,
                    at: now_ms(),
                    body: body.to_vec(),
                });
            }
        })
    }
}

/// The label of a putt result's frame.
const PUTT_RESULT_LABEL: &str = "PuttResult";

/// Whether a message may leave the machine as a raw frame, and its label.
#[derive(Debug, PartialEq, Eq)]
enum Frame {
    /// Carries no device or user identity: a raw frame.
    Shared(&'static str),
    /// A camera image (the player's surroundings), or a message the crate
    /// does not recognize, which it cannot vouch for: an audit line only.
    Private(&'static str),
}

/// The crate's identity-exclusion list. Everything this client sends is a
/// fixed packet; of what the device sends, the known events are shared and
/// camera frames and unrecognized messages are not.
fn classify(dir: Direction, body: &[u8]) -> Frame {
    match dir {
        Direction::Tx => Frame::Shared(sent_label(body)),
        Direction::Rx => match Event::parse_filtered(body, false) {
            None => Frame::Private("camera frame"),
            Some(event) => match event {
                Event::Ack { .. } => Frame::Shared("N_OK"),
                Event::Heartbeat => Frame::Shared("Test"),
                Event::FindingMat => Frame::Shared("FindMat"),
                Event::MatFound => Frame::Shared("MatOK"),
                Event::PuttReady => Frame::Shared("PuttReady"),
                Event::BallStruck => Frame::Shared("PuttShot"),
                Event::Indicator => Frame::Shared("IndiCator"),
                Event::IndicatorOut => Frame::Shared("IndiCatorOut"),
                Event::PuttResult(_) => Frame::Shared(PUTT_RESULT_LABEL),
                Event::CameraFrame(_) => Frame::Private("camera frame"),
                Event::Unknown { .. } => Frame::Private("unknown"),
            },
        },
    }
}

/// The name of a packet this client sent: the command of an `N_TEST`, else
/// the packet type.
fn sent_label(body: &[u8]) -> &'static str {
    const SENT: [Request; 11] = [
        Request::VerifyId,
        Request::Connect,
        Request::StartCamera,
        Request::ResyncToMat,
        Request::StopCamera,
        Request::SetHand(Hand::Right),
        Request::SetHand(Hand::Left),
        Request::MatchBegin,
        Request::MatchEnded,
        Request::Alive,
        Request::AppPause,
    ];
    #[derive(Deserialize)]
    struct Head {
        #[serde(rename = "PacketID")]
        packet_id: Option<String>,
        #[serde(rename = "Comment")]
        comment: Option<String>,
    }
    let Ok(head) = serde_json::from_slice::<Head>(body) else {
        return "unknown";
    };
    SENT.iter()
        .find(|r| {
            Some(r.packet_id()) == head.packet_id.as_deref()
                && r.comment() == head.comment.as_deref()
        })
        .map_or("unknown", |r| match (r.packet_id(), r.comment()) {
            ("N_TEST", Some(command)) => command,
            (packet_id, _) => packet_id,
        })
}

// ---------------------------------------------------------------------------
// Policies
// ---------------------------------------------------------------------------

/// Counts since the driver was built, kept across reconnects, for the logs.
#[derive(Debug, Default)]
struct Totals {
    connects: AtomicU32,
    resyncs: AtomicU32,
}

/// Reconnect delays: the host's backoff, shortened for the first
/// [`QUICK_RETRIES`] attempts after a session that was measuring, and
/// lengthened after a session lost to silence.
#[derive(Debug, Default)]
struct Retry {
    /// Attempts left whose delay is capped at [`QUICK_RETRY`].
    quick_left: u32,
    /// The last session that ended was lost to silence and none has
    /// succeeded since.
    calm: bool,
}

impl Retry {
    /// Delay before the next attempt. `session` is how long the attempt that
    /// just ended was measuring, `None` if it never got there; `silent` is
    /// whether it ended because the device went silent. After a silent
    /// session every delay is at least `stall_wait`, until a session
    /// succeeds again.
    fn delay(
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
struct Unanswered {
    /// How long the outage may go on before the user is told.
    after: Duration,
    /// When the first unanswered attempt of the outage happened.
    since: Option<Instant>,
    /// The user has been told about this outage.
    told: bool,
}

impl Unanswered {
    fn new(after: Duration) -> Self {
        Self {
            after,
            since: None,
            told: false,
        }
    }

    /// An attempt got no answer at `now`. Whether the user is to be told
    /// now: once per outage.
    fn no_answer(&mut self, now: Instant) -> bool {
        let since = *self.since.get_or_insert(now);
        if !self.told && now.saturating_duration_since(since) >= self.after {
            self.told = true;
            return true;
        }
        false
    }

    /// A session succeeded: the outage is over.
    fn recovered(&mut self) {
        self.since = None;
        self.told = false;
    }
}

/// What a mat search is asked for.
#[derive(Debug, PartialEq, Eq)]
enum Resync {
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
struct ResyncGate {
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
    fn new(gap: Duration, hand: Hand, now: Instant) -> Self {
        Self {
            gap,
            last: now,
            hand,
            want: hand,
            find: false,
        }
    }

    fn want_hand(&mut self, hand: Hand) {
        self.want = hand;
    }

    fn want_find(&mut self) {
        self.find = true;
    }

    /// Something is asked for that has not run.
    fn pending(&self) -> bool {
        self.find || self.want != self.hand
    }

    /// Time left until a pending search may run, `None` if none is pending or
    /// it may run now.
    fn hold(&self, now: Instant) -> Option<Duration> {
        if !self.pending() {
            return None;
        }
        self.gap
            .checked_sub(now.saturating_duration_since(self.last))
            .filter(|left| !left.is_zero())
    }

    /// The search to start at `now`, if one is pending and the gap is over.
    fn due(&mut self, now: Instant) -> Option<Resync> {
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
struct SilenceLimits {
    /// Longest silence before the session is treated as lost.
    baseline: Duration,
    /// Longest silence after a strike, until the putt's result or the device
    /// is ready again.
    after_strike: Duration,
    /// Silence after which one warning is logged and `ready` turns false.
    warn_after: Duration,
}

/// What a quiet spell amounts to so far.
#[derive(Debug, PartialEq, Eq)]
enum Quiet {
    /// Within the limits.
    Fine,
    /// Past the warning threshold for the first time in this spell.
    Warn(Duration),
    /// Past the limit: the session is lost.
    Lost(Duration),
}

/// Tracks silence from the device against [`SilenceLimits`].
#[derive(Debug)]
struct Watchdog {
    limits: SilenceLimits,
    last: Instant,
    /// A strike was seen and its result has not arrived yet.
    awaiting_result: bool,
    /// The current quiet spell has been warned about.
    warned: bool,
}

impl Watchdog {
    fn new(limits: SilenceLimits, now: Instant) -> Self {
        Self {
            limits,
            last: now,
            awaiting_result: false,
            warned: false,
        }
    }

    /// A device event arrived at `now`. Returns the length of the quiet spell
    /// it ends when that spell was warned about.
    fn event(&mut self, event: &Event, now: Instant) -> Option<Duration> {
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
    fn limit(&self) -> Duration {
        if self.awaiting_result {
            self.limits.after_strike
        } else {
            self.limits.baseline
        }
    }

    fn check(&mut self, now: Instant) -> Quiet {
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

#[cfg(test)]
mod tests;
