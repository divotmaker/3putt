//! The driver on one thread, in virtual time: `TestHost` steps the session
//! over a channel [`Link`] that the test plays the device on.

use std::sync::Mutex;

use flightrelay::driver::{ConnectHost, DeviceEvent, HostFlags, Stop};
use flightrelay::testing::{TestHost, alerts, bodies, logs, raw_frames, statuses};
use flightrelay::{Club, ShotClub, ShotFinished, ShotLaunch, ShotTrigger};

use super::*;
use crate::packet::Request;

const PUTT_READY: &str = "PuttReady,106";
const BALL_STRUCK: &str = "PuttShot,107";
const HEARTBEAT: &str = "Test,yy";
const CAMERA_FRAME: &str = "/9j/4AAQSkZJRgABAQ==";

const LOCALHOST: IpAddr = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);

/// What the driver asked of the link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sent {
    /// The session started with this hand.
    Start(Hand),
    FindMat,
    Restart(Hand),
    Close,
}

/// The driver's end of the channel.
#[derive(Debug)]
struct ChanLink {
    events: Receiver<Event>,
    sent: Arc<Mutex<Vec<Sent>>>,
}

impl Link for ChanLink {
    fn events(&self) -> &Receiver<Event> {
        &self.events
    }

    fn find_mat(&self) -> crate::Result<()> {
        self.sent.lock().unwrap().push(Sent::FindMat);
        Ok(())
    }

    fn restart_match(&self, hand: Hand) -> crate::Result<()> {
        self.sent.lock().unwrap().push(Sent::Restart(hand));
        Ok(())
    }

    fn close(self) -> crate::Result<()> {
        self.sent.lock().unwrap().push(Sent::Close);
        Ok(())
    }
}

/// The test's end: the device. Like the TCP session, every message passes
/// the raw tap before its event is delivered.
struct Device {
    events: Option<Sender<Event>>,
    sent: Arc<Mutex<Vec<Sent>>>,
    tap: Arc<Mutex<Option<Tap>>>,
}

impl Device {
    /// The device sends the event `comment` (e.g. `PuttReady,106`).
    fn send(&self, comment: &str) {
        let body = device_event(comment);
        self.tap(Direction::Rx, &body);
        if let Some(events) = &self.events {
            let _ = events.send(Event::parse(&body));
        }
    }

    /// A message that passes the tap and delivers no event: a sent request,
    /// or a camera frame the session drops.
    fn tap(&self, dir: Direction, body: &[u8]) {
        if let Some(tap) = &*self.tap.lock().unwrap() {
            tap(dir, body);
        }
    }

    /// The device closes the connection.
    fn disconnect(&mut self) {
        self.events = None;
    }

    /// What the driver asked of the link since the last call.
    fn sent(&self) -> Vec<Sent> {
        std::mem::take(&mut *self.sent.lock().unwrap())
    }
}

/// A driver with the device's defaults.
fn driver() -> ExPutt {
    ExPutt::new(Config::default()).expect("driver")
}

/// The device side and the link `driver.open` starts with.
fn link_start() -> (
    Device,
    impl FnOnce(&Tuning, Hand, Tap) -> crate::Result<ChanLink>,
) {
    let (tx, events) = mpsc::channel();
    let sent = Arc::new(Mutex::new(Vec::new()));
    let tap = Arc::new(Mutex::new(None));
    let device = Device {
        events: Some(tx),
        sent: Arc::clone(&sent),
        tap: Arc::clone(&tap),
    };
    let start = move |_: &Tuning, hand: Hand, t: Tap| {
        *tap.lock().unwrap() = Some(t);
        sent.lock().unwrap().push(Sent::Start(hand));
        Ok(ChanLink { events, sent })
    };
    (device, start)
}

/// A session on `host`, measuring.
fn open(host: &mut TestHost, driver: &mut ExPutt) -> (Measuring<ChanLink>, Device) {
    let (device, start) = link_start();
    let session = host
        .connect_with(|ctx| driver.open(ctx, LOCALHOST, None, start))
        .expect("open");
    (session, device)
}

/// Step at the current instant until nothing is waiting.
fn settle(host: &mut TestHost, s: &mut Measuring<ChanLink>) {
    while host.with(|now, out| s.step(now, out)).expect("step") == Activity::Active {}
}

/// `d` of time, with the device's heartbeat once a second, up to and
/// including a step at its end.
fn live_for(host: &mut TestHost, s: &mut Measuring<ChanLink>, device: &Device, d: Duration) {
    let end = host.now() + d;
    while let Some(left) = end
        .checked_duration_since(host.now())
        .filter(|d| !d.is_zero())
    {
        device.send(HEARTBEAT);
        host.run_for(left.min(Duration::from_secs(1)), |now, out| {
            s.step(now, out)
        })
        .expect("step");
    }
    settle(host, s);
}

/// `(ready, device_state)` of each status the host holds, in order.
fn states(events: &[DeviceEvent]) -> Vec<(bool, Option<String>)> {
    statuses(events)
        .iter()
        .map(|s| {
            (
                s.blockers.is_empty(),
                s.telemetry.get("device_state").cloned(),
            )
        })
        .collect()
}

/// The `device_state` of each status, in order, repeats folded.
fn device_states(events: &[DeviceEvent]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for (_, state) in states(events) {
        if let Some(state) = state
            && out.last() != Some(&state)
        {
            out.push(state);
        }
    }
    out
}

fn ready(events: &[DeviceEvent]) -> Option<bool> {
    statuses(events).last().map(|s| s.blockers.is_empty())
}

fn alert_messages(events: &[DeviceEvent]) -> Vec<String> {
    alerts(events).iter().map(|a| a.message.clone()).collect()
}

fn logged(events: &[DeviceEvent], level: Level, prefix: &str) -> bool {
    logs(events)
        .iter()
        .any(|(l, line)| *l == level && line.starts_with(prefix))
}

/// The shot events, trigger to finish.
fn shot(events: &[DeviceEvent]) -> Vec<FrpBody> {
    bodies(events)
        .into_iter()
        .filter(|b| b.kind().starts_with("shot.") && b.kind() != "shot.raw")
        .cloned()
        .collect()
}

fn raw_labels(events: &[DeviceEvent]) -> Vec<String> {
    raw_frames(events)
        .iter()
        .filter_map(|f| f.label.clone())
        .collect()
}

#[test]
fn a_session_starts_not_ready_and_is_ready_once_the_device_says_so() {
    let mut host = TestHost::new();
    let mut driver = driver();
    let (mut s, device) = open(&mut host, &mut driver);
    let connect = host.take();
    let phases: Vec<&str> = connect
        .iter()
        .filter_map(|e| match e {
            DeviceEvent::Phase(p) => Some(*p),
            _ => None,
        })
        .collect();
    assert_eq!(phases, ["connecting", "measuring"]);
    assert_eq!(device.sent(), [Sent::Start(Hand::Right)], "no hint: right");
    assert_eq!(s.identity.device, "ExPutt RG (127.0.0.1)");
    assert_eq!(s.identity.model.as_deref(), Some("ExPutt RG"));
    assert_eq!(s.identity.firmware, None);

    settle(&mut host, &mut s);
    assert_eq!(states(&host.take()), [(false, None)]);
    device.send("MatOK,102");
    device.send(PUTT_READY);
    settle(&mut host, &mut s);
    let seen = host.take();
    assert_eq!(device_states(&seen), ["mat found", "ready"]);
    assert_eq!(ready(&seen), Some(true));
    assert!(logged(
        &seen,
        Level::Info,
        "mat found 0.0s into session start"
    ));

    host.with(|_, out| s.close(out)).expect("close");
    assert_eq!(ready(&host.take()), Some(false), "not ready after close");
    assert_eq!(device.sent(), [Sent::Close]);
}

#[test]
fn putts_are_putter_shots_numbered_in_order() {
    let mut host = TestHost::new();
    let mut driver = driver();
    let (mut s, device) = open(&mut host, &mut driver);
    settle(&mut host, &mut s);
    host.take();

    device.send(PUTT_RESULT_FULL);
    settle(&mut host, &mut s);
    let events = shot(&host.take());
    let [
        FrpBody::ShotTrigger(trigger),
        FrpBody::ShotLaunch(ShotLaunch { key: k1, launch }),
        FrpBody::ShotClub(ShotClub { key: k2, club }),
        FrpBody::ShotFinished(ShotFinished { key: k3 }),
    ] = &events[..]
    else {
        panic!("{events:#?}");
    };
    assert_eq!(trigger.key.shot_number, 1);
    assert!([k1, k2, k3].iter().all(|k| **k == trigger.key));
    assert_eq!(trigger.timestamp, None, "no strike seen, no strike time");
    assert_eq!(*launch, launch_of(&putt(PUTT_RESULT_FULL)));
    assert_eq!(Some(club.clone()), club_of(&putt(PUTT_RESULT_FULL)));

    // An untracked putter: ball data only.
    device.send(PUTT_RESULT_BALL_ONLY);
    settle(&mut host, &mut s);
    let events = shot(&host.take());
    assert!(
        matches!(
            &events[..],
            [
                FrpBody::ShotTrigger(ShotTrigger { key, .. }),
                FrpBody::ShotLaunch(_),
                FrpBody::ShotFinished(_)
            ] if key.shot_number == 2
        ),
        "{events:#?}"
    );
}

#[test]
fn a_strike_dates_the_shot_and_a_strike_taken_back_dates_nothing() {
    let mut host = TestHost::new();
    let mut driver = driver();
    let (mut s, device) = open(&mut host, &mut driver);
    device.send(PUTT_READY);
    settle(&mut host, &mut s);

    let before = utc_ms().expect("clock");
    device.send(BALL_STRUCK);
    settle(&mut host, &mut s);
    let after = utc_ms().expect("clock");
    device.send(PUTT_RESULT_FULL);
    settle(&mut host, &mut s);
    let events = shot(&host.take());
    let FrpBody::ShotTrigger(ShotTrigger {
        timestamp: Some(at),
        ..
    }) = &events[0]
    else {
        panic!("{events:?}");
    };
    assert!((before..=after).contains(at), "{before} <= {at} <= {after}");

    for comment in [PUTT_READY, BALL_STRUCK, PUTT_READY, PUTT_RESULT_FULL] {
        device.send(comment);
    }
    settle(&mut host, &mut s);
    let events = shot(&host.take());
    assert!(
        matches!(
            events[0],
            FrpBody::ShotTrigger(ShotTrigger {
                timestamp: None,
                ..
            })
        ),
        "{events:?}"
    );
}

#[test]
fn readiness_follows_the_putt_cycle() {
    let mut host = TestHost::new();
    let mut driver = driver();
    let (mut s, device) = open(&mut host, &mut driver);
    device.send(PUTT_READY);
    settle(&mut host, &mut s);
    assert_eq!(ready(&host.take()), Some(true));

    // Struck: not ready until the device says so again.
    device.send(BALL_STRUCK);
    settle(&mut host, &mut s);
    assert_eq!(
        states(&host.take()),
        [(false, Some("ball struck".to_owned()))]
    );

    // The result changes neither value.
    device.send(PUTT_RESULT_FULL);
    settle(&mut host, &mut s);
    assert_eq!(states(&host.take()), []);

    device.send(PUTT_READY);
    settle(&mut host, &mut s);
    assert_eq!(states(&host.take()), [(true, Some("ready".to_owned()))]);
}

/// A device that closes the connection ends the session; the next attempt
/// comes quickly after a measuring session, and shot numbers go on.
#[test]
fn a_lost_session_is_retried_quickly_and_shot_numbers_go_on() {
    let mut host = TestHost::new();
    let mut driver = driver();
    let (mut s, mut device) = open(&mut host, &mut driver);
    device.send(PUTT_RESULT_FULL);
    device.disconnect();
    let error = host
        .run_for(Duration::from_secs(1), |now, out| s.step(now, out))
        .unwrap_err();
    assert!(matches!(error, Error::Lost), "{error:?}");
    host.with(|_, out| s.close(out)).expect("close");
    let seen = host.take();
    assert_eq!(shot(&seen).len(), 4, "the putt before the close went out");
    assert!(logged(&seen, Level::Info, "device closed the connection"));
    assert_eq!(ready(&seen), Some(false));

    let mut backoff = Backoff::new(Duration::from_millis(50), Duration::from_secs(5));
    let delay = host.with(|now, out| {
        driver.retry_delay_at(
            &SessionEnd {
                error: Some(&error),
                connected_for: Some(Duration::from_secs(1)),
                attempts: 0,
            },
            &mut backoff,
            out,
            now,
        )
    });
    assert_eq!(delay, Duration::from_millis(50));
    assert_eq!(
        alert_messages(&host.take()),
        ["ExPutt error: connection to the device lost"]
    );

    let (mut s, device) = open(&mut host, &mut driver);
    device.send(PUTT_RESULT_FULL);
    settle(&mut host, &mut s);
    let FrpBody::ShotTrigger(trigger) = &shot(&host.take())[0] else {
        unreachable!()
    };
    assert_eq!(trigger.key.shot_number, 2);
}

/// Silence: not ready from 10 s until the device sends again, lost at 90 s
/// (120 s after a strike), and the next attempt waits the stall wait.
#[test]
fn a_silent_device_is_not_ready_from_10s_and_lost_at_90s() {
    let mut host = TestHost::new();
    let mut driver = driver();
    let (mut s, device) = open(&mut host, &mut driver);
    device.send(PUTT_READY);
    settle(&mut host, &mut s);
    host.take();

    host.run_for(SILENCE.warn_after, |now, out| s.step(now, out))
        .expect("within the limit");
    let seen = host.take();
    assert_eq!(ready(&seen), None, "fine until the warning");
    host.with(|now, out| s.step(now, out)).expect("step");
    let seen = host.take();
    assert_eq!(ready(&seen), Some(false));
    assert!(logged(
        &seen,
        Level::Warn,
        "no data from the device for 10.0s"
    ));

    device.send(HEARTBEAT);
    settle(&mut host, &mut s);
    let seen = host.take();
    assert_eq!(ready(&seen), Some(true));
    assert!(logged(
        &seen,
        Level::Info,
        "device sent data again after 10.0s"
    ));

    // After a strike the limit is 120 s.
    device.send(BALL_STRUCK);
    settle(&mut host, &mut s);
    let struck = host.now();
    let error = host
        .run_for(Duration::from_secs(200), |now, out| s.step(now, out))
        .unwrap_err();
    assert!(matches!(error, Error::Silent(d) if d == SILENCE.after_strike));
    assert_eq!(host.now() - struck, SILENCE.after_strike);
    host.with(|_, out| s.close(out)).expect("close");

    // The next attempts wait at least the stall wait.
    let mut backoff = Backoff::new(Duration::from_millis(50), Duration::from_secs(5));
    let delay = host.with(|now, out| {
        driver.retry_delay_at(
            &SessionEnd {
                error: Some(&error),
                connected_for: Some(Duration::from_secs(130)),
                attempts: 0,
            },
            &mut backoff,
            out,
            now,
        )
    });
    assert_eq!(delay, STALL_RECONNECT_WAIT);

    // Without a strike, 90 s.
    host.take();
    let (mut s, _device) = open(&mut host, &mut driver);
    let opened = host.now();
    let error = host
        .run_for(Duration::from_secs(200), |now, out| s.step(now, out))
        .unwrap_err();
    assert!(matches!(error, Error::Silent(d) if d == SILENCE.baseline));
    assert_eq!(host.now() - opened, SILENCE.baseline);
    assert!(logged(
        &host.take(),
        Level::Warn,
        "giving up on the device after 90s"
    ));
}

/// A hand change is a new round, held until 10 s after the last mat
/// search (the session's own included); requests inside the gap fold into
/// the latest hand.
#[test]
fn the_hand_follows_the_player_one_round_per_gap() {
    let mut host = TestHost::new();
    let mut driver = driver();
    let (mut s, device) = open(&mut host, &mut driver);
    device.send(PUTT_READY);
    settle(&mut host, &mut s);
    assert_eq!(device.sent(), [Sent::Start(Hand::Right)]);
    host.take();

    // Left, right, left again inside the gap: one round, to the left.
    host.advance(Duration::from_secs(4));
    host.with(|_, out| {
        s.hints(
            &TurnHints::default().with_handedness(Handedness::Right),
            out,
        )
    })
    .expect("hints");
    host.with(|_, out| {
        s.hints(
            &TurnHints::default()
                .with_mode(Mode::Full)
                .with_club(Club::Driver)
                .with_handedness(Handedness::Left),
            out,
        )
    })
    .expect("hints");
    settle(&mut host, &mut s);
    let seen = host.take();
    assert!(logged(&seen, Level::Info, "mat search held for 6.0s"));
    assert_eq!(ready(&seen), Some(false), "not ready while one waits");
    assert_eq!(device.sent(), []);

    live_for(&mut host, &mut s, &device, Duration::from_secs(6));
    assert_eq!(device.sent(), [Sent::Restart(Hand::Left)]);
    for comment in ["FindMat,101", "MatOK,102", PUTT_READY] {
        device.send(comment);
    }
    host.advance(Duration::from_secs(2));
    settle(&mut host, &mut s);
    let seen = host.take();
    assert_eq!(device_states(&seen), ["finding mat", "mat found", "ready"]);
    assert_eq!(ready(&seen), Some(true));
    assert!(logged(&seen, Level::Info, "mat found after 2.0s"));

    // The same hand again asks for nothing.
    host.with(|_, out| s.hints(&TurnHints::default().with_handedness(Handedness::Left), out))
        .expect("hints");
    live_for(&mut host, &mut s, &device, RESYNC_GAP);
    assert_eq!(device.sent(), []);
    assert_eq!(states(&host.take()), []);
}

/// A hand change inside the gap is held, not ready meanwhile; a change back
/// to the device's hand drops it, and nothing is sent.
#[test]
fn a_hand_flipped_back_inside_the_gap_sends_nothing() {
    let mut host = TestHost::new();
    let mut driver = driver();
    let (mut s, device) = open(&mut host, &mut driver);
    device.send(PUTT_READY);
    settle(&mut host, &mut s);
    device.sent();
    host.take();

    host.with(|_, out| s.hints(&TurnHints::default().with_handedness(Handedness::Left), out))
        .expect("hints");
    settle(&mut host, &mut s);
    assert_eq!(ready(&host.take()), Some(false));
    host.with(|_, out| {
        s.hints(
            &TurnHints::default().with_handedness(Handedness::Right),
            out,
        )
    })
    .expect("hints");
    settle(&mut host, &mut s);
    assert_eq!(ready(&host.take()), Some(true));
    live_for(&mut host, &mut s, &device, RESYNC_GAP);
    assert_eq!(device.sent(), []);
}

#[test]
fn a_hand_hinted_while_the_session_starts_is_a_restart_to_it() {
    /// A connect host whose hints turn left-handed after the first tick,
    /// while the session starts.
    struct TurnsLeft {
        hints: TurnHints,
        now: Instant,
    }
    impl ConnectHost for TurnsLeft {
        fn tick(&mut self) {
            self.hints = TurnHints::default().with_handedness(Handedness::Left);
        }
        fn hints(&self) -> &TurnHints {
            &self.hints
        }
        fn now(&self) -> Instant {
            self.now
        }
    }

    let mut host = TestHost::new();
    let mut connect = TurnsLeft {
        hints: TurnHints::default(),
        now: host.now(),
    };
    let mut driver = driver();
    let (device, start) = link_start();
    let (stop, flags) = (Stop::new(), HostFlags::new());
    let mut sink = |_: DeviceEvent| {};
    let mut ctx = ConnectCtx::new(&stop, Events::new(&mut sink, &flags), &mut connect);
    let mut s = driver.open(&mut ctx, LOCALHOST, None, start).expect("open");
    assert_eq!(device.sent(), [Sent::Start(Hand::Right)]);
    host.advance(RESYNC_GAP);
    settle(&mut host, &mut s);
    assert_eq!(device.sent(), [Sent::Restart(Hand::Left)]);
}

/// A turn with no handedness keeps the device's hand.
#[test]
fn a_mode_or_club_alone_changes_nothing() {
    let mut host =
        TestHost::new().with_hints(TurnHints::default().with_handedness(Handedness::Left));
    let mut driver = driver();
    let (mut s, device) = open(&mut host, &mut driver);
    device.send(PUTT_READY);
    settle(&mut host, &mut s);
    assert_eq!(device.sent(), [Sent::Start(Hand::Left)], "starts left");
    host.take();

    host.with(|_, out| s.hints(&TurnHints::default().with_mode(Mode::Chipping), out))
        .expect("hints");
    host.with(|_, out| s.hints(&TurnHints::default().with_club(Club::PitchingWedge), out))
        .expect("hints");
    host.with(|_, out| s.hints(&TurnHints::default(), out))
        .expect("hints");
    live_for(&mut host, &mut s, &device, RESYNC_GAP);
    assert_eq!(device.sent(), []);
    assert_eq!(states(&host.take()), []);
}

#[test]
fn find_mat_resyncs_the_device_and_a_slow_search_is_logged() {
    let mut host = TestHost::new();
    let mut driver = driver();
    let (mut s, device) = open(&mut host, &mut driver);
    device.send(PUTT_READY);
    live_for(&mut host, &mut s, &device, RESYNC_GAP);
    device.sent();
    host.take();

    // Past the gap: at once.
    host.with(|_, out| s.action(&Action::FindMat, out))
        .expect("action");
    settle(&mut host, &mut s);
    assert_eq!(device.sent(), [Sent::FindMat]);
    device.send("FindMat,101");
    live_for(&mut host, &mut s, &device, Duration::from_secs(4));
    device.send("MatOK,102");
    device.send(PUTT_READY);
    settle(&mut host, &mut s);
    let seen = host.take();
    assert_eq!(
        device_states(&seen),
        ["ready", "finding mat", "mat found", "ready"]
    );
    assert!(logged(&seen, Level::Warn, "mat search took 4.0s, over 3s"));

    // A request at shutdown is dropped.
    host.with(|_, out| s.action(&Action::FindMat, out))
        .expect("action");
    host.with(|_, out| s.close(out)).expect("close");
    assert!(logged(
        &host.take(),
        Level::Debug,
        "mat search request dropped"
    ));
    assert_eq!(device.sent(), [Sent::Close]);
}

/// Each failed attempt alerts, until two minutes of unanswered attempts:
/// then one "not answering" alert, and quiet until a session succeeds.
#[test]
fn the_user_is_told_once_that_the_device_is_not_answering() {
    let mut host = TestHost::new();
    let mut driver = driver();
    let mut backoff = Backoff::new(Duration::from_millis(50), Duration::from_secs(5));
    let timeout = Error::Device(crate::Error::Timeout("the device's handshake reply"));
    let refused = Error::Lost;
    let mut attempt = |host: &mut TestHost, error: &Error, connected_for| {
        host.with(|now, out| {
            driver.retry_delay_at(
                &SessionEnd {
                    error: Some(error),
                    connected_for,
                    attempts: 1,
                },
                &mut backoff,
                out,
                now,
            )
        });
        alert_messages(&host.take())
    };
    assert_eq!(
        attempt(&mut host, &timeout, None),
        ["ExPutt error: timed out waiting for the device's handshake reply"]
    );
    host.advance(Duration::from_secs(60));
    assert_eq!(
        attempt(&mut host, &refused, None),
        ["ExPutt error: connection to the device lost"]
    );
    host.advance(Duration::from_secs(60));
    assert_eq!(attempt(&mut host, &timeout, None), [NOT_ANSWERING]);
    // Quiet while the outage lasts, attempts that fail another way included.
    for error in [&timeout, &refused, &timeout] {
        host.advance(Duration::from_secs(10));
        assert_eq!(attempt(&mut host, error, None), Vec::<String>::new());
    }
    // A session that measured ends the outage; the next one is told again.
    assert_eq!(
        attempt(&mut host, &refused, Some(Duration::from_secs(5))),
        ["ExPutt error: connection to the device lost"]
    );
    attempt(&mut host, &timeout, None);
    host.advance(NOT_ANSWERING_AFTER);
    assert_eq!(attempt(&mut host, &timeout, None), [NOT_ANSWERING]);
}

#[test]
fn putts_and_messages_are_raw_frames_while_wanted() {
    let mut host = TestHost::new();
    host.flags.set_wants_raw(true);
    let mut driver = driver();
    let (mut s, device) = open(&mut host, &mut driver);
    device.tap(Direction::Tx, Request::Alive.to_json().as_bytes());
    device.send(HEARTBEAT);
    device.tap(Direction::Rx, &device_event(CAMERA_FRAME));
    settle(&mut host, &mut s);
    device.send(PUTT_RESULT_FULL);
    settle(&mut host, &mut s);
    let seen = host.take();
    assert_eq!(raw_labels(&seen), ["N_ALIVE", "Test", "PuttResult"]);
    assert!(
        logged(&seen, Level::Trace, "rx camera frame 7b"),
        "camera frames reach only the trace log"
    );

    // The putt's own frame: as received, with the shot's key, before the
    // shot events built from it.
    let all = bodies(&seen);
    let frame_at = all
        .iter()
        .position(|b| matches!(b, FrpBody::ShotRaw(f) if f.label.as_deref() == Some("PuttResult")))
        .expect("frame");
    let FrpBody::ShotRaw(frame) = all[frame_at] else {
        unreachable!()
    };
    let FrpBody::ShotTrigger(trigger) = all[frame_at + 1] else {
        panic!("{all:#?}");
    };
    assert_eq!(frame.key.as_ref(), Some(&trigger.key));
    assert_eq!(frame.dir, RawDir::Rx);
    assert_eq!(frame.seq, 0, "the host numbers frames");
    assert!(frame.timestamp.is_some());
    assert_eq!(frame.data, device_event(PUTT_RESULT_FULL));
}

#[test]
fn raw_frames_start_and_stop_with_the_hosts_wish() {
    let mut host = TestHost::new();
    host.flags.set_min_level(Level::Info);
    let mut driver = driver();
    let (mut s, device) = open(&mut host, &mut driver);
    device.send(HEARTBEAT);
    settle(&mut host, &mut s);
    assert_eq!(raw_labels(&host.take()), Vec::<String>::new());

    // A controller asks for raw frames: the messages from the next step on.
    host.flags.set_wants_raw(true);
    settle(&mut host, &mut s);
    device.send(PUTT_RESULT_FULL);
    settle(&mut host, &mut s);
    assert_eq!(raw_labels(&host.take()), ["PuttResult"]);

    // The last one leaves: frames stop, the session goes on.
    host.flags.set_wants_raw(false);
    settle(&mut host, &mut s);
    device.send(PUTT_RESULT_FULL);
    settle(&mut host, &mut s);
    let seen = host.take();
    assert_eq!(raw_labels(&seen), Vec::<String>::new());
    assert_eq!(shot(&seen).len(), 4);
}

#[test]
fn trace_alone_logs_every_message_as_hex() {
    // Trace on (the default level), no raw frames wanted.
    let mut host = TestHost::new();
    let mut driver = driver();
    let (mut s, device) = open(&mut host, &mut driver);
    device.tap(Direction::Tx, Request::Alive.to_json().as_bytes());
    device.send(HEARTBEAT);
    settle(&mut host, &mut s);
    device.send(PUTT_RESULT_FULL);
    settle(&mut host, &mut s);
    let seen = host.take();
    assert_eq!(raw_labels(&seen), Vec::<String>::new());
    for prefix in ["tx N_ALIVE 7b", "rx Test 7b", "rx PuttResult 7b"] {
        assert!(logged(&seen, Level::Trace, prefix), "{prefix}: {seen:#?}");
    }
}

#[test]
fn no_raw_frames_or_audit_lines_unless_wanted() {
    let mut host = TestHost::new();
    host.flags.set_min_level(Level::Info);
    let mut driver = driver();
    let (mut s, device) = open(&mut host, &mut driver);
    device.tap(Direction::Tx, Request::Alive.to_json().as_bytes());
    device.send(HEARTBEAT);
    settle(&mut host, &mut s);
    device.send(PUTT_RESULT_FULL);
    settle(&mut host, &mut s);
    let seen = host.take();
    assert_eq!(shot(&seen).len(), 4);
    assert!(!seen.iter().any(|e| matches!(
        e,
        DeviceEvent::Frp(FrpBody::ShotRaw(_)) | DeviceEvent::Log(Level::Trace, _)
    )));
}
