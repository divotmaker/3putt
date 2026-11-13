//! The driver against `testing::MockDevice` (real localhost TCP), through a
//! small host loop like the one every `DeviceDriver` host runs.

use std::net::{TcpListener, UdpSocket};
use std::sync::atomic::AtomicU16;
use std::thread::JoinHandle;

use flightrelay::driver::{ConnectHost, DeviceEvent, HostFlags, Stop};
use flightrelay::types::blockers;
use flightrelay::{Club, ShotClub, ShotFinished, ShotLaunch, ShotTrigger};

use super::*;
use crate::testing::{
    BALL_STRUCK, MockConfig, MockDevice, PUTT_READY, PUTT_RESULT_BALL_ONLY, PUTT_RESULT_FULL,
    PUTT_RESULT_PULLED, ReceivedPacket,
};

/// Longest any test waits for one thing.
const WAIT: Duration = Duration::from_secs(8);

/// The silence limits scaled down, one second standing for 90.
const TEST_SILENCE: SilenceLimits = SilenceLimits {
    baseline: Duration::from_millis(1000),
    after_strike: Duration::from_millis(2000),
    warn_after: Duration::from_millis(333),
};

/// A UDP port nothing listens on.
fn free_udp_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .and_then(|s| s.local_addr())
        .expect("udp port")
        .port()
}

/// A TCP port nothing listens on: connects are refused.
fn closed_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("tcp port")
        .port()
}

/// Test timings: each connection attempt goes to the port held in `port`
/// at that moment.
fn tuning(port: &Arc<AtomicU16>) -> Tuning {
    Tuning {
        port: Arc::clone(port),
        discovery_port: free_udp_port(),
        connect_timeout: Duration::from_secs(1),
        reply_timeout: Duration::from_secs(2),
        mat_timeout: Duration::from_secs(2),
        keepalive_interval: Duration::from_millis(100),
        discovery_hint_after: Duration::from_secs(3600),
        silence_baseline: TEST_SILENCE.baseline,
        silence_after_strike: TEST_SILENCE.after_strike,
        silence_warn_after: TEST_SILENCE.warn_after,
        stall_wait: Duration::ZERO,
        resync_gap: Duration::ZERO,
        not_answering_after: Duration::from_secs(3600),
    }
}

fn port_of(mock: &MockDevice) -> Arc<AtomicU16> {
    Arc::new(AtomicU16::new(mock.port()))
}

fn localhost() -> Config {
    Config {
        address: Some("127.0.0.1".into()),
    }
}

/// What the host saw, in order.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)] // a test's record, kept by value
enum Seen {
    /// A status: the device's state after it, with the
    /// blockers as `ready` (`"true"` for none, `"false"` for `not_armed`).
    State(HashMap<String, String>),
    /// Any other event the driver emitted.
    Event(DeviceEvent),
    /// `connect` returned a session.
    Connected(DeviceIdentity),
    /// A session or a connect ended, and the driver advised `delay`.
    Ended {
        error: Option<String>,
        delay: Option<Duration>,
    },
}

enum Input {
    /// The hints of a new `golfer.turn`: they replace the earlier ones.
    Turn(TurnHints),
    Action(Action),
}

/// The host side of a connect: each turn replaces the hints; actions wait
/// for the session.
struct Ticks<'a> {
    inputs: &'a Receiver<Input>,
    hints: &'a mut TurnHints,
    actions: &'a mut Vec<Action>,
}

impl ConnectHost for Ticks<'_> {
    fn tick(&mut self) {
        while let Ok(input) = self.inputs.try_recv() {
            match input {
                Input::Turn(hints) => *self.hints = hints,
                Input::Action(action) => self.actions.push(action),
            }
        }
    }

    fn hints(&self) -> &TurnHints {
        self.hints
    }
}

/// The driver running on a host thread.
struct Run {
    stop: Stop,
    /// The host's flags, live: a test can change them while the driver runs.
    flags: Arc<HostFlags>,
    inputs: Sender<Input>,
    seen: Receiver<Seen>,
    /// The device's status as last seen (cleared at each connect).
    state: HashMap<String, String>,
    thread: Option<JoinHandle<()>>,
}

impl Run {
    fn start(driver: ExPutt) -> Self {
        Self::start_with(driver, TurnHints::default(), false)
    }

    fn start_with(driver: ExPutt, hints: TurnHints, raw: bool) -> Self {
        let flags = HostFlags::new();
        flags.set_wants_raw(raw);
        Self::start_flags(driver, hints, flags)
    }

    fn start_flags(mut driver: ExPutt, hints: TurnHints, flags: HostFlags) -> Self {
        let stop = Stop::new();
        let flags = Arc::new(flags);
        let (inputs, input_rx) = mpsc::channel();
        let (seen_tx, seen) = mpsc::channel();
        let thread = {
            let (stop, flags) = (stop.clone(), Arc::clone(&flags));
            std::thread::spawn(move || {
                host(&mut driver, &stop, &flags, &input_rx, &seen_tx, hints);
            })
        };
        Self {
            stop,
            flags,
            inputs,
            seen,
            state: HashMap::new(),
            thread: Some(thread),
        }
    }

    /// A new turn with these hints.
    fn turn(&self, hints: TurnHints) {
        let _ = self.inputs.send(Input::Turn(hints));
    }

    /// A new turn whose only hint is the handedness.
    fn hand(&self, handed: Handedness) {
        self.turn(TurnHints::default().with_handedness(handed));
    }

    fn find_mat(&self) {
        let _ = self.inputs.send(Input::Action(Action::FindMat));
    }

    /// The next thing seen within `left`, with status as a state map.
    fn next(&mut self, left: Duration) -> Option<Seen> {
        let seen = self.seen.recv_timeout(left).ok()?;
        Some(match seen {
            Seen::Event(DeviceEvent::Status(status)) => {
                // A status is the whole of it: it replaces what was seen.
                let ready = match status.blockers.as_slice() {
                    [] => "true",
                    [b] if b == blockers::NOT_ARMED => "false",
                    other => panic!("unexpected blockers {other:?}"),
                };
                self.state.clear();
                self.state.insert("ready".to_owned(), ready.to_owned());
                self.state.extend(status.telemetry);
                Seen::State(self.state.clone())
            }
            Seen::Connected(id) => {
                self.state.clear();
                Seen::Connected(id)
            }
            other => other,
        })
    }

    /// Everything seen until `want` matches, the match last.
    fn until(&mut self, what: &str, want: impl Fn(&Seen) -> bool) -> Vec<Seen> {
        let deadline = Instant::now() + WAIT;
        let mut seen = Vec::new();
        while let Some(left) = deadline.checked_duration_since(Instant::now()) {
            let Some(s) = self.next(left) else { break };
            let done = want(&s);
            seen.push(s);
            if done {
                return seen;
            }
        }
        panic!("{what} not seen; got {seen:#?}");
    }

    /// Everything waiting, without waiting.
    fn drain(&mut self) -> Vec<Seen> {
        let mut seen = Vec::new();
        while let Some(s) = self.next(Duration::ZERO) {
            seen.push(s);
        }
        seen
    }

    fn until_connected(&mut self) -> Vec<Seen> {
        self.until("connected", |s| matches!(s, Seen::Connected(_)))
    }

    fn until_ended(&mut self) -> Vec<Seen> {
        self.until("ended", |s| matches!(s, Seen::Ended { .. }))
    }

    /// Until the merged state has `ready` = `ready` and, when given,
    /// `device_state` = `state`.
    fn until_ready(&mut self, ready: bool, state: Option<&str>) -> Vec<Seen> {
        let ready = ready.to_string();
        self.until(&format!("ready={ready} {state:?}"), |s| {
            matches!(s, Seen::State(t)
                if t.get("ready") == Some(&ready)
                    && state.is_none_or(|s| t.get("device_state").map(String::as_str) == Some(s)))
        })
    }

    /// The events of the next shot, trigger to finish.
    fn next_shot(&mut self) -> Vec<FrpBody> {
        let events: Vec<FrpBody> = self
            .until("shot.finished", |s| {
                matches!(s, Seen::Event(DeviceEvent::Frp(FrpBody::ShotFinished(_))))
            })
            .into_iter()
            .filter_map(|s| match s {
                Seen::Event(DeviceEvent::Frp(e)) => Some(e),
                _ => None,
            })
            .collect();
        let start = events
            .iter()
            .rposition(|e| matches!(e, FrpBody::ShotTrigger(_)))
            .expect("shot.trigger");
        events[start..].to_vec()
    }

    fn stop(&mut self) {
        self.stop.stop();
        if let Some(t) = self.thread.take() {
            t.join().expect("host thread");
        }
    }
}

impl Drop for Run {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The host loop: connect, poll with the inputs passed on, close, ask the
/// driver for the delay, wait, again; until the stop.
fn host(
    driver: &mut ExPutt,
    stop: &Stop,
    flags: &HostFlags,
    inputs: &Receiver<Input>,
    seen: &Sender<Seen>,
    mut hints: TurnHints,
) {
    let tx = seen.clone();
    let mut sink = move |e: DeviceEvent| {
        let _ = tx.send(Seen::Event(e));
    };
    let mut backoff = Backoff::new(Duration::from_millis(50), Duration::from_millis(100));
    let mut attempts = 0;
    let mut actions = Vec::new();
    while !stop.is_stopped() {
        let connected = {
            let mut ticks = Ticks {
                inputs,
                hints: &mut hints,
                actions: &mut actions,
            };
            let mut ctx = ConnectCtx::new(stop, Events::new(&mut sink, flags), &mut ticks);
            driver.connect(&mut ctx)
        };
        let (error, connected_for) = match connected {
            Ok(mut session) => {
                attempts = 0;
                let started = Instant::now();
                let _ = seen.send(Seen::Connected(session.identity().clone()));
                let mut failed = None;
                for action in actions.drain(..) {
                    if let Err(e) = session.action(&action) {
                        failed = Some(e);
                    }
                }
                while failed.is_none() && !stop.is_stopped() {
                    while let Ok(input) = inputs.try_recv() {
                        let result = match input {
                            // The session gets the hints whenever they change.
                            Input::Turn(new) if new != hints => {
                                hints = new;
                                session.hints(&hints)
                            }
                            Input::Turn(_) => Ok(()),
                            Input::Action(action) => session.action(&action),
                        };
                        if let Err(e) = result {
                            failed = Some(e);
                            break;
                        }
                    }
                    if failed.is_none() {
                        failed = session
                            .poll(
                                Duration::from_millis(20),
                                &mut Events::new(&mut sink, flags),
                            )
                            .err();
                    }
                }
                session
                    .close(&mut Events::new(&mut sink, flags))
                    .expect("close");
                (failed, Some(started.elapsed()))
            }
            Err(e) => {
                attempts += 1;
                (Some(e), None)
            }
        };
        if stop.is_stopped() {
            break;
        }
        let delay = driver.retry_delay(
            &SessionEnd {
                error: error.as_ref(),
                connected_for,
                attempts,
            },
            &mut backoff,
            &mut Events::new(&mut sink, flags),
        );
        let _ = seen.send(Seen::Ended {
            error: error.as_ref().map(ToString::to_string),
            delay,
        });
        let Some(delay) = delay else { break };
        let until = Instant::now() + delay;
        while Instant::now() < until && !stop.is_stopped() {
            while let Ok(input) = inputs.try_recv() {
                if let Input::Turn(new) = input {
                    hints = new;
                }
            }
            let _ = stop.sleep(
                until
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(10)),
            );
        }
    }
}

fn alerts(seen: &[Seen]) -> Vec<String> {
    seen.iter()
        .filter_map(|s| match s {
            Seen::Event(DeviceEvent::Frp(FrpBody::BusAlert(a))) => Some(a.message.clone()),
            _ => None,
        })
        .collect()
}

fn phases(seen: &[Seen]) -> Vec<&'static str> {
    seen.iter()
        .filter_map(|s| match s {
            Seen::Event(DeviceEvent::Phase(p)) => Some(*p),
            _ => None,
        })
        .collect()
}

/// The `device_state` values in `seen`, in order, repeats folded.
fn device_states(seen: &[Seen]) -> Vec<String> {
    let mut states: Vec<String> = Vec::new();
    for s in seen {
        if let Seen::State(t) = s
            && let Some(state) = t.get("device_state")
            && states.last() != Some(state)
        {
            states.push(state.clone());
        }
    }
    states
}

fn reconnected(seen: &[Seen]) -> bool {
    seen.iter().any(|s| matches!(s, Seen::Ended { .. }))
}

fn sent(mock: &MockDevice, request: Request) -> bool {
    let json = request.to_json();
    mock.wait_until(WAIT, |packets| packets.iter().any(|p| p.json == json))
}

/// Whether `packets`, keepalives aside, are exactly a new round with
/// `hand`: match end, mat resync, hand, match begin.
fn round_sent(packets: &[ReceivedPacket], hand: Hand) -> bool {
    let want: Vec<String> = [
        Request::MatchEnded,
        Request::StartCamera,
        Request::ResyncToMat,
        Request::StopCamera,
        Request::SetHand(hand),
        Request::MatchBegin,
    ]
    .iter()
    .map(|r| r.to_json())
    .collect();
    let alive = Request::Alive.to_json();
    let sent: Vec<&String> = packets
        .iter()
        .map(|p| &p.json)
        .filter(|j| **j != alive)
        .collect();
    sent == want.iter().collect::<Vec<_>>()
}

fn only_keepalives(packets: &[ReceivedPacket]) -> bool {
    packets.iter().all(|p| p.json == Request::Alive.to_json())
}

fn quiet_mock() -> MockDevice {
    MockDevice::spawn(MockConfig {
        heartbeat: None,
        ..MockConfig::default()
    })
    .expect("mock")
}

fn driver_for(mock: &MockDevice) -> (ExPutt, Arc<AtomicU16>) {
    let port = port_of(mock);
    let driver = ExPutt::with_tuning(localhost(), tuning(&port)).expect("driver");
    (driver, port)
}

#[test]
fn connects_measures_and_closes_the_session_on_stop() {
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let (driver, _) = driver_for(&mock);
    let mut run = Run::start(driver);

    let seen = run.until_connected();
    // A configured address skips discovery.
    assert_eq!(phases(&seen), ["connecting", "measuring"]);
    let Some(Seen::Connected(identity)) = seen.last() else {
        unreachable!()
    };
    assert_eq!(identity.device, "ExPutt RG (127.0.0.1)");
    assert_eq!(identity.label(), "ExPutt RG (127.0.0.1)");
    assert_eq!(identity.model.as_deref(), Some("ExPutt RG"));
    assert_eq!(identity.manufacturer, None);
    assert_eq!(identity.firmware, None);

    // Not ready at first, then ready once the device says so.
    let seen = run.until_ready(true, Some("ready"));
    let first = seen
        .iter()
        .find_map(|s| match s {
            Seen::State(t) => Some(t.clone()),
            _ => None,
        })
        .expect("telemetry");
    assert_eq!(first.get("ready").map(String::as_str), Some("false"));
    assert!(sent(&mock, Request::SetHand(Hand::Right)));
    assert!(sent(&mock, Request::MatchBegin));

    run.stop();
    assert!(sent(&mock, Request::MatchEnded), "measuring ended");
    assert!(sent(&mock, Request::AppPause), "goodbye");
    assert!(mock.wait_closed(WAIT), "connection closed");
    let rest = run.drain();
    assert!(
        rest.iter()
            .any(|s| matches!(s, Seen::State(t) if t["ready"] == "false")),
        "not ready after close: {rest:?}"
    );
}

#[test]
fn a_putt_becomes_a_putter_shot_with_speed_direction_path_and_face() {
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let (driver, _) = driver_for(&mock);
    let mut run = Run::start(driver);
    run.until_connected();

    mock.push_event(PUTT_RESULT_FULL);
    let events = run.next_shot();
    let expected = putt(PUTT_RESULT_FULL);
    let [
        FrpBody::ShotTrigger(trigger),
        FrpBody::ShotLaunch(ShotLaunch { key: k1, launch }),
        FrpBody::ShotClub(ShotClub { key: k2, club }),
        FrpBody::ShotFinished(ShotFinished { key: k3 }),
    ] = &events[..]
    else {
        panic!("{events:#?}");
    };
    let key = &trigger.key;
    assert_eq!(key.shot_number, 1);
    assert!([k1, k2, k3].iter().all(|k| *k == key));
    assert_eq!(trigger.timestamp, None, "no strike seen, no strike time");
    assert_eq!(*launch, launch_of(&expected));
    assert!((launch.launch_speed.unwrap().as_mps() - 2.50).abs() < 0.01);
    assert!(launch.launch_azimuth.unwrap() > 1.4, "R1.4 is positive");
    assert_eq!(launch.launch_elevation, None);
    assert_eq!(club.path, expected.path_deg);
    assert_eq!(club.face_angle, expected.face_deg);
    assert!(club.path.unwrap() > 0.0 && club.face_angle.unwrap() > 0.0);
}

#[test]
fn a_pulled_putt_is_negative_left() {
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let (driver, _) = driver_for(&mock);
    let mut run = Run::start(driver);
    run.until_connected();
    mock.push_event(PUTT_RESULT_PULLED);
    let events = run.next_shot();
    let launch = events
        .iter()
        .find_map(|e| match e {
            FrpBody::ShotLaunch(m) => Some(m.launch.clone()),
            _ => None,
        })
        .expect("launch");
    assert!(launch.launch_azimuth.unwrap() < -10.0, "L10.3 is negative");
    let club = events
        .iter()
        .find_map(|e| match e {
            FrpBody::ShotClub(m) => Some(m.club.clone()),
            _ => None,
        })
        .expect("club");
    assert!(club.path.unwrap() < -5.0 && club.face_angle.unwrap() < -10.0);
}

#[test]
fn a_putt_without_putter_data_is_a_ball_only_shot() {
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let (driver, _) = driver_for(&mock);
    let mut run = Run::start(driver);
    run.until_connected();
    mock.push_event(PUTT_RESULT_BALL_ONLY);
    let events = run.next_shot();
    assert!(
        !events.iter().any(|e| matches!(e, FrpBody::ShotClub(_))),
        "{events:?}"
    );
    assert!(events.iter().any(|e| matches!(e, FrpBody::ShotLaunch(m)
        if m.launch.launch_azimuth == Some(putt(PUTT_RESULT_BALL_ONLY).launch_direction_deg))));
}

#[test]
fn a_strike_dates_the_shot() {
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let (driver, _) = driver_for(&mock);
    let mut run = Run::start(driver);
    run.until_ready(true, Some("ready"));
    let before = now_ms().expect("clock");
    mock.push_event(BALL_STRUCK);
    run.until_ready(false, Some("ball struck"));
    let after = now_ms().expect("clock");
    mock.push_event(PUTT_RESULT_FULL);
    let events = run.next_shot();
    let FrpBody::ShotTrigger(ShotTrigger {
        timestamp: Some(at),
        ..
    }) = &events[0]
    else {
        panic!("{events:?}");
    };
    assert!((before..=after).contains(at), "{before} <= {at} <= {after}");

    // A strike the device takes back (ready again) dates nothing.
    mock.push_event(PUTT_READY);
    run.until_ready(true, Some("ready"));
    mock.push_event(BALL_STRUCK);
    run.until_ready(false, Some("ball struck"));
    mock.push_event(PUTT_READY);
    run.until_ready(true, Some("ready"));
    mock.push_event(PUTT_RESULT_FULL);
    let events = run.next_shot();
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

fn mock_putt(mock: &MockDevice, run: &mut Run, number: u32) {
    mock.push_event(PUTT_RESULT_FULL);
    let events = run.next_shot();
    let FrpBody::ShotTrigger(trigger) = &events[0] else {
        unreachable!()
    };
    assert_eq!(trigger.key.shot_number, number);
}

#[test]
fn a_dropped_device_is_reconnected_and_shot_numbers_go_on() {
    let first = MockDevice::spawn(MockConfig::default()).expect("mock");
    let (driver, port) = driver_for(&first);
    let mut run = Run::start(driver);
    run.until_connected();
    mock_putt(&first, &mut run, 1);

    // The device goes away; the next attempt finds it again (on a new
    // mock, so on a new port).
    let second = MockDevice::spawn(MockConfig::default()).expect("mock");
    port.store(second.port(), Ordering::SeqCst);
    first.disconnect();
    let seen = run.until_ended();
    // Not ready before the session ends.
    let not_ready = seen
        .iter()
        .position(|s| matches!(s, Seen::State(t) if t["ready"] == "false"));
    assert!(
        not_ready.is_some_and(|i| i < seen.len() - 1),
        "ready=false before the end: {seen:#?}"
    );
    let Some(Seen::Ended { error, delay }) = seen.last() else {
        unreachable!()
    };
    assert_eq!(error.as_deref(), Some("connection to the device lost"));
    assert_eq!(
        *delay,
        Some(Duration::from_millis(50)),
        "after a measuring session, quickly"
    );
    assert_eq!(
        alerts(&seen),
        ["ExPutt error: connection to the device lost"]
    );
    run.until_connected();
    assert!(sent(&second, Request::MatchBegin));
    mock_putt(&second, &mut run, 2);
}

#[test]
fn a_silent_device_is_reconnected() {
    // No heartbeat: the session goes quiet.
    let mock = quiet_mock();
    let second = MockDevice::spawn(MockConfig::default()).expect("mock");
    let (driver, port) = driver_for(&mock);
    let mut run = Run::start(driver);
    run.until_connected();
    port.store(second.port(), Ordering::SeqCst);
    let seen = run.until_ended();
    let Some(Seen::Ended { error, .. }) = seen.last() else {
        unreachable!()
    };
    assert!(
        error
            .as_deref()
            .is_some_and(|e| e.starts_with("no data from the device for")),
        "{error:?}"
    );
    run.until_connected();
    assert!(sent(&second, Request::MatchBegin));
}

/// A hand change starts a new round, which finds the mat first: not ready
/// through "finding mat" and "mat found" until the device is ready again,
/// as after a Find mat, and no reconnect.
#[test]
fn the_hand_follows_the_player() {
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let (driver, _) = driver_for(&mock);
    let mut run = Run::start_with(
        driver,
        TurnHints::default().with_handedness(Handedness::Left),
        false,
    );
    run.until_ready(true, Some("ready"));
    assert!(sent(&mock, Request::SetHand(Hand::Left)), "starts left");
    let before = mock.received().len();

    // The player is right-handed now. The mode and club of a turn do not
    // apply.
    run.turn(
        TurnHints::default()
            .with_mode(Mode::Full)
            .with_club(Club::Driver)
            .with_handedness(Handedness::Right),
    );
    let mut seen = run.until_ready(false, Some("ready"));
    seen.extend(run.until_ready(true, Some("ready")));
    assert_eq!(
        device_states(&seen),
        ["ready", "finding mat", "mat found", "ready"]
    );
    assert!(!reconnected(&seen), "no reconnect");
    assert!(
        mock.wait_until(WAIT, |p| round_sent(&p[before..], Hand::Right)),
        "{:?}",
        mock.received()
    );

    // The same hand again, then the other one as a sentinel: the only
    // round since is the sentinel's.
    let after = mock.received().len();
    run.hand(Handedness::Right);
    run.hand(Handedness::Left);
    assert!(
        mock.wait_until(WAIT, |p| round_sent(&p[after..], Hand::Left)),
        "{:?}",
        mock.received()
    );
}

#[test]
fn a_hand_hinted_while_the_session_starts_is_a_restart_to_it() {
    // The device finds no mat until told to, so the session start waits.
    let mock = MockDevice::spawn(MockConfig {
        find_mat: false,
        ..MockConfig::default()
    })
    .expect("mock");
    let (driver, _) = driver_for(&mock);
    let mut run = Run::start(driver);
    // The session start has read the hand (right, no hint yet) and is
    // waiting for the mat when the player turns out to be left-handed.
    assert!(sent(&mock, Request::ResyncToMat));
    run.hand(Handedness::Left);
    mock.set_find_mat(true);
    mock.push_event("MatOK,102");

    let mut seen = run.until_connected();
    seen.extend(run.until_ready(true, Some("ready")));
    assert!(!reconnected(&seen), "no reconnect");
    // The start went out with the right hand, then one round to the left.
    let packets = mock.received();
    let started = packets
        .iter()
        .position(|p| p.json == Request::MatchBegin.to_json())
        .expect("match begin");
    assert!(
        packets[..started]
            .iter()
            .any(|p| p.json == Request::SetHand(Hand::Right).to_json()),
        "{packets:?}"
    );
    assert!(
        mock.wait_until(WAIT, |p| round_sent(&p[started + 1..], Hand::Left)),
        "{:?}",
        mock.received()
    );
}

/// A turn with no handedness keeps the device's hand, so a left-handed
/// session stays left-handed.
#[test]
fn a_mode_or_club_alone_changes_nothing() {
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let (driver, _) = driver_for(&mock);
    let mut run = Run::start_with(
        driver,
        TurnHints::default().with_handedness(Handedness::Left),
        false,
    );
    run.until_ready(true, Some("ready"));
    assert!(sent(&mock, Request::SetHand(Hand::Left)), "starts left");
    let before = mock.received().len();
    run.turn(TurnHints::default().with_mode(Mode::Chipping));
    run.turn(TurnHints::default().with_club(Club::PitchingWedge));
    run.turn(TurnHints::default());
    std::thread::sleep(Duration::from_millis(300));
    assert!(only_keepalives(&mock.received()[before..]));
    assert!(run.drain().iter().all(|s| !matches!(s, Seen::State(_))));
}

#[test]
fn a_device_that_is_not_there_is_retried() {
    let port = Arc::new(AtomicU16::new(closed_port()));
    let driver = ExPutt::with_tuning(localhost(), tuning(&port)).expect("driver");
    let mut run = Run::start(driver);
    let seen = run.until("alert", |s| {
        matches!(s, Seen::Event(DeviceEvent::Frp(FrpBody::BusAlert(_))))
    });
    assert!(!seen.iter().any(|s| matches!(s, Seen::Connected(_))));
    assert!(alerts(&seen)[0].starts_with("ExPutt error: "), "{seen:?}");
    // Comes up once the device does.
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    port.store(mock.port(), Ordering::SeqCst);
    run.until_connected();
}

#[test]
fn a_putt_result_after_a_quiet_spell_keeps_the_session() {
    // No heartbeat; the result comes half way to the silence limit.
    let mock = quiet_mock();
    let (driver, _) = driver_for(&mock);
    let mut run = Run::start(driver);
    let mut seen = run.until_connected();
    std::thread::sleep(TEST_SILENCE.baseline / 2);
    mock.push_event(PUTT_RESULT_FULL);
    run.next_shot();
    seen.extend(run.drain());
    assert!(!reconnected(&seen), "no reconnect");
    assert_eq!(alerts(&seen), Vec::<String>::new());
}

#[test]
fn a_slow_result_after_a_strike_keeps_the_session() {
    // No heartbeat; the result comes past the baseline limit but within the
    // limit after a strike.
    let mock = quiet_mock();
    let (driver, _) = driver_for(&mock);
    let mut run = Run::start(driver);
    let mut seen = run.until_ready(true, Some("ready"));
    mock.push_event(BALL_STRUCK);
    seen.extend(run.until_ready(false, Some("ball struck")));
    std::thread::sleep(TEST_SILENCE.after_strike * 2 / 3);
    mock.push_event(PUTT_RESULT_FULL);
    run.next_shot();
    seen.extend(run.drain());
    assert!(!reconnected(&seen), "no reconnect");
    assert_eq!(alerts(&seen), Vec::<String>::new());
}

/// A UDP sender of the device's beacon, pointing at localhost, to `port`.
struct Beaconing {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Beaconing {
    fn to(port: u16, after: Duration) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            std::thread::sleep(after);
            let tx = UdpSocket::bind("127.0.0.1:0").expect("bind");
            let beacon = r#"{"_serverUUID":"00000000-0000-4000-8000-000000000001","_ipAddress":"127.0.0.1","_serverUDPPort":9999,"_appVersion":"1.2.3"}"#;
            while !flag.load(Ordering::SeqCst) {
                let _ = tx.send_to(beacon.as_bytes(), ("127.0.0.1", port));
                std::thread::sleep(Duration::from_millis(50));
            }
        });
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Beaconing {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

#[test]
fn a_discovered_device_is_connected() {
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let port = port_of(&mock);
    let tuning = tuning(&port);
    let _beacon = Beaconing::to(tuning.discovery_port, Duration::from_millis(600));
    let driver = ExPutt::with_tuning(Config::default(), tuning).expect("driver");
    let mut run = Run::start(driver);
    let seen = run.until_connected();
    assert_eq!(phases(&seen), ["discovering", "connecting", "measuring"]);
    let Some(Seen::Connected(identity)) = seen.last() else {
        unreachable!()
    };
    assert_eq!(identity.firmware.as_deref(), Some("1.2.3"));
    assert_eq!(identity.device, "ExPutt RG (127.0.0.1)");
}

#[test]
fn discovery_without_a_device_hints_once() {
    let port = Arc::new(AtomicU16::new(closed_port()));
    let mut tuning = tuning(&port);
    tuning.discovery_hint_after = Duration::from_millis(300);
    let driver = ExPutt::with_tuning(Config::default(), tuning).expect("driver");
    let mut run = Run::start(driver);
    let mut seen = run.until("alert", |s| {
        matches!(s, Seen::Event(DeviceEvent::Frp(FrpBody::BusAlert(_))))
    });
    let severity = seen.iter().find_map(|s| match s {
        Seen::Event(DeviceEvent::Frp(FrpBody::BusAlert(a))) => Some(a.severity),
        _ => None,
    });
    assert_eq!(severity, Some(Severity::Warn));
    // Many more slices, no second hint.
    std::thread::sleep(Duration::from_millis(800));
    seen.extend(run.drain());
    assert_eq!(alerts(&seen), [DISCOVERY_HINT]);
    assert!(!seen.iter().any(|s| matches!(s, Seen::Connected(_))));
}

#[test]
fn stopping_during_discovery_ends_at_once() {
    let port = Arc::new(AtomicU16::new(closed_port()));
    let driver = ExPutt::with_tuning(Config::default(), tuning(&port)).expect("driver");
    let mut run = Run::start(driver);
    run.until("discovering", |s| {
        matches!(s, Seen::Event(DeviceEvent::Phase("discovering")))
    });
    let stopped = Instant::now();
    run.stop();
    assert!(
        stopped.elapsed() < Duration::from_secs(1),
        "{:?}",
        stopped.elapsed()
    );
}

#[test]
fn readiness_follows_the_putt_cycle() {
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let (driver, _) = driver_for(&mock);
    let mut run = Run::start(driver);
    run.until_ready(true, Some("ready"));

    // Struck: not ready until the device says so again.
    mock.push_event(BALL_STRUCK);
    run.until_ready(false, Some("ball struck"));

    // The result changes neither value.
    mock.push_event(PUTT_RESULT_FULL);
    let shot = run.until("shot_finished", |s| {
        matches!(s, Seen::Event(DeviceEvent::Frp(FrpBody::ShotFinished(_))))
    });
    assert!(
        !shot.iter().any(|s| matches!(s, Seen::State(_))),
        "no telemetry for the result"
    );

    mock.push_event(PUTT_READY);
    run.until_ready(true, Some("ready"));
}

#[test]
fn find_mat_resyncs_the_device_and_is_ready_again() {
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let (driver, _) = driver_for(&mock);
    let mut run = Run::start(driver);
    run.until_ready(true, Some("ready"));
    let before = mock.received().len();

    run.find_mat();
    let mut seen = run.until_ready(false, Some("ready"));
    seen.extend(run.until_ready(true, Some("ready")));
    assert_eq!(
        device_states(&seen),
        ["ready", "finding mat", "mat found", "ready"]
    );
    assert!(!reconnected(&seen), "no reconnect");
    assert!(
        mock.wait_until(WAIT, |p| round_sent(&p[before..], Hand::Right)),
        "{:?}",
        mock.received()
    );
}

/// A device whose app has stalled: it accepts every connection and greets,
/// and never answers anything.
struct Wedged {
    port: u16,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Wedged {
    fn spawn() -> Self {
        use std::io::Write as _;
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let port = listener.local_addr().expect("addr").port();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            let mut held = Vec::new();
            while !flag.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ =
                            stream.write_all(&crate::zmtp::greeting(crate::zmtp::SOCKET_ROUTER));
                        held.push(stream);
                    }
                    Err(_) => std::thread::sleep(Duration::from_millis(5)),
                }
            }
        });
        Self {
            port,
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Wedged {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// Alerts and their arrival times, until `want` matches one.
fn alert_times(run: &mut Run, what: &str, want: impl Fn(&str) -> bool) -> Vec<(String, Instant)> {
    let seen = std::cell::RefCell::new(Vec::new());
    run.until(what, |s| {
        let Seen::Event(DeviceEvent::Frp(FrpBody::BusAlert(alert))) = s else {
            return false;
        };
        let message = &alert.message;
        seen.borrow_mut().push((message.clone(), Instant::now()));
        want(message)
    });
    seen.into_inner()
}

#[test]
fn a_silent_device_gets_time_before_the_reconnect() {
    let stall_wait = Duration::from_millis(600);
    let mock = quiet_mock();
    let second = MockDevice::spawn(MockConfig::default()).expect("mock");
    let port = port_of(&mock);
    let mut tuning = tuning(&port);
    tuning.stall_wait = stall_wait;
    let driver = ExPutt::with_tuning(localhost(), tuning).expect("driver");
    let mut run = Run::start(driver);
    run.until_connected();
    port.store(second.port(), Ordering::SeqCst);

    let alerts = alert_times(&mut run, "silence", |m| {
        m.contains("no data from the device")
    });
    let lost = alerts.last().expect("alert").1;
    run.until_connected();
    let waited = lost.elapsed();
    assert!(
        waited + Duration::from_millis(30) >= stall_wait,
        "reconnected after {waited:?}"
    );
    assert!(sent(&second, Request::MatchBegin));
}

#[test]
fn hand_changes_inside_the_gap_are_one_restart_to_the_final_hand() {
    let gap = Duration::from_millis(900);
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let port = port_of(&mock);
    let mut tuning = tuning(&port);
    tuning.resync_gap = gap;
    let driver = ExPutt::with_tuning(localhost(), tuning).expect("driver");
    let mut run = Run::start(driver);
    run.until_ready(true, Some("ready"));
    let before = mock.received().len();
    // Within the gap after the session's own search: right (in effect),
    // left, right, left, left.
    for handed in [
        Handedness::Right,
        Handedness::Left,
        Handedness::Right,
        Handedness::Left,
        Handedness::Left,
    ] {
        run.hand(handed);
    }
    // Held: not ready, nothing sent yet.
    run.until_ready(false, None);
    std::thread::sleep(gap / 3);
    assert!(
        only_keepalives(&mock.received()[before..]),
        "{:?}",
        mock.received()
    );

    // One restart, to the final hand, once the gap is over.
    let mut seen = run.until_ready(true, Some("ready"));
    assert!(
        mock.wait_until(WAIT, |p| round_sent(&p[before..], Hand::Left)),
        "{:?}",
        mock.received()
    );
    seen.extend(run.drain());
    assert!(!reconnected(&seen));
}

#[test]
fn a_hand_flipped_back_inside_the_gap_sends_nothing() {
    let gap = Duration::from_millis(1000);
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let port = port_of(&mock);
    let mut tuning = tuning(&port);
    tuning.resync_gap = gap;
    let driver = ExPutt::with_tuning(localhost(), tuning).expect("driver");
    let mut run = Run::start(driver);
    run.until_ready(true, Some("ready"));
    let before = mock.received().len();
    // Held for the gap: not ready meanwhile.
    run.hand(Handedness::Left);
    run.until_ready(false, None);
    // Back to the hand in effect: nothing to do, ready as the device last
    // said.
    run.hand(Handedness::Right);
    run.until_ready(true, None);
    std::thread::sleep(gap * 3 / 2);
    assert!(
        only_keepalives(&mock.received()[before..]),
        "{:?}",
        mock.received()
    );
}

#[test]
fn find_mat_inside_the_gap_waits_for_it() {
    let gap = Duration::from_millis(1000);
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let port = port_of(&mock);
    let mut tuning = tuning(&port);
    tuning.resync_gap = gap;
    let driver = ExPutt::with_tuning(localhost(), tuning).expect("driver");
    let mut run = Run::start(driver);
    run.until_ready(true, Some("ready"));
    let before = mock.received().len();
    for _ in 0..3 {
        run.find_mat();
    }
    // Not ready, nothing sent until the gap is over; then one search.
    run.until_ready(false, None);
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        only_keepalives(&mock.received()[before..]),
        "{:?}",
        mock.received()
    );
    run.until_ready(true, Some("ready"));
    std::thread::sleep(gap / 2);
    assert!(
        mock.wait_until(WAIT, |p| round_sent(&p[before..], Hand::Right)),
        "{:?}",
        mock.received()
    );
}

/// The alerts of one outage, up to and including the "not answering" one:
/// the failed attempts are alerted first, the user is told once the outage
/// has lasted `after`, and attempts after that are not alerted.
fn assert_outage_alerts(seen: &[(String, Instant)], after: Duration) {
    let errors: Vec<&Instant> = seen
        .iter()
        .filter(|(m, _)| m.starts_with("ExPutt error") && !m.contains("lost"))
        .map(|(_, at)| at)
        .collect();
    assert!(errors.len() >= 2, "several failed attempts first: {seen:?}");
    let told = seen.last().expect("alert").1;
    assert!(
        told.duration_since(*errors[0]) + Duration::from_millis(30) >= after,
        "told after {:?}: {seen:?}",
        told.duration_since(*errors[0])
    );
}

#[test]
fn a_device_that_never_answers_is_reported_once_per_outage() {
    let after = Duration::from_millis(600);
    let wedged = Wedged::spawn();
    let good = MockDevice::spawn(MockConfig::default()).expect("mock");
    let port = Arc::new(AtomicU16::new(wedged.port));
    let mut tuning = tuning(&port);
    tuning.not_answering_after = after;
    tuning.reply_timeout = Duration::from_millis(200);
    let driver = ExPutt::with_tuning(localhost(), tuning).expect("driver");
    let mut run = Run::start(driver);

    let seen = alert_times(&mut run, "not answering", |m| m == NOT_ANSWERING);
    assert_outage_alerts(&seen, after);

    // Attempts go on, silently: neither the per-attempt alert nor the "not
    // answering" one again while the outage lasts.
    std::thread::sleep(Duration::from_millis(1500));
    let quiet = run.drain();
    assert!(
        quiet
            .iter()
            .filter(|s| matches!(s, Seen::Ended { .. }))
            .count()
            >= 2,
        "attempts go on: {quiet:?}"
    );
    assert_eq!(alerts(&quiet), Vec::<String>::new());

    // A session that succeeds ends the outage; the next one is alerted per
    // attempt again and told once more.
    port.store(good.port(), Ordering::SeqCst);
    run.until_connected();
    port.store(wedged.port, Ordering::SeqCst);
    good.disconnect();
    let again = alert_times(&mut run, "not answering again", |m| m == NOT_ANSWERING);
    assert_outage_alerts(&again, after);
}

#[test]
fn an_outage_that_flaps_between_refused_and_unanswered_is_still_reported() {
    let after = Duration::from_millis(600);
    let wedged = Wedged::spawn();
    let closed = closed_port();
    let port = Arc::new(AtomicU16::new(wedged.port));
    let flapping = Arc::new(AtomicBool::new(true));
    let flapper = {
        let (port, flapping, wedged) = (Arc::clone(&port), Arc::clone(&flapping), wedged.port);
        std::thread::spawn(move || {
            let mut ports = [wedged, closed].into_iter().cycle();
            while flapping.load(Ordering::SeqCst) {
                port.store(ports.next().expect("cycle"), Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(120));
            }
        })
    };
    let mut tuning = tuning(&port);
    tuning.not_answering_after = after;
    tuning.reply_timeout = Duration::from_millis(200);
    let driver = ExPutt::with_tuning(localhost(), tuning).expect("driver");
    let mut run = Run::start(driver);

    let seen = alert_times(&mut run, "not answering", |m| m == NOT_ANSWERING);
    flapping.store(false, Ordering::SeqCst);
    flapper.join().expect("flapper");
    assert!(
        seen.iter().any(|(m, _)| m.contains("refused")),
        "some attempts were refused: {seen:?}"
    );
}

#[test]
fn a_silent_device_is_not_ready_until_it_sends_again() {
    // No heartbeat: ready from the device's own report, then silence past
    // the warning threshold.
    let mock = quiet_mock();
    let (driver, _) = driver_for(&mock);
    let mut run = Run::start(driver);
    run.until_ready(true, Some("ready"));
    run.until_ready(false, None);

    // It sends again before the limit: ready as the device last said, and
    // the session stands.
    mock.push_event("Test,yy");
    let mut seen = run.until_ready(true, None);
    std::thread::sleep(Duration::from_millis(100));
    seen.extend(run.drain());
    assert!(!reconnected(&seen), "no reconnect");
}

#[test]
fn a_stop_while_measuring_closes_the_session_at_once() {
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let (driver, _) = driver_for(&mock);
    let mut run = Run::start(driver);
    run.until_ready(true, Some("ready"));
    let stopped = Instant::now();
    run.stop();
    let took = stopped.elapsed();
    assert!(took < Duration::from_secs(1), "{took:?}");
    assert!(sent(&mock, Request::MatchEnded), "the session was closed");
}

/// Starting a session cannot be interrupted: a stop during a handshake the
/// device never answers ends when the handshake times out, inside the
/// driver's stop wait.
#[test]
fn a_stop_during_a_wedged_session_start_ends_within_the_stop_wait() {
    let wedged = Wedged::spawn();
    let port = Arc::new(AtomicU16::new(wedged.port));
    let mut tuning = tuning(&port);
    tuning.reply_timeout = Duration::from_millis(1500);
    let driver = ExPutt::with_tuning(localhost(), tuning).expect("driver");
    let mut run = Run::start(driver);
    run.until("connecting", |s| {
        matches!(s, Seen::Event(DeviceEvent::Phase("connecting")))
    });
    std::thread::sleep(Duration::from_millis(200));
    let stopped = Instant::now();
    run.stop();
    let took = stopped.elapsed();
    assert!(took < Duration::from_millis(2500), "{took:?}");
    assert!(took < ExPutt::STOP_WAIT, "{took:?}");
}

#[test]
fn putts_and_messages_are_raw_frames_while_wanted() {
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let (driver, _) = driver_for(&mock);
    let mut run = Run::start_with(driver, TurnHints::default(), true);
    let mut seen = run.until_ready(true, Some("ready"));
    // A keepalive and a heartbeat go by.
    std::thread::sleep(Duration::from_millis(300));
    mock.push_event(PUTT_RESULT_FULL);
    seen.extend(run.until("shot_finished", |s| {
        matches!(s, Seen::Event(DeviceEvent::Frp(FrpBody::ShotFinished(_))))
    }));
    let frames: Vec<RawFrame> = seen
        .iter()
        .filter_map(|s| match s {
            Seen::Event(DeviceEvent::Frp(FrpBody::ShotRaw(f))) => Some(f.clone()),
            _ => None,
        })
        .collect();
    let labels: Vec<&str> = frames.iter().filter_map(|f| f.label.as_deref()).collect();
    assert!(labels.contains(&"Test"), "{labels:?}");
    assert!(labels.contains(&"N_ALIVE"), "{labels:?}");
    assert!(!labels.contains(&"camera frame"), "{labels:?}");

    // The putt's own frame: the message as received, with the shot's key,
    // before the shot events built from it.
    let key = seen
        .iter()
        .find_map(|s| match s {
            Seen::Event(DeviceEvent::Frp(FrpBody::ShotTrigger(t))) => Some(t.key.clone()),
            _ => None,
        })
        .expect("trigger");
    let putt_frame = frames
        .iter()
        .find(|f| f.label.as_deref() == Some("PuttResult"))
        .expect("putt frame");
    assert_eq!(putt_frame.dir, RawDir::Rx);
    assert_eq!(putt_frame.seq, 0, "the host numbers frames");
    assert_eq!(putt_frame.key.as_ref(), Some(&key));
    assert!(putt_frame.timestamp.is_some());
    let text = std::str::from_utf8(&putt_frame.data).expect("text");
    let json: serde_json::Value = serde_json::from_str(text).expect("json");
    assert_eq!(json["Comment"], PUTT_RESULT_FULL);
    let finished = seen.len() - 1;
    let frame_at = seen
        .iter()
        .position(|s| matches!(s, Seen::Event(DeviceEvent::Frp(FrpBody::ShotRaw(f))) if f.label.as_deref() == Some("PuttResult")))
        .expect("frame");
    let trigger_at = seen
        .iter()
        .position(|s| matches!(s, Seen::Event(DeviceEvent::Frp(FrpBody::ShotTrigger(_)))))
        .expect("trigger");
    assert!(frame_at < trigger_at && trigger_at < finished);

    // Camera frames of a mat search reach only the trace log, as hex.
    run.find_mat();
    let seen = run.until_ready(true, Some("ready"));
    assert!(mock.camera_frames_sent() > 0);
    let audited = seen
        .iter()
        .filter(|s| matches!(s, Seen::Event(DeviceEvent::Log(Level::Trace, line)) if line.starts_with("rx camera frame 7b")))
        .count();
    assert!(audited > 0, "camera frames audited");
    assert!(
        !seen.iter().any(|s| matches!(s, Seen::Event(DeviceEvent::Frp(FrpBody::ShotRaw(f))) if f.data.windows(4).any(|w| w == b"/9j/"))),
        "no image leaves as a raw frame"
    );
}

fn raw_frames(seen: &[Seen]) -> usize {
    seen.iter()
        .filter(|s| matches!(s, Seen::Event(DeviceEvent::Frp(FrpBody::ShotRaw(_)))))
        .count()
}

#[test]
fn raw_frames_start_and_stop_with_the_hosts_wish() {
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let (driver, _) = driver_for(&mock);
    let flags = HostFlags::new();
    flags.set_min_level(Level::Info);
    let mut run = Run::start_flags(driver, TurnHints::default(), flags);
    run.until_ready(true, Some("ready"));
    // Heartbeats and keepalives go by, unwanted.
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(raw_frames(&run.drain()), 0);

    // A controller asks for raw frames: the messages from then on arrive.
    run.flags.set_wants_raw(true);
    run.until("a raw frame", |s| {
        matches!(s, Seen::Event(DeviceEvent::Frp(FrpBody::ShotRaw(_))))
    });
    mock.push_event(PUTT_RESULT_FULL);
    let shot = run.until("shot_finished", |s| {
        matches!(s, Seen::Event(DeviceEvent::Frp(FrpBody::ShotFinished(_))))
    });
    assert!(shot.iter().any(|s| matches!(
        s,
        Seen::Event(DeviceEvent::Frp(FrpBody::ShotRaw(f))) if f.label.as_deref() == Some("PuttResult")
    )));

    // The last one leaves: frames stop, the session goes on.
    run.flags.set_wants_raw(false);
    std::thread::sleep(Duration::from_millis(100));
    let _ = run.drain();
    std::thread::sleep(Duration::from_millis(600));
    mock.push_event(PUTT_RESULT_FULL);
    let shot = run.until("shot_finished", |s| {
        matches!(s, Seen::Event(DeviceEvent::Frp(FrpBody::ShotFinished(_))))
    });
    assert_eq!(raw_frames(&shot), 0, "{shot:#?}");
    assert!(!reconnected(&shot));
}

#[test]
fn trace_alone_logs_every_message_as_hex() {
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let (driver, _) = driver_for(&mock);
    // Trace on (the default level), no raw frames wanted.
    let mut run = Run::start(driver);
    let mut seen = run.until_ready(true, Some("ready"));
    // A keepalive and a heartbeat go by.
    std::thread::sleep(Duration::from_millis(400));
    mock.push_event(PUTT_RESULT_FULL);
    seen.extend(run.until("shot_finished", |s| {
        matches!(s, Seen::Event(DeviceEvent::Frp(FrpBody::ShotFinished(_))))
    }));
    assert_eq!(raw_frames(&seen), 0);
    let traced = |prefix: &str| {
        seen.iter().any(|s| {
            matches!(s, Seen::Event(DeviceEvent::Log(Level::Trace, line)) if line.starts_with(prefix))
        })
    };
    assert!(traced("rx PuttResult 7b"), "{seen:#?}");
    assert!(traced("rx Test 7b"), "{seen:#?}");
    assert!(traced("tx N_ALIVE 7b"), "{seen:#?}");
}

#[test]
fn a_new_address_applies_at_the_next_connect() {
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let port = port_of(&mock);
    let mut driver = ExPutt::with_tuning(
        Config {
            address: Some("10.0.0.2".into()),
        },
        tuning(&port),
    )
    .expect("driver");
    driver.apply_config(&localhost());
    let mut run = Run::start(driver);
    let seen = run.until_connected();
    let Some(Seen::Connected(identity)) = seen.last() else {
        unreachable!()
    };
    assert_eq!(identity.device, "ExPutt RG (127.0.0.1)");
}

#[test]
fn no_raw_frames_or_audit_lines_unless_wanted() {
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let (driver, _) = driver_for(&mock);
    let flags = HostFlags::new();
    flags.set_min_level(Level::Info);
    let mut run = Run::start_flags(driver, TurnHints::default(), flags);
    run.until_ready(true, Some("ready"));
    mock.push_event(PUTT_RESULT_FULL);
    let seen = run.until("shot_finished", |s| {
        matches!(s, Seen::Event(DeviceEvent::Frp(FrpBody::ShotFinished(_))))
    });
    assert!(!seen.iter().any(|s| matches!(
        s,
        Seen::Event(DeviceEvent::Frp(FrpBody::ShotRaw(_)) | DeviceEvent::Log(Level::Trace, _))
    )));
}
