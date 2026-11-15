//! The driver on one thread, in virtual time: `TestHost` steps the session
//! over `testing::FakeDevice` through `ExPutt::connect_with`.

use flightrelay::driver::DeviceEvent;
use flightrelay::testing::{TestHost, alerts, bodies, logs, raw_frames, statuses};
use flightrelay::{Club, ShotClub, ShotFinished, ShotLaunch, ShotTrigger};

use super::*;
use crate::testing::FakeDevice;

const PUTT_READY: &str = "PuttReady,106";
const BALL_STRUCK: &str = "PuttShot,107";
const HEARTBEAT: &str = "Test,yy";

/// A driver with the device's defaults.
fn driver() -> ExPutt {
    ExPutt::new(Config::default()).expect("driver")
}

/// A session on `host` over a fresh device, its handshake done.
fn open(host: &mut TestHost, driver: &mut ExPutt) -> (ExPuttSession<FakeDevice>, FakeDevice) {
    let device = FakeDevice::new();
    let session = host
        .connect_with(|ctx| driver.connect_with(ctx, device.clone()))
        .expect("open");
    (session, device)
}

/// A session on `host`, measuring: its mat search done, the device ready,
/// the events taken.
fn measuring(host: &mut TestHost, driver: &mut ExPutt) -> (ExPuttSession<FakeDevice>, FakeDevice) {
    let (mut s, device) = open(host, driver);
    settle(host, &mut s);
    assert_eq!(ready(&host.take()), Some(true));
    (s, device)
}

/// How many packets the client has sent so far.
fn mark(device: &FakeDevice) -> usize {
    device.sent().len()
}

/// What the client sent from packet `from` on, keepalives left out: the
/// `N_TEST` command or the packet type.
fn requests(device: &FakeDevice, from: usize) -> Vec<String> {
    device.sent()[from..]
        .iter()
        .filter(|p| p.packet_id != "N_ALIVE")
        .map(|p| match (&p.comment, p.packet_id.as_str()) {
            (Some(c), "N_TEST") => c.clone(),
            (_, id) => id.to_owned(),
        })
        .collect()
}

/// The requests of a new round with `hand`'s command.
fn round(hand: &str) -> Vec<String> {
    [
        "PM_API_MatchEnded",
        "sendImage",
        "resyncToMat",
        "stopSendImage",
        hand,
        "PM_API_MatchBegin",
    ]
    .map(str::to_owned)
    .to_vec()
}

/// Step at the current instant until nothing is waiting.
fn settle(host: &mut TestHost, s: &mut ExPuttSession<FakeDevice>) {
    while host.with(|now, out| s.step(now, out)).expect("step") == Activity::Active {}
}

/// `d` of time, with the device's heartbeat once a second, up to and
/// including a step at its end.
fn live_for(
    host: &mut TestHost,
    s: &mut ExPuttSession<FakeDevice>,
    device: &FakeDevice,
    d: Duration,
) {
    let end = host.now() + d;
    while let Some(left) = end
        .checked_duration_since(host.now())
        .filter(|d| !d.is_zero())
    {
        device.push_event(HEARTBEAT);
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
fn a_session_starts_not_ready_finds_the_mat_and_is_ready_once_the_device_says_so() {
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
    assert_eq!(requests(&device, 0), ["N_VERIFY_ID", "N_REQ_CONNECT"]);
    assert_eq!(s.identity.device, "ExPutt RG (127.0.0.1)");
    assert_eq!(s.identity.model.as_deref(), Some("ExPutt RG"));
    assert_eq!(s.identity.firmware, None);

    // The first step starts the mat search; the device answers at once.
    settle(&mut host, &mut s);
    assert_eq!(
        requests(&device, 2),
        [
            "sendImage",
            "resyncToMat",
            "stopSendImage",
            "setDexterityRight",
            "PM_API_MatchBegin"
        ],
        "no hint: right"
    );
    let seen = host.take();
    assert_eq!(states(&seen)[0], (false, Some("finding mat".to_owned())));
    assert_eq!(device_states(&seen), ["finding mat", "mat found", "ready"]);
    assert_eq!(ready(&seen), Some(true));
    assert!(logged(
        &seen,
        Level::Info,
        "mat found 0.0s into session start"
    ));
    // Every state's actor carries the same, declared keys.
    let info = ExPutt::info(&Config::default());
    for status in statuses(&seen) {
        let actor = info.bus_actor("x", Duration::from_secs(2), None, &status);
        let keys: Vec<String> = actor.telemetry.expect("telemetry").into_keys().collect();
        assert_eq!(keys, ["device_state"]);
    }

    let before = mark(&device);
    host.with(|_, out| s.close(out));
    assert_eq!(
        requests(&device, before),
        ["PM_API_MatchEnded", "N_APP_PAUSE"]
    );
}

#[test]
fn putts_are_putter_shots_numbered_in_order() {
    let mut host = TestHost::new();
    let mut driver = driver();
    let (mut s, device) = measuring(&mut host, &mut driver);

    device.push_event(PUTT_RESULT_FULL);
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
    device.push_event(PUTT_RESULT_BALL_ONLY);
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
    let (mut s, device) = measuring(&mut host, &mut driver);

    let before = utc_ms().expect("clock");
    device.push_event(BALL_STRUCK);
    settle(&mut host, &mut s);
    let after = utc_ms().expect("clock");
    device.push_event(PUTT_RESULT_FULL);
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
        device.push_event(comment);
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
    let (mut s, device) = measuring(&mut host, &mut driver);

    // Struck: not ready until the device says so again.
    device.push_event(BALL_STRUCK);
    settle(&mut host, &mut s);
    assert_eq!(
        states(&host.take()),
        [(false, Some("ball struck".to_owned()))]
    );

    // The result changes neither value.
    device.push_event(PUTT_RESULT_FULL);
    settle(&mut host, &mut s);
    assert_eq!(states(&host.take()), []);

    device.push_event(PUTT_READY);
    settle(&mut host, &mut s);
    assert_eq!(states(&host.take()), [(true, Some("ready".to_owned()))]);
}

/// A device that closes the connection ends the session; the next attempt
/// comes quickly after a measuring session, and shot numbers go on.
#[test]
fn a_lost_session_is_retried_quickly_and_shot_numbers_go_on() {
    let mut host = TestHost::new();
    let mut driver = driver();
    let (mut s, device) = measuring(&mut host, &mut driver);
    device.push_event(PUTT_RESULT_FULL);
    device.disconnect();
    let error = host
        .run_for(Duration::from_secs(1), |now, out| s.step(now, out))
        .unwrap_err();
    assert!(matches!(error, Error::Lost), "{error:?}");
    host.with(|_, out| s.close(out));
    let seen = host.take();
    assert_eq!(shot(&seen).len(), 4, "the putt before the close went out");
    assert!(logged(&seen, Level::Info, "device closed the connection"));

    let mut backoff = Backoff::new(Duration::from_millis(50), Duration::from_secs(5));
    let delay = host.with(|now, out| {
        driver.retry_delay(
            &SessionEnd::new(&error, now, Some(Duration::from_secs(1)), 0),
            &mut backoff,
            out,
        )
    });
    assert_eq!(delay, Some(Duration::from_millis(50)));
    assert_eq!(
        alert_messages(&host.take()),
        ["ExPutt error: connection to the device lost"]
    );

    let (mut s, device) = measuring(&mut host, &mut driver);
    device.push_event(PUTT_RESULT_FULL);
    settle(&mut host, &mut s);
    let FrpBody::ShotTrigger(trigger) = &shot(&host.take())[0] else {
        unreachable!()
    };
    assert_eq!(trigger.key.shot_number, 2);
}

/// Silence: not ready from 10 s until the device sends again, lost at 90 s
/// (120 s after a strike), and the next attempt waits the stall wait. The
/// client's keepalives go on meanwhile; they are no data from the device.
#[test]
fn a_silent_device_is_not_ready_from_10s_and_lost_at_90s() {
    let mut host = TestHost::new();
    let mut driver = driver();
    let (mut s, device) = measuring(&mut host, &mut driver);

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

    device.push_event(HEARTBEAT);
    settle(&mut host, &mut s);
    let seen = host.take();
    assert_eq!(ready(&seen), Some(true));
    assert!(logged(
        &seen,
        Level::Info,
        "device sent data again after 10.0s"
    ));

    // After a strike the limit is 120 s.
    device.push_event(BALL_STRUCK);
    settle(&mut host, &mut s);
    let struck = host.now();
    let error = host
        .run_for(Duration::from_secs(200), |now, out| s.step(now, out))
        .unwrap_err();
    assert!(matches!(error, Error::Silent(d) if d == SILENCE.after_strike));
    assert_eq!(host.now() - struck, SILENCE.after_strike);
    assert!(
        requests(&device, 0).len() < mark(&device),
        "keepalives went out"
    );
    host.with(|_, out| s.close(out));

    // The next attempts wait at least the stall wait.
    let mut backoff = Backoff::new(Duration::from_millis(50), Duration::from_secs(5));
    let delay = host.with(|now, out| {
        driver.retry_delay(
            &SessionEnd::new(&error, now, Some(Duration::from_secs(130)), 0),
            &mut backoff,
            out,
        )
    });
    assert_eq!(delay, Some(STALL_RECONNECT_WAIT));

    // Without a strike, 90 s.
    host.take();
    let (mut s, _device) = measuring(&mut host, &mut driver);
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
    let (mut s, device) = measuring(&mut host, &mut driver);
    let before = mark(&device);

    // Right, then left inside the gap: one round, to the left.
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
    device.set_find_mat(false);
    settle(&mut host, &mut s);
    let seen = host.take();
    assert!(logged(&seen, Level::Info, "mat search held for 6.0s"));
    assert_eq!(ready(&seen), Some(false), "not ready while one waits");
    assert_eq!(requests(&device, before), Vec::<String>::new());

    live_for(&mut host, &mut s, &device, Duration::from_secs(6));
    assert_eq!(
        requests(&device, before),
        round("setDexterityLeft")[..3].to_vec()
    );
    device.push_event("FindMat,101");
    host.advance(Duration::from_secs(2));
    device.push_event("MatOK,102");
    settle(&mut host, &mut s);
    assert_eq!(requests(&device, before), round("setDexterityLeft"));
    let seen = host.take();
    // The restart reports not ready with the state the device last sent.
    assert_eq!(
        device_states(&seen),
        ["ready", "finding mat", "mat found", "ready"]
    );
    assert_eq!(ready(&seen), Some(true));
    assert!(logged(&seen, Level::Info, "mat found after 2.0s"));

    // The same hand again asks for nothing.
    let before = mark(&device);
    host.with(|_, out| s.hints(&TurnHints::default().with_handedness(Handedness::Left), out))
        .expect("hints");
    live_for(&mut host, &mut s, &device, RESYNC_GAP);
    assert_eq!(requests(&device, before), Vec::<String>::new());
    assert!(states(&host.take()).iter().all(|(ready, _)| *ready));
}

/// A hand change inside the gap is held, not ready meanwhile; a change back
/// to the device's hand drops it, and nothing is sent.
#[test]
fn a_hand_flipped_back_inside_the_gap_sends_nothing() {
    let mut host = TestHost::new();
    let mut driver = driver();
    let (mut s, device) = measuring(&mut host, &mut driver);
    let before = mark(&device);

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
    assert_eq!(requests(&device, before), Vec::<String>::new());
}

/// A turn with no handedness keeps the device's hand.
#[test]
fn a_mode_or_club_alone_changes_nothing() {
    let mut host =
        TestHost::new().with_hints(TurnHints::default().with_handedness(Handedness::Left));
    let mut driver = driver();
    let (mut s, device) = measuring(&mut host, &mut driver);
    assert!(
        requests(&device, 0).contains(&"setDexterityLeft".to_owned()),
        "starts left"
    );
    let before = mark(&device);

    host.with(|_, out| s.hints(&TurnHints::default().with_mode(Mode::Chipping), out))
        .expect("hints");
    host.with(|_, out| s.hints(&TurnHints::default().with_club(Club::PitchingWedge), out))
        .expect("hints");
    host.with(|_, out| s.hints(&TurnHints::default(), out))
        .expect("hints");
    live_for(&mut host, &mut s, &device, RESYNC_GAP);
    assert_eq!(requests(&device, before), Vec::<String>::new());
    assert_eq!(states(&host.take()), []);
}

#[test]
fn find_mat_resyncs_the_device_and_a_slow_search_is_logged() {
    let mut host = TestHost::new();
    let mut driver = driver();
    let (mut s, device) = measuring(&mut host, &mut driver);
    live_for(&mut host, &mut s, &device, RESYNC_GAP);
    host.take();
    let before = mark(&device);

    // Past the gap: at once.
    device.set_find_mat(false);
    host.with(|_, out| s.action(&Action::FindMat, out))
        .expect("action");
    settle(&mut host, &mut s);
    assert_eq!(
        requests(&device, before),
        round("setDexterityRight")[..3].to_vec()
    );
    device.push_event("FindMat,101");
    live_for(&mut host, &mut s, &device, Duration::from_secs(4));
    device.push_event("MatOK,102");
    settle(&mut host, &mut s);
    assert_eq!(requests(&device, before), round("setDexterityRight"));
    let seen = host.take();
    assert_eq!(
        device_states(&seen),
        ["ready", "finding mat", "mat found", "ready"]
    );
    assert!(logged(&seen, Level::Warn, "mat search took 4.0s, over 3s"));

    // A request at shutdown is dropped.
    let before = mark(&device);
    host.with(|_, out| s.action(&Action::FindMat, out))
        .expect("action");
    host.with(|_, out| s.close(out));
    assert!(logged(
        &host.take(),
        Level::Debug,
        "mat search request dropped"
    ));
    assert_eq!(
        requests(&device, before),
        ["PM_API_MatchEnded", "N_APP_PAUSE"]
    );
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
            driver.retry_delay(
                &SessionEnd::new(error, now, connected_for, 1),
                &mut backoff,
                out,
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

/// The session's own mat search, as raw frame labels: what the client
/// sends and what the device answers, its camera frame left out.
const MAT_SEARCH: [&str; 9] = [
    "sendImage",
    "resyncToMat",
    "FindMat",
    "MatOK",
    "stopSendImage",
    "setDexterityRight",
    "PM_API_MatchBegin",
    "IndiCatorOut",
    "PuttReady",
];

#[test]
fn putts_and_messages_are_raw_frames_while_wanted() {
    let mut host = TestHost::new();
    host.flags.set_wants_raw(true);
    let mut driver = driver();
    let (mut s, device) = open(&mut host, &mut driver);
    settle(&mut host, &mut s);
    let seen = host.take();
    assert_eq!(raw_labels(&seen), MAT_SEARCH, "nothing of the handshake");
    assert!(
        logged(&seen, Level::Trace, "rx camera frame 7b"),
        "camera frames reach only the trace log"
    );
    device.push_event(HEARTBEAT);
    host.run_for(KEEPALIVE_AFTER, |now, out| s.step(now, out))
        .expect("step");
    device.push_event(PUTT_RESULT_FULL);
    settle(&mut host, &mut s);
    let seen = host.take();
    assert_eq!(raw_labels(&seen), ["Test", "N_ALIVE", "PuttResult"]);

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

    // The goodbye is raw frames too.
    host.with(|_, out| s.close(out));
    assert_eq!(
        raw_labels(&host.take()),
        ["PM_API_MatchEnded", "N_APP_PAUSE"]
    );
}

/// How long until the client's first keepalive, from the handshake.
const KEEPALIVE_AFTER: Duration = Duration::from_secs(2);

#[test]
fn raw_frames_start_and_stop_with_the_hosts_wish() {
    let mut host = TestHost::new();
    host.flags.set_min_level(Level::Info);
    let mut driver = driver();
    let (mut s, device) = measuring(&mut host, &mut driver);
    device.push_event(HEARTBEAT);
    settle(&mut host, &mut s);
    assert_eq!(raw_labels(&host.take()), Vec::<String>::new());

    // A controller asks for raw frames: the messages from the next step on.
    host.flags.set_wants_raw(true);
    settle(&mut host, &mut s);
    device.push_event(PUTT_RESULT_FULL);
    settle(&mut host, &mut s);
    assert_eq!(raw_labels(&host.take()), ["PuttResult"]);

    // The last one leaves: frames stop, the session goes on.
    host.flags.set_wants_raw(false);
    settle(&mut host, &mut s);
    device.push_event(PUTT_RESULT_FULL);
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
    let (mut s, device) = measuring(&mut host, &mut driver);
    device.push_event(HEARTBEAT);
    host.run_for(KEEPALIVE_AFTER, |now, out| s.step(now, out))
        .expect("step");
    device.push_event(PUTT_RESULT_FULL);
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
    device.push_event(HEARTBEAT);
    host.run_for(KEEPALIVE_AFTER, |now, out| s.step(now, out))
        .expect("step");
    device.push_event(PUTT_RESULT_FULL);
    settle(&mut host, &mut s);
    let seen = host.take();
    assert_eq!(shot(&seen).len(), 4);
    assert!(!seen.iter().any(|e| matches!(
        e,
        DeviceEvent::Frp(FrpBody::ShotRaw(_)) | DeviceEvent::Log(Level::Trace, _)
    )));
}
