use super::*;

const PUTT_RESULT_FULL: &str = "PuttResult,1.43502296839333,2.50345148852703,1531.8,1.50199074589741,0.55270159796981,1.48146093525773,105,101,101,201,400,599,2.15371070126487,1.48146093525773,1.3619127672204,51";
const PUTT_RESULT_BALL_ONLY: &str = "PuttResult,0.561243876918884,1.80518847921913,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999";

fn device_event(comment: &str) -> Vec<u8> {
    serde_json::json!({
        "PacketID": "N_FAIL",
        "RequestPacketID": "N_REQ_CONNECT",
        "Comment": comment,
    })
    .to_string()
    .into_bytes()
}

/// The putt the device reports for `comment`, as parsed.
fn putt(comment: &str) -> PuttResult {
    match Event::parse(&device_event(comment)) {
        Event::PuttResult(p) => p,
        other => panic!("not a putt: {other:?}"),
    }
}

#[test]
fn silence_is_allowed_90s_and_120s_after_a_strike() {
    let t0 = Instant::now();
    let at = |s: u64| t0 + Duration::from_secs(s);
    let mut w = Watchdog::new(SILENCE, t0);
    assert_eq!(w.check(at(9)), Quiet::Fine);
    // One warning per quiet spell.
    assert_eq!(w.check(at(10)), Quiet::Warn(Duration::from_secs(10)));
    assert_eq!(w.check(at(15)), Quiet::Fine);
    assert_eq!(w.check(at(89)), Quiet::Fine);
    assert_eq!(w.check(at(90)), Quiet::Lost(Duration::from_secs(90)));

    // A heartbeat ends the spell; the next one warns again.
    assert_eq!(
        w.event(&Event::Heartbeat, at(85)),
        Some(Duration::from_secs(85))
    );
    assert_eq!(w.event(&Event::Heartbeat, at(86)), None);
    assert_eq!(w.check(at(96)), Quiet::Warn(Duration::from_secs(10)));

    // After a strike, 120 s until the result or ready.
    let _ = w.event(&Event::BallStruck, at(100));
    assert_eq!(w.check(at(140)), Quiet::Warn(Duration::from_secs(40)));
    assert_eq!(w.check(at(219)), Quiet::Fine);
    assert_eq!(w.check(at(220)), Quiet::Lost(Duration::from_secs(120)));

    // Heartbeats in between keep the longer limit until the result.
    let mut w = Watchdog::new(SILENCE, t0);
    let _ = w.event(&Event::BallStruck, at(0));
    let _ = w.event(&Event::Heartbeat, at(20));
    assert_eq!(w.check(at(75)), Quiet::Warn(Duration::from_secs(55)));
    assert_eq!(w.check(at(139)), Quiet::Fine);
    assert_eq!(w.check(at(140)), Quiet::Lost(Duration::from_secs(120)));
    let _ = w.event(&Event::PuttResult(putt(PUTT_RESULT_FULL)), at(141));
    assert_eq!(w.check(at(231)), Quiet::Lost(Duration::from_secs(90)));

    // Ready again also ends the longer limit.
    let _ = w.event(&Event::BallStruck, at(240));
    let _ = w.event(&Event::PuttReady, at(241));
    assert_eq!(w.check(at(331)), Quiet::Lost(Duration::from_secs(90)));
}

#[test]
fn session_start_times_out_quickly() {
    let o = Tuning::default().session_options(Hand::Left);
    assert_eq!(o.connect_timeout, Duration::from_secs(3));
    assert_eq!(o.reply_timeout, Duration::from_secs(3));
    assert_eq!(o.mat_timeout, Duration::from_secs(5));
    assert_eq!(o.hand, Hand::Left);
    assert_eq!(o.port, crate::DEVICE_PORT);
    assert!(!o.camera);
    assert_eq!(ExPutt::STOP_WAIT, Duration::from_secs(18));
}

#[test]
fn the_driver_is_a_measured_putting_mat_with_text_raw_frames() {
    let info = ExPutt::info(&Config::default());
    assert_eq!(info.kind, "exputt");
    assert_eq!(info.name, "ExPutt RG");
    assert_eq!(info.source, Source::Measured);
    assert!(info.exclusive);
    assert_eq!(info.fixed_mode, Some(Mode::Putting));
    let raw = info.raw.expect("raw profile");
    assert_eq!(raw.transport, "exputt-wifi");
    assert!(raw.text);
    assert!(!(raw.is_bulk)(&RawFrame::new(
        RawDir::Rx,
        0,
        b"{}".to_vec()
    )));
}

#[test]
fn an_invalid_address_builds_no_driver() {
    let e = ExPutt::new(Config {
        address: Some("putter".into()),
    })
    .unwrap_err();
    assert_eq!(e.field.as_deref(), Some("address"));
    assert!(
        ExPutt::new(Config {
            address: Some("10.0.0.9".into())
        })
        .is_ok()
    );
}

#[test]
fn a_new_config_moves_the_driver_to_its_address() {
    let at = |a: Option<&str>| Config {
        address: a.map(str::to_owned),
    };
    let ip = |s: &str| s.parse::<IpAddr>().unwrap();
    let mut driver = ExPutt::new(at(Some("10.0.0.2"))).unwrap();
    assert_eq!(driver.ip, Some(ip("10.0.0.2")));

    // The same device written differently: no restart, the same address.
    assert!(!Config::restart_needed(
        driver.config(),
        &at(Some(" 10.0.0.2 "))
    ));
    driver.apply_config(&at(Some(" 10.0.0.2 ")));
    assert_eq!(driver.ip, Some(ip("10.0.0.2")));
    assert_eq!(driver.config(), &at(Some(" 10.0.0.2 ")));

    // A new address, then blank: discovery.
    assert!(Config::restart_needed(
        driver.config(),
        &at(Some("10.0.0.3"))
    ));
    driver.apply_config(&at(Some("10.0.0.3")));
    assert_eq!(driver.ip, Some(ip("10.0.0.3")));
    driver.apply_config(&at(Some("")));
    assert_eq!(driver.ip, None);

    // Not a config a host applies (it fails `validate`), but harmless: the
    // address in use stays.
    driver.apply_config(&at(Some("10.0.0.4")));
    driver.apply_config(&at(Some("putter")));
    assert_eq!(driver.ip, Some(ip("10.0.0.4")));
}

/// `retry_delay` for `error` after a session that measured for
/// `connected_for`; the delay and the alerts it raised.
fn retry_after(
    driver: &mut ExPutt,
    error: Option<&Error>,
    connected_for: Option<Duration>,
    backoff: &mut Backoff,
) -> (Option<Duration>, Vec<String>) {
    use flightrelay::BusAlert;
    use flightrelay::driver::{DeviceEvent, HostFlags};
    let flags = HostFlags::new();
    let mut alerts = Vec::new();
    let mut sink = |e: DeviceEvent| {
        if let DeviceEvent::Frp(FrpBody::BusAlert(BusAlert { message, .. })) = e {
            alerts.push(message);
        }
    };
    let delay = driver.retry_delay(
        &SessionEnd {
            error,
            connected_for,
            attempts: 1,
        },
        backoff,
        &mut Events::new(&mut sink, &flags),
    );
    (delay, alerts)
}

#[test]
fn a_session_without_an_error_or_a_stop_gets_the_backoff_and_no_alert() {
    let secs = Duration::from_secs;
    let mut driver = ExPutt::new(Config::default()).unwrap();
    let mut backoff = Backoff::default();

    // A session that ended without an error: the host's backoff, no quick
    // retry, no alert.
    assert_eq!(
        retry_after(&mut driver, None, Some(secs(2)), &mut backoff),
        (Some(secs(1)), vec![])
    );
    assert_eq!(
        retry_after(&mut driver, None, None, &mut backoff),
        (Some(secs(2)), vec![])
    );

    // A stop: nothing to tell, and the reconnect policy is untouched.
    assert_eq!(
        retry_after(&mut driver, Some(&Error::Stopped), None, &mut backoff),
        (Some(secs(3)), vec![])
    );
    assert_eq!(
        retry_after(
            &mut driver,
            Some(&Error::Stopped),
            Some(secs(2)),
            &mut backoff
        ),
        (Some(secs(4)), vec![])
    );
    assert_eq!(driver.retry.quick_left, 0);
    assert!(!driver.retry.calm);

    // Any other error still alerts.
    let (_, alerts) = retry_after(&mut driver, Some(&Error::Lost), None, &mut backoff);
    assert_eq!(alerts, ["ExPutt error: connection to the device lost"]);
}

#[test]
fn reconnects_quickly_after_a_measuring_session() {
    let secs = Duration::from_secs;
    let mut backoff = Backoff::default();
    let mut retry = Retry::default();
    let mut delay = |session, silent| retry.delay(&mut backoff, session, silent, secs(15));
    // Never measured: the host's backoff.
    let delays: Vec<_> = (0..3).map(|_| delay(None, false)).collect();
    assert_eq!(delays, [secs(1), secs(2), secs(3)]);

    // A session that measured briefly and dropped: three attempts at
    // 1 s, then the backoff where it had got to.
    let mut delays = vec![delay(Some(secs(2)), false)];
    delays.extend((0..3).map(|_| delay(None, false)));
    assert_eq!(delays, [secs(1), secs(1), secs(1), secs(7)]);
}

#[test]
fn reconnects_calmly_after_silence_until_a_session_succeeds() {
    let secs = Duration::from_secs;
    let mut backoff = Backoff::default();
    let mut retry = Retry::default();
    let mut delay = |session, silent| retry.delay(&mut backoff, session, silent, secs(15));
    // A measuring session lost to silence: 15 s before the first
    // attempt, and at least 15 s between the failed ones after it.
    let mut delays = vec![delay(Some(secs(300)), true)];
    delays.extend((0..4).map(|_| delay(None, false)));
    assert_eq!(delays, [secs(15); 5]);

    // A session that succeeded and was lost another way: fast again.
    assert_eq!(delay(Some(secs(60)), false), secs(1));
    assert_eq!(delay(None, false), secs(1));

    // A silent session that never measured also waits.
    assert_eq!(delay(None, true), secs(15));
}

#[test]
fn mat_searches_are_spaced_and_coalesced() {
    let t0 = Instant::now();
    let at = |s: u64| t0 + Duration::from_secs(s);
    let mut gate = ResyncGate::new(Duration::from_secs(10), Hand::Right, t0);
    assert_eq!(gate.due(at(1)), None, "nothing asked for");

    // rh, rh, lh, lh inside the gap: one restart to lh when it ends.
    gate.want_hand(Hand::Right);
    gate.want_hand(Hand::Right);
    assert!(!gate.pending(), "the hand in effect is no request");
    gate.want_hand(Hand::Left);
    gate.want_hand(Hand::Left);
    assert!(gate.pending());
    assert_eq!(gate.hold(at(2)), Some(Duration::from_secs(8)));
    assert_eq!(gate.due(at(9)), None);
    assert_eq!(gate.due(at(10)), Some(Resync::Restart(Hand::Left)));
    assert_eq!(gate.due(at(11)), None, "run once");

    // A flip back to the hand in effect cancels the request.
    gate.want_hand(Hand::Right);
    gate.want_hand(Hand::Left);
    assert!(!gate.pending());
    assert_eq!(gate.due(at(25)), None);

    // Find mat requests inside the gap are one search; a hand change
    // alongside it makes the search that hand's round.
    gate.want_find();
    gate.want_find();
    assert_eq!(gate.due(at(19)), None, "9 s after the last search");
    assert_eq!(gate.due(at(20)), Some(Resync::Find));
    assert_eq!(gate.due(at(21)), None);
    gate.want_find();
    gate.want_hand(Hand::Right);
    assert_eq!(gate.due(at(29)), None);
    assert_eq!(gate.due(at(30)), Some(Resync::Restart(Hand::Right)));
    assert_eq!(gate.due(at(31)), None);

    // Outside the gap a request runs at once.
    gate.want_find();
    assert_eq!(gate.due(at(60)), Some(Resync::Find));
}

#[test]
fn the_user_is_told_once_per_outage_that_the_device_is_not_answering() {
    let t0 = Instant::now();
    let at = |s: u64| t0 + Duration::from_secs(s);
    let mut u = Unanswered::new(Duration::from_secs(120));
    assert!(!u.no_answer(at(0)));
    assert!(!u.no_answer(at(60)));
    assert!(!u.no_answer(at(119)));
    assert!(u.no_answer(at(120)));
    assert!(!u.no_answer(at(135)), "not again");
    assert!(!u.no_answer(at(600)));

    // A session that succeeds ends the outage; the next one counts from
    // its own first failure.
    u.recovered();
    assert!(!u.no_answer(at(700)));
    assert!(!u.no_answer(at(819)));
    assert!(u.no_answer(at(820)));

    // Attempts that fail some other way in between (refused, not found)
    // are not reported here and do not move the clock: it runs from the
    // first unanswered attempt until a session succeeds.
    let mut u = Unanswered::new(Duration::from_secs(120));
    assert!(!u.no_answer(at(0)));
    assert!(!u.no_answer(at(100)));
    assert!(u.no_answer(at(120)));
}

#[test]
fn a_putt_is_a_launch_without_elevation_or_spin() {
    let launch = launch_of(&putt(PUTT_RESULT_FULL));
    assert_eq!(
        launch.launch_speed,
        Some(Velocity::MetersPerSecond(2.503_451_488_527_03))
    );
    assert_eq!(launch.launch_azimuth, Some(1.435_022_968_393_33));
    assert_eq!(launch.launch_elevation, None, "not measured, not sent");
    assert_eq!(launch.backspin_rpm, None);
    assert_eq!(launch.sidespin_rpm, None);
    assert_eq!(launch.total_spin_rpm, None);
    assert_eq!(launch.spin_axis, None);

    let club = club_of(&putt(PUTT_RESULT_FULL)).expect("putter tracked");
    assert_eq!(club.path, Some(0.552_701_597_969_81));
    assert_eq!(club.face_angle, Some(1.481_460_935_257_73));
    assert_eq!(club.club_speed, None, "the putter speed is not used");
    assert_eq!(club_of(&putt(PUTT_RESULT_BALL_ONLY)), None);
}

#[test]
fn camera_frames_and_unknown_messages_are_private() {
    let jpeg = device_event("/9j/4AAQSkZJRgABAQ==");
    assert_eq!(
        classify(Direction::Rx, &jpeg),
        Frame::Private("camera frame")
    );
    assert_eq!(
        classify(Direction::Rx, &device_event("SomethingNew,1,2")),
        Frame::Private("unknown")
    );
    assert_eq!(
        classify(Direction::Rx, b"not json"),
        Frame::Private("unknown")
    );
    assert_eq!(
        classify(Direction::Rx, &device_event(PUTT_RESULT_FULL)),
        Frame::Shared("PuttResult")
    );
    assert_eq!(
        classify(Direction::Rx, &device_event("Test,yy")),
        Frame::Shared("Test")
    );
    assert_eq!(
        classify(Direction::Rx, &device_event("PuttShot,107")),
        Frame::Shared("PuttShot")
    );
    for (request, label) in [
        (Request::Alive, "N_ALIVE"),
        (Request::ResyncToMat, "resyncToMat"),
        (Request::SetHand(Hand::Left), "setDexterityLeft"),
        (Request::MatchBegin, "PM_API_MatchBegin"),
        (Request::AppPause, "N_APP_PAUSE"),
        (Request::VerifyId, "N_VERIFY_ID"),
        (Request::Connect, "N_REQ_CONNECT"),
    ] {
        assert_eq!(
            classify(Direction::Tx, request.to_json().as_bytes()),
            Frame::Shared(label),
            "{request:?}"
        );
    }
}

/// The `threeputt-frp` command line: the config keys and refusals.
#[cfg(feature = "frp")]
mod cli {
    use std::process::ExitCode;

    use flightrelay::RunOptions;

    use super::*;

    fn run(args: &[&str]) -> ExitCode {
        flightrelay::run_args::<ExPutt>(args.iter().map(|a| (*a).to_owned()), RunOptions::default())
    }

    #[test]
    fn help_lists_the_address_as_unset() {
        assert!(Config::help_toml().contains("# address = (unset)"));
        assert_eq!(run(&["--help"]), ExitCode::SUCCESS);
    }

    #[test]
    fn an_address_set_on_the_command_line_reads_as_a_string() {
        // `--set address=10.0.0.2`: not a TOML value, so taken as a string.
        let config = Config::from_toml_str("address = \"10.0.0.2\"").expect("config");
        assert_eq!(config.ip(), Ok(Some("10.0.0.2".parse().unwrap())));
        assert_eq!(Config::from_toml_str(""), Ok(Config::default()));
    }

    #[test]
    fn bad_values_and_unknown_keys_exit_2() {
        assert_eq!(run(&["--set", "address=putter"]), ExitCode::from(2));
        assert_eq!(run(&["--set", "range=40"]), ExitCode::from(2));
        assert_eq!(run(&["exputt.local"]), ExitCode::from(2), "no positionals");
        let e = Config::from_toml_str("address = \"putter\"").unwrap_err();
        assert_eq!(e.field.as_deref(), Some("address"));
    }
}

#[cfg(feature = "testing")]
mod live;
