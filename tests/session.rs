//! Session flow against the in-process mock device.

use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::time::{Duration, Instant};

use threeputt::testing::{
    BALL_STRUCK, MockConfig, MockDevice, PUTT_READY, PUTT_RESULT_BALL_ONLY, PUTT_RESULT_FULL,
    ReceivedPacket, device_event, fake_jpeg,
};
use threeputt::{Connection, Error, Event, Hand, Request, Session, SessionOptions};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
const WAIT: Duration = Duration::from_secs(5);

fn options(mock: &MockDevice) -> SessionOptions {
    SessionOptions {
        port: mock.port(),
        keepalive_interval: Duration::from_millis(100),
        mat_timeout: Duration::from_secs(5),
        ..SessionOptions::default()
    }
}

/// Receive events until `done` matches one, returning everything received.
fn collect_until(session: &Session, mut done: impl FnMut(&Event) -> bool) -> Vec<Event> {
    let deadline = Instant::now() + WAIT;
    let mut seen = Vec::new();
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        let Ok(event) = session.events().recv_timeout(left) else {
            break;
        };
        let stop = done(&event);
        seen.push(event);
        if stop {
            return seen;
        }
    }
    panic!("condition not reached; received {seen:?}");
}

/// The client's packets other than keepalives, as requests.
fn non_alive(packets: &[ReceivedPacket]) -> Vec<String> {
    packets
        .iter()
        .filter(|p| p.packet_id != "N_ALIVE")
        .map(|p| p.json.clone())
        .collect()
}

fn json(requests: &[Request]) -> Vec<String> {
    requests.iter().map(|r| r.to_json()).collect()
}

#[test]
fn full_session_flow() {
    let mock = MockDevice::spawn(MockConfig::default()).unwrap();
    let opts = SessionOptions {
        camera: true,
        ..options(&mock)
    };
    let session = Session::start(LOCALHOST, &opts).unwrap();
    assert!(session.is_connected());

    // Exact greeting: signature, revision 1, DEALER, empty identity frame.
    assert_eq!(
        mock.client_greeting(),
        [
            0xFF, 0, 0, 0, 0, 0, 0, 0, 0x01, 0x7F, 0x01, 0x05, 0x00, 0x00
        ]
    );

    // Setup events reach the caller, including camera frames from long frames.
    let setup = collect_until(&session, |e| *e == Event::PuttReady);
    let (jpeg, _) = fake_jpeg(MockConfig::default().camera_frame_len);
    let frames = setup
        .iter()
        .filter(|e| **e == Event::CameraFrame(jpeg.clone()))
        .count();
    assert!(frames >= 1, "{setup:?}");
    for expected in [
        Event::Heartbeat,
        Event::FindingMat,
        Event::MatFound,
        Event::IndicatorOut,
    ] {
        assert!(setup.contains(&expected), "missing {expected:?}");
    }

    mock.push_event(PUTT_RESULT_FULL);
    mock.push_event("PuttShot,107");
    mock.push_event(PUTT_RESULT_BALL_ONLY);
    let mut putts = Vec::new();
    collect_until(&session, |e| {
        if let Event::PuttResult(p) = e {
            putts.push(*p);
        }
        putts.len() == 2
    });
    assert!((putts[0].ball_speed_mps - 2.503_451_488_527_03).abs() < 1e-12);
    assert_eq!(putts[0].path_deg, Some(0.552_701_597_969_81));
    assert!((putts[1].ball_speed_mps - 1.805_188_479_219_13).abs() < 1e-12);
    assert_eq!(putts[1].face_deg, None);

    // Keepalives flow for the whole connection.
    assert!(mock.wait_until(WAIT, |p| {
        p.iter().filter(|p| p.packet_id == "N_ALIVE").count() >= 3
    }));

    session.close().unwrap();
    assert!(mock.wait_closed(WAIT));

    let packets = mock.received();
    assert_eq!(
        non_alive(&packets),
        json(&[
            Request::VerifyId,
            Request::Connect,
            Request::StartCamera,
            Request::ResyncToMat,
            Request::StopCamera,
            Request::SetHand(Hand::Right),
            Request::MatchBegin,
            Request::MatchEnded,
            Request::AppPause,
        ])
    );
    // Keepalives start after the handshake and stop before the goodbye.
    let first_alive = packets.iter().position(|p| p.packet_id == "N_ALIVE");
    let connect = packets.iter().position(|p| p.packet_id == "N_REQ_CONNECT");
    assert!(first_alive > connect);
    assert_eq!(packets.last().unwrap().packet_id, "N_APP_PAUSE");
    for p in packets.iter().filter(|p| p.packet_id == "N_ALIVE") {
        assert_eq!(p.json, Request::Alive.to_json());
    }
}

#[test]
fn without_camera_left_handed() {
    let mock = MockDevice::spawn(MockConfig::default()).unwrap();
    let opts = SessionOptions {
        hand: Hand::Left,
        ..options(&mock)
    };
    let session = Session::start(LOCALHOST, &opts).unwrap();
    let setup = collect_until(&session, |e| *e == Event::PuttReady);
    assert!(!setup.iter().any(|e| matches!(e, Event::CameraFrame(_))));
    session.close().unwrap();
    assert!(mock.wait_closed(WAIT));
    assert_eq!(
        non_alive(&mock.received()),
        json(&[
            Request::VerifyId,
            Request::Connect,
            Request::StartCamera,
            Request::ResyncToMat,
            Request::StopCamera,
            Request::SetHand(Hand::Left),
            Request::MatchBegin,
            Request::MatchEnded,
            Request::AppPause,
        ])
    );
}

/// The device streams camera frames from `resyncToMat` until `stopSendImage`,
/// whether or not the caller wants them: the session stops the stream either
/// way and delivers no frames when `camera` is off.
#[test]
fn stops_camera_stream_without_camera() {
    let mock = MockDevice::spawn(MockConfig::default()).unwrap();
    let session = Session::start(LOCALHOST, &options(&mock)).unwrap();
    assert!(!options(&mock).camera);
    assert!(mock.wait_until(WAIT, |p| {
        p.iter()
            .any(|p| p.comment.as_deref() == Some("stopSendImage"))
    }));
    let seen = collect_until(&session, |e| *e == Event::PuttReady);
    assert!(!seen.iter().any(|e| matches!(e, Event::CameraFrame(_))));

    // The mock streamed frames, then stopped once told to.
    assert!(mock.camera_frames_sent() >= 1);
    let after_stop = mock.camera_frames_sent();
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(mock.camera_frames_sent(), after_stop);
    // Nothing leaks out later either.
    while let Ok(event) = session.events().try_recv() {
        assert!(!matches!(event, Event::CameraFrame(_)), "{event:?}");
    }
    let order = non_alive(&mock.received());
    let pos = |r: Request| order.iter().position(|j| *j == r.to_json()).unwrap();
    assert!(pos(Request::StartCamera) < pos(Request::ResyncToMat));
    assert!(pos(Request::ResyncToMat) < pos(Request::StopCamera));
    assert!(pos(Request::StopCamera) < pos(Request::MatchBegin));
}

#[test]
fn starts_measuring_when_mat_is_not_found() {
    let mock = MockDevice::spawn(MockConfig {
        find_mat: false,
        ..MockConfig::default()
    })
    .unwrap();
    let opts = SessionOptions {
        mat_timeout: Duration::from_millis(200),
        ..options(&mock)
    };
    let started = Instant::now();
    let session = Session::start(LOCALHOST, &opts).unwrap();
    assert!(started.elapsed() >= Duration::from_millis(200));
    assert!(mock.wait_until(WAIT, |p| {
        p.iter()
            .any(|p| p.comment.as_deref() == Some("PM_API_MatchBegin"))
    }));
    drop(session);
}

#[test]
fn drop_says_goodbye() {
    let mock = MockDevice::spawn(MockConfig::default()).unwrap();
    let session = Session::start(LOCALHOST, &options(&mock)).unwrap();
    // `PuttReady` follows every camera frame on the wire. Closing with frames
    // still unread would reset the connection and lose the goodbye.
    collect_until(&session, |e| *e == Event::PuttReady);
    drop(session);
    assert!(mock.wait_closed(WAIT));
    let tail: Vec<String> = non_alive(&mock.received())
        .into_iter()
        .rev()
        .take(2)
        .collect();
    assert_eq!(
        tail,
        json(&[Request::AppPause, Request::MatchEnded]),
        "goodbye packets, last first"
    );
}

/// A new round locates the mat again: match end and resync, then the hand
/// and match begin only after the device reports the mat found.
#[test]
fn restart_match_resyncs_to_the_mat_and_switches_hand() {
    let mock = MockDevice::spawn(MockConfig::default()).unwrap();
    let session = Session::start(LOCALHOST, &options(&mock)).unwrap();
    collect_until(&session, |e| *e == Event::PuttReady);
    let before = non_alive(&mock.received()).len();

    mock.set_find_mat(false);
    session.restart_match(Hand::Left).unwrap();
    assert!(mock.wait_until(WAIT, |p| non_alive(p).len() >= before + 3));
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(
        non_alive(&mock.received())[before..],
        json(&[
            Request::MatchEnded,
            Request::StartCamera,
            Request::ResyncToMat,
        ]),
        "nothing more until the mat is found"
    );

    mock.push_event("MatOK,102");
    let seen = collect_until(&session, |e| *e == Event::PuttReady);
    assert!(seen.contains(&Event::MatFound), "{seen:?}");
    assert!(mock.wait_until(WAIT, |p| non_alive(p).len() >= before + 6));
    assert_eq!(
        non_alive(&mock.received())[before..],
        find_mat_requests(Hand::Left)
    );
    session.close().unwrap();
}

/// A hand change while `find_mat` waits for the mat sends nothing more; the
/// round it begins uses the new hand.
#[test]
fn restart_match_during_find_mat_begins_with_the_new_hand() {
    let mock = MockDevice::spawn(MockConfig::default()).unwrap();
    let session = Session::start(LOCALHOST, &options(&mock)).unwrap();
    collect_until(&session, |e| *e == Event::PuttReady);
    let before = non_alive(&mock.received()).len();

    mock.set_find_mat(false);
    session.find_mat().unwrap();
    session.restart_match(Hand::Left).unwrap();
    assert!(mock.wait_until(WAIT, |p| non_alive(p).len() >= before + 3));
    mock.push_event("MatOK,102");
    assert!(mock.wait_until(WAIT, |p| non_alive(p).len() >= before + 6));
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(
        non_alive(&mock.received())[before..],
        find_mat_requests(Hand::Left)
    );
    session.close().unwrap();
}

#[test]
fn device_disconnect_ends_event_stream() {
    let mock = MockDevice::spawn(MockConfig::default()).unwrap();
    let session = Session::start(LOCALHOST, &options(&mock)).unwrap();
    mock.disconnect();
    let deadline = Instant::now() + WAIT;
    loop {
        match session
            .events()
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        {
            Ok(_) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            Err(e) => panic!("event stream still open: {e}"),
        }
    }
    assert!(!session.is_connected());
    // Closing a dead session reports the failed goodbye but does not hang.
    let _ = session.close();
}

#[test]
fn handshake_timeout() {
    let mock = MockDevice::spawn(MockConfig {
        answer_handshake: false,
        ..MockConfig::default()
    })
    .unwrap();
    let opts = SessionOptions {
        reply_timeout: Duration::from_millis(300),
        ..options(&mock)
    };
    match Session::start(LOCALHOST, &opts) {
        Err(err @ Error::Timeout(_)) => {
            let text = err.to_string();
            assert_eq!(text, "timed out waiting for the device's handshake reply");
        }
        other => panic!("expected timeout, got {other:?}"),
    }
}

#[test]
fn connection_refused() {
    // Reserve a port, then free it so nothing listens there.
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let opts = SessionOptions {
        port,
        connect_timeout: Duration::from_secs(1),
        ..SessionOptions::default()
    };
    assert!(matches!(
        Session::start(LOCALHOST, &opts),
        Err(Error::Io(_))
    ));
}

#[test]
fn low_level_connection() {
    let mock = MockDevice::spawn(MockConfig::default()).unwrap();
    let addr = SocketAddr::new(LOCALHOST, mock.port());
    let mut conn = Connection::connect(addr, WAIT).unwrap();
    conn.handshake(WAIT).unwrap();
    conn.send(Request::Alive).unwrap();
    mock.push_event("IndiCator,109");
    let deadline = Instant::now() + WAIT;
    loop {
        let event = conn
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap();
        if event == Event::Indicator {
            break;
        }
    }
    assert!(matches!(
        conn.recv_timeout(Duration::from_millis(50)),
        Err(Error::Timeout(_))
    ));
    assert!(mock.wait_until(WAIT, |p| p.iter().any(|p| p.packet_id == "N_ALIVE")));
}

#[test]
fn testing_events_parse_as_named() {
    assert_eq!(Event::parse(&device_event(PUTT_READY)), Event::PuttReady);
    assert_eq!(Event::parse(&device_event(BALL_STRUCK)), Event::BallStruck);
}

/// The requests `find_mat` sends with `hand`, in order.
fn find_mat_requests(hand: Hand) -> Vec<String> {
    json(&[
        Request::MatchEnded,
        Request::StartCamera,
        Request::ResyncToMat,
        Request::StopCamera,
        Request::SetHand(hand),
        Request::MatchBegin,
    ])
}

/// Position of the first event matching `want` in `events`.
fn position(events: &[Event], want: &Event) -> usize {
    events
        .iter()
        .position(|e| e == want)
        .unwrap_or_else(|| panic!("missing {want:?} in {events:?}"))
}

#[test]
fn find_mat_resyncs_and_resumes_measuring() {
    let mock = MockDevice::spawn(MockConfig::default()).unwrap();
    let session = Session::start(LOCALHOST, &options(&mock)).unwrap();
    collect_until(&session, |e| *e == Event::PuttReady);
    let before = non_alive(&mock.received()).len();

    session.find_mat().unwrap();
    let seen = collect_until(&session, |e| *e == Event::PuttReady);
    let finding = position(&seen, &Event::FindingMat);
    let found = position(&seen, &Event::MatFound);
    assert!(finding < found, "{seen:?}");
    assert!(!seen.iter().any(|e| matches!(e, Event::CameraFrame(_))));

    assert!(mock.wait_until(WAIT, |p| non_alive(p).len() >= before + 6));
    assert_eq!(
        non_alive(&mock.received())[before..],
        find_mat_requests(Hand::Right)
    );
    // The camera stream stopped again.
    let after_stop = mock.camera_frames_sent();
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(mock.camera_frames_sent(), after_stop);
    session.close().unwrap();
}

#[test]
fn find_mat_delivers_camera_frames_when_camera_is_on() {
    let mock = MockDevice::spawn(MockConfig::default()).unwrap();
    let opts = SessionOptions {
        camera: true,
        ..options(&mock)
    };
    let session = Session::start(LOCALHOST, &opts).unwrap();
    collect_until(&session, |e| *e == Event::PuttReady);
    session.find_mat().unwrap();
    let seen = collect_until(&session, |e| *e == Event::PuttReady);
    let (jpeg, _) = fake_jpeg(MockConfig::default().camera_frame_len);
    assert!(seen.contains(&Event::CameraFrame(jpeg)), "{seen:?}");
    session.close().unwrap();
}

#[test]
fn find_mat_keeps_the_current_hand() {
    let mock = MockDevice::spawn(MockConfig::default()).unwrap();
    let session = Session::start(LOCALHOST, &options(&mock)).unwrap();
    collect_until(&session, |e| *e == Event::PuttReady);
    session.restart_match(Hand::Left).unwrap();
    collect_until(&session, |e| *e == Event::PuttReady);
    let before = non_alive(&mock.received()).len();
    session.find_mat().unwrap();
    assert!(mock.wait_until(WAIT, |p| non_alive(p).len() >= before + 6));
    assert_eq!(
        non_alive(&mock.received())[before..],
        find_mat_requests(Hand::Left)
    );
    session.close().unwrap();
}

/// `find_mat` returns before the mat is found; without it, measuring resumes
/// after `mat_timeout`. A second call during the wait sends nothing.
#[test]
fn find_mat_does_not_wait_for_the_mat() {
    let mock = MockDevice::spawn(MockConfig {
        find_mat: false,
        ..MockConfig::default()
    })
    .unwrap();
    let opts = SessionOptions {
        mat_timeout: Duration::from_millis(300),
        ..options(&mock)
    };
    let session = Session::start(LOCALHOST, &opts).unwrap();
    assert!(mock.wait_until(WAIT, |p| {
        non_alive(p).ends_with(&json(&[Request::MatchBegin]))
    }));
    let before = non_alive(&mock.received()).len();

    let called = Instant::now();
    session.find_mat().unwrap();
    session.find_mat().unwrap();
    assert!(called.elapsed() < Duration::from_millis(200));
    assert!(mock.wait_until(WAIT, |p| non_alive(p).len() >= before + 6));
    assert!(called.elapsed() >= Duration::from_millis(300));
    assert_eq!(
        non_alive(&mock.received())[before..],
        find_mat_requests(Hand::Right)
    );

    // Once the first has finished, the next call runs again.
    session.find_mat().unwrap();
    assert!(mock.wait_until(WAIT, |p| non_alive(p).len() >= before + 12));
    assert_eq!(
        non_alive(&mock.received())[before + 6..],
        find_mat_requests(Hand::Right)
    );
    session.close().unwrap();
}

/// Closing while `find_mat` waits for the mat returns at once and sends no
/// hand or match begin. The camera is still streaming, so frames can be
/// unread when the socket closes; the reset that follows can cost the
/// device the goodbye packets, so only their order is checked.
#[test]
fn close_during_find_mat_ends_the_wait() {
    let mock = MockDevice::spawn(MockConfig::default()).unwrap();
    let opts = SessionOptions {
        mat_timeout: Duration::from_secs(30),
        ..options(&mock)
    };
    let session = Session::start(LOCALHOST, &opts).unwrap();
    collect_until(&session, |e| *e == Event::PuttReady);
    let before = non_alive(&mock.received()).len();

    mock.set_find_mat(false);
    session.find_mat().unwrap();
    assert!(mock.wait_until(WAIT, |p| non_alive(p).len() >= before + 3));
    let closing = Instant::now();
    let _ = session.close();
    assert!(closing.elapsed() < Duration::from_secs(2));
    assert!(mock.wait_closed(WAIT));

    let sent = non_alive(&mock.received())[before..].to_vec();
    assert_eq!(
        sent[..3],
        json(&[
            Request::MatchEnded,
            Request::StartCamera,
            Request::ResyncToMat,
        ])
    );
    let rest = json(&[Request::StopCamera, Request::MatchEnded, Request::AppPause]);
    assert!(rest.starts_with(&sent[3..]), "{sent:?}");
}
