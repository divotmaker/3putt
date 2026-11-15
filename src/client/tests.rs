//! The client against `testing::FakeDevice`, in virtual time.

use std::time::{Duration, Instant};

use super::*;
use crate::testing::{FakeDevice, PUTT_RESULT_FULL};

/// The `(PacketID or N_TEST command)` of each packet the client sent to the device.
fn sent(device: &FakeDevice) -> Vec<String> {
    device
        .sent()
        .iter()
        .map(|p| match (&p.comment, p.packet_id.as_str()) {
            (Some(c), "N_TEST") => c.clone(),
            (_, id) => id.to_owned(),
        })
        .collect()
}

/// As [`sent`], without the keepalives.
fn requests(device: &FakeDevice) -> Vec<String> {
    let mut sent = sent(device);
    sent.retain(|p| p != "N_ALIVE");
    sent
}

/// Everything the client delivers at `now`, until nothing is waiting.
fn drain(client: &mut Client<FakeDevice>, now: Instant) -> Vec<Event> {
    let mut events = Vec::new();
    while let Some(event) = client.poll_at(now).expect("poll") {
        events.push(event);
    }
    events
}

/// A client with `hand` on a fresh device, measuring at `t0`.
fn measuring(hand: Hand, t0: Instant) -> (Client<FakeDevice>, FakeDevice) {
    let device = FakeDevice::new();
    let mut client = Client::new(device.clone(), hand);
    let events = drain(&mut client, t0);
    assert_eq!(events.last(), Some(&Event::PuttReady));
    (client, device)
}

fn round(hand: &str) -> Vec<&str> {
    vec![
        "PM_API_MatchEnded",
        "sendImage",
        "resyncToMat",
        "stopSendImage",
        hand,
        "PM_API_MatchBegin",
    ]
}

#[test]
fn a_session_greets_joins_finds_the_mat_measures_and_keeps_alive() {
    let t0 = Instant::now();
    let device = FakeDevice::new();
    let mut client = Client::new(device.clone(), Hand::Right);
    let events = drain(&mut client, t0);
    assert_eq!(
        sent(&device),
        [
            "N_VERIFY_ID",
            "N_REQ_CONNECT",
            "sendImage",
            "resyncToMat",
            "stopSendImage",
            "setDexterityRight",
            "PM_API_MatchBegin",
        ]
    );
    // The heartbeat during the handshake comes out like any event; the
    // camera frame does not (camera off).
    let ack = |id: &str| Event::Ack {
        request: id.into(),
        comment: Some("_INVANT_EXPUTT".into()),
    };
    assert_eq!(
        events,
        [
            Event::Heartbeat,
            ack("N_VERIFY_ID"),
            Event::Heartbeat,
            ack("N_REQ_CONNECT"),
            Event::FindingMat,
            Event::MatFound,
            Event::IndicatorOut,
            Event::PuttReady,
        ]
    );

    device.push_event(PUTT_RESULT_FULL);
    assert!(matches!(drain(&mut client, t0)[..], [Event::PuttResult(_)]));

    // A keepalive every 2 s from the end of the handshake, nothing else.
    let before = device.sent().len();
    for s in [1, 2, 3, 4, 5, 6] {
        drain(&mut client, t0 + Duration::from_millis(s * 1000 - 1));
        drain(&mut client, t0 + Duration::from_secs(s));
    }
    assert_eq!(sent(&device)[before..], ["N_ALIVE"; 3]);
}

#[test]
fn camera_frames_come_out_only_when_asked_for() {
    let t0 = Instant::now();
    let (mut client, device) = measuring(Hand::Left, t0);
    assert!(sent(&device).contains(&"setDexterityLeft".to_owned()));
    client.set_camera(true);
    client.find_mat().expect("find mat");
    let events = drain(&mut client, t0);
    let frame = crate::testing::fake_jpeg(60_000).0;
    assert_eq!(
        events[..2],
        [Event::IndicatorOut, Event::CameraFrame(frame)]
    );
}

#[test]
fn measuring_starts_without_the_mat_after_the_wait() {
    let t0 = Instant::now();
    let device = FakeDevice::new();
    device.set_find_mat(false);
    let mut client = Client::new(device.clone(), Hand::Right);
    drain(&mut client, t0);
    assert_eq!(
        requests(&device).last().map(String::as_str),
        Some("resyncToMat")
    );
    drain(
        &mut client,
        t0 + MAT_TIMEOUT.saturating_sub(Duration::from_millis(1)),
    );
    assert_eq!(
        requests(&device).last().map(String::as_str),
        Some("resyncToMat")
    );
    drain(&mut client, t0 + MAT_TIMEOUT);
    assert_eq!(
        requests(&device)[3..],
        [
            "resyncToMat",
            "stopSendImage",
            "setDexterityRight",
            "PM_API_MatchBegin"
        ]
    );
}

/// A device that never greets.
struct Mute;

impl Read for Mute {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }
}

impl Write for Mute {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn an_unanswered_handshake_times_out() {
    let t0 = Instant::now();
    let mut client = Client::new(FakeDevice::unanswering(), Hand::Right);
    drain(&mut client, t0);
    drain(
        &mut client,
        t0 + REPLY_TIMEOUT.saturating_sub(Duration::from_millis(1)),
    );
    assert!(matches!(
        client.poll_at(t0 + REPLY_TIMEOUT),
        Err(Error::Timeout("the device's handshake reply"))
    ));

    let mut client = Client::new(Mute, Hand::Right);
    assert!(matches!(client.poll_at(t0), Ok(None)));
    assert!(matches!(
        client.poll_at(t0 + REPLY_TIMEOUT),
        Err(Error::Timeout("the device's greeting"))
    ));
}

#[test]
fn find_mat_and_restart_match_run_a_round_and_fold_into_one_under_way() {
    let t0 = Instant::now();
    let (mut client, device) = measuring(Hand::Right, t0);
    device.set_find_mat(false);

    // Find mat keeps the hand; a second one and a hand change during the
    // search send nothing, and the search ends with the new hand.
    let before = requests(&device).len();
    client.find_mat().expect("find mat");
    client.find_mat().expect("find mat");
    client.restart_match(Hand::Left).expect("restart");
    drain(&mut client, t0);
    drain(&mut client, t0 + MAT_TIMEOUT);
    assert_eq!(requests(&device)[before..], round("setDexterityLeft"));

    // A restart on a measuring session is a round with its hand, which
    // later rounds keep.
    device.set_find_mat(true);
    let before = requests(&device).len();
    client.restart_match(Hand::Right).expect("restart");
    drain(&mut client, t0 + MAT_TIMEOUT);
    client.find_mat().expect("find mat");
    drain(&mut client, t0 + MAT_TIMEOUT);
    let mut want = round("setDexterityRight");
    want.extend(round("setDexterityRight"));
    assert_eq!(requests(&device)[before..], want);
}

#[test]
fn close_says_goodbye_and_stops_the_camera_during_a_search() {
    let t0 = Instant::now();
    let (mut client, device) = measuring(Hand::Right, t0);
    let before = device.sent().len();
    client.close().expect("close");
    assert_eq!(
        sent(&device)[before..],
        ["PM_API_MatchEnded", "N_APP_PAUSE"]
    );
    // No keepalives after the goodbye.
    drain(&mut client, t0 + Duration::from_secs(10));
    assert_eq!(device.sent().len(), before + 2);

    let (mut client, device) = measuring(Hand::Right, t0);
    device.set_find_mat(false);
    client.find_mat().expect("find mat");
    let before = device.sent().len();
    client.close().expect("close");
    assert_eq!(
        sent(&device)[before..],
        ["stopSendImage", "PM_API_MatchEnded", "N_APP_PAUSE"]
    );
}

#[test]
fn a_device_that_closes_the_connection_ends_the_session() {
    let t0 = Instant::now();
    let (mut client, device) = measuring(Hand::Right, t0);
    device.push_event(PUTT_RESULT_FULL);
    device.disconnect();
    assert!(matches!(client.poll_at(t0), Ok(Some(Event::PuttResult(_)))));
    assert!(matches!(client.poll_at(t0), Err(Error::Closed)));
}

/// A stream that takes `n` bytes per write and then would block once.
struct Trickle {
    device: FakeDevice,
    n: usize,
    blocked: bool,
}

impl Read for Trickle {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.device.read(buf)
    }
}

impl Write for Trickle {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.blocked = !self.blocked;
        if self.blocked {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.device.write(&buf[..buf.len().min(self.n)])
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn a_write_that_would_block_goes_out_whole_at_later_polls() {
    let t0 = Instant::now();
    let device = FakeDevice::new();
    let stream = Trickle {
        device: device.clone(),
        n: 7,
        blocked: false,
    };
    let mut client = Client::new(stream, Hand::Right);
    while device.sent().len() < 7 {
        client.poll_at(t0).expect("poll");
    }
    assert_eq!(sent(&device)[6], "PM_API_MatchBegin");
    let exact: Vec<String> = device.sent().iter().map(|p| p.json.clone()).collect();
    assert_eq!(exact[0], Request::VerifyId.to_json());
    assert_eq!(exact[5], Request::SetHand(Hand::Right).to_json());
}
