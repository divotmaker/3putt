//! Smoke tests of the driver's IO shell: the TCP session against
//! `testing::MockDevice` and discovery's UDP socket, through `TestHost`.
//! What the driver does with the device's events is tested in `stepped`.

use std::net::{TcpListener, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU16};
use std::thread::JoinHandle;

use flightrelay::driver::DeviceEvent;
use flightrelay::testing::TestHost;

use super::*;
use crate::testing::{MockConfig, MockDevice, PUTT_RESULT_FULL};

/// How long a smoke test gives the real device threads before it fails.
const GIVE_UP: Duration = Duration::from_secs(8);

/// A UDP port nothing sends beacons to.
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

/// The device's timings, its TCP port held in `port`, a keepalive every
/// 100 ms.
fn tuning(port: u16) -> Tuning {
    Tuning {
        port: Arc::new(AtomicU16::new(port)),
        discovery_port: free_udp_port(),
        keepalive_interval: Duration::from_millis(100),
        ..Tuning::default()
    }
}

fn localhost() -> Config {
    Config {
        address: Some("127.0.0.1".into()),
    }
}

/// Step `s` until an event `want` matches; everything seen, the match last.
/// The host's clock stands still: these tests wait on the device's threads,
/// never on the session's timers.
fn step_until(
    host: &mut TestHost,
    s: &mut ExPuttSession,
    what: &str,
    want: impl Fn(&DeviceEvent) -> bool,
) -> Result<Vec<DeviceEvent>, Error> {
    let give_up = Instant::now() + GIVE_UP;
    let mut seen = Vec::new();
    loop {
        let activity = host.with(|now, out| s.step(now, out));
        seen.extend(host.take());
        if activity? == Activity::Idle {
            std::thread::yield_now();
        }
        if let Some(at) = seen.iter().position(&want) {
            seen.truncate(at + 1);
            return Ok(seen);
        }
        assert!(Instant::now() < give_up, "{what} not seen; got {seen:#?}");
    }
}

fn traced(prefix: &str) -> impl Fn(&DeviceEvent) -> bool + '_ {
    move |e| matches!(e, DeviceEvent::Log(Level::Trace, line) if line.starts_with(prefix))
}

fn phases(events: &[DeviceEvent]) -> Vec<&'static str> {
    events
        .iter()
        .filter_map(|e| match e {
            DeviceEvent::Phase(p) => Some(*p),
            _ => None,
        })
        .collect()
}

#[test]
fn connects_over_tcp_measures_and_closes_the_session() {
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let mut driver = ExPutt::with_tuning(
        Config {
            address: Some("10.0.0.2".into()),
        },
        tuning(mock.port()),
    )
    .expect("driver");
    // A new address applies at the next connect.
    driver.apply_config(&localhost());
    let mut host = TestHost::new();
    let mut s = host.connect(&mut driver).expect("connect");
    // A configured address skips discovery.
    assert_eq!(phases(&host.take()), ["connecting", "measuring"]);
    assert_eq!(s.identity().device, "ExPutt RG (127.0.0.1)");

    // The tap sees both directions (trace is the default level).
    step_until(&mut host, &mut s, "a heartbeat", traced("rx Test 7b")).expect("live");
    step_until(&mut host, &mut s, "a keepalive", traced("tx N_ALIVE 7b")).expect("live");

    mock.push_event(PUTT_RESULT_FULL);
    let seen = step_until(&mut host, &mut s, "a shot", |e| {
        matches!(e, DeviceEvent::Frp(FrpBody::ShotFinished(_)))
    })
    .expect("live");
    assert!(seen.iter().any(|e| matches!(
        e,
        DeviceEvent::Frp(FrpBody::ShotLaunch(m)) if m.launch.launch_speed.is_some()
    )));

    host.with(|_, out| s.close(out)).expect("close");
    let sent = |r: Request| {
        let json = r.to_json();
        mock.wait_until(GIVE_UP, |p| p.iter().any(|p| p.json == json))
    };
    assert!(sent(Request::MatchEnded), "measuring ended");
    assert!(sent(Request::AppPause), "goodbye");
    assert!(mock.wait_closed(GIVE_UP), "connection closed");
}

#[test]
fn a_device_that_closes_the_connection_ends_the_session() {
    let mock = MockDevice::spawn(MockConfig::default()).expect("mock");
    let mut driver = ExPutt::with_tuning(localhost(), tuning(mock.port())).expect("driver");
    let mut host = TestHost::new();
    let mut s = host.connect(&mut driver).expect("connect");
    mock.disconnect();
    let error = step_until(&mut host, &mut s, "the end", |_| false).unwrap_err();
    assert!(matches!(error, Error::Lost), "{error:?}");
}

#[test]
fn a_device_that_is_not_there_fails_the_connect() {
    let mut driver = ExPutt::with_tuning(localhost(), tuning(closed_port())).expect("driver");
    let error = TestHost::new().connect(&mut driver).unwrap_err();
    assert!(matches!(error, Error::Device(_)), "{error:?}");
}

/// A UDP sender of the device's beacon, pointing at localhost, to `port`.
struct Beaconing {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Beaconing {
    fn to(port: u16) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
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
    let tuning = tuning(mock.port());
    let _beacon = Beaconing::to(tuning.discovery_port);
    let mut driver = ExPutt::with_tuning(Config::default(), tuning).expect("driver");
    let mut host = TestHost::new();
    let s = host.connect(&mut driver).expect("connect");
    assert_eq!(
        phases(&host.take()),
        ["discovering", "connecting", "measuring"]
    );
    assert_eq!(s.identity().firmware.as_deref(), Some("1.2.3"));
    assert_eq!(s.identity().device, "ExPutt RG (127.0.0.1)");
}

/// Each tick takes the hint's delay in virtual time, so the hint is due on
/// the first pass; the next pass raises no second one, and the stop on the
/// fourth tick ends discovery.
#[test]
fn discovery_without_a_device_hints_once_and_stops_at_the_stop() {
    let mut driver = ExPutt::with_tuning(Config::default(), tuning(closed_port())).expect("driver");
    let mut host = TestHost::new()
        .tick_takes(DISCOVERY_HINT_AFTER)
        .stop_after_ticks(4);
    let error = host.connect(&mut driver).unwrap_err();
    assert!(matches!(error, Error::Stopped), "{error:?}");
    let seen = host.take();
    assert_eq!(phases(&seen), ["discovering"]);
    let alerts: Vec<_> = seen
        .iter()
        .filter_map(|e| match e {
            DeviceEvent::Frp(FrpBody::BusAlert(a)) => Some((a.severity, a.message.as_str())),
            _ => None,
        })
        .collect();
    assert_eq!(alerts, [(Severity::Warn, DISCOVERY_HINT)]);
}
