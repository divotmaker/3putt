//! Smoke tests of the driver's IO shell, on one thread: discovery's UDP
//! socket and the TCP session, with the test's host carrying the bytes
//! between the socket and a `testing::FakeDevice` on every tick. Its clock
//! stands still, so these tests wait on the device's answers, never on the
//! session's timers. What the driver does with the device's events is
//! tested in `stepped`.

use std::io::{Read as _, Write as _};
use std::net::{TcpListener, UdpSocket};

use flightrelay::driver::{ConnectHost, DeviceEvent, HostFlags, Stop};
use flightrelay::testing::TestHost;

use super::*;
use crate::testing::{FakeDevice, PUTT_RESULT_FULL};

/// Ticks a smoke test gives the sockets before it fails.
const GIVE_UP: u32 = 1_000_000;

/// A UDP port nothing sends beacons to.
fn free_udp_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .and_then(|s| s.local_addr())
        .expect("udp port")
        .port()
}

/// The host: on every tick it sends the device's beacon (pointing at
/// localhost) to the discovery port, accepts the driver's connection and
/// carries the bytes both ways between it and `device`.
struct Pump {
    now: Instant,
    hints: TurnHints,
    beacon: UdpSocket,
    discovery_port: u16,
    listener: TcpListener,
    conn: Option<TcpStream>,
    device: FakeDevice,
    /// The driver closed its sending direction.
    ended: bool,
    ticks: u32,
}

impl Pump {
    fn new(device: FakeDevice) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listen");
        listener.set_nonblocking(true).expect("non-blocking");
        Self {
            now: Instant::now(),
            hints: TurnHints::default(),
            beacon: UdpSocket::bind("127.0.0.1:0").expect("bind"),
            discovery_port: free_udp_port(),
            listener,
            conn: None,
            device,
            ended: false,
            ticks: 0,
        }
    }

    fn tuning(&self) -> Tuning {
        Tuning {
            port: self.listener.local_addr().expect("addr").port(),
            discovery_port: self.discovery_port,
        }
    }

    fn pump(&mut self) {
        self.ticks += 1;
        assert!(self.ticks < GIVE_UP, "the sockets never delivered");
        let beacon = r#"{"_serverUUID":"00000000-0000-4000-8000-000000000001","_ipAddress":"127.0.0.1","_appVersion":"1.2.3"}"#;
        let _ = self
            .beacon
            .send_to(beacon.as_bytes(), ("127.0.0.1", self.discovery_port));
        if self.conn.is_none()
            && let Ok((conn, _)) = self.listener.accept()
        {
            conn.set_nonblocking(true).expect("non-blocking");
            self.conn = Some(conn);
        }
        let Some(conn) = &mut self.conn else {
            return;
        };
        let mut buf = [0u8; 16 * 1024];
        loop {
            match conn.read(&mut buf) {
                Ok(0) => {
                    self.ended = true;
                    break;
                }
                Ok(n) => self.device.write_all(&buf[..n]).expect("device write"),
                Err(_) => break,
            }
        }
        while let Ok(n) = self.device.read(&mut buf) {
            let _ = conn.write_all(&buf[..n]);
        }
    }
}

impl ConnectHost for Pump {
    fn tick(&mut self) {
        self.pump();
    }

    fn hints(&self) -> &TurnHints {
        &self.hints
    }

    fn now(&self) -> Instant {
        self.now
    }

    fn wait(&mut self, _: Duration) {}
}

/// Step `s` until an event `want` matches, pumping between steps.
fn step_until(
    pump: &mut Pump,
    s: &mut ExPuttSession<TcpStream>,
    flags: &HostFlags,
    want: impl Fn(&DeviceEvent) -> bool,
) -> Vec<DeviceEvent> {
    let mut seen = Vec::new();
    while !seen.iter().any(&want) {
        pump.pump();
        let mut sink = |e: DeviceEvent| seen.push(e);
        s.step(pump.now, &mut Events::new(&mut sink, flags))
            .expect("step");
    }
    seen
}

/// The device found by its beacon, the session over TCP measuring, a putt,
/// and the goodbye followed by the end of the driver's stream.
#[test]
fn a_discovered_device_is_connected_measures_and_is_closed() {
    let device = FakeDevice::new();
    let mut pump = Pump::new(device.clone());
    let mut driver = ExPutt::with_tuning(Config::default(), pump.tuning()).expect("driver");
    let (stop, flags) = (Stop::new(), HostFlags::new());
    let mut events = Vec::new();
    let mut s = {
        let mut sink = |e: DeviceEvent| events.push(e);
        let mut ctx = ConnectCtx::new(&stop, Events::new(&mut sink, &flags), &mut pump);
        driver.connect(&mut ctx).expect("connect")
    };
    let phases: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            DeviceEvent::Phase(p) => Some(*p),
            _ => None,
        })
        .collect();
    assert_eq!(phases, ["discovering", "connecting", "measuring"]);
    assert_eq!(s.identity().firmware.as_deref(), Some("1.2.3"));
    assert_eq!(s.identity().device, "ExPutt RG (127.0.0.1)");

    step_until(
        &mut pump,
        &mut s,
        &flags,
        |e| matches!(e, DeviceEvent::Status(st) if st.blockers.is_empty()),
    );
    device.push_event(PUTT_RESULT_FULL);
    let seen = step_until(&mut pump, &mut s, &flags, |e| {
        matches!(e, DeviceEvent::Frp(FrpBody::ShotFinished(_)))
    });
    assert!(seen.iter().any(|e| matches!(
        e,
        DeviceEvent::Frp(FrpBody::ShotLaunch(m)) if m.launch.launch_speed.is_some()
    )));

    // The device hangs up first, so the close's drain ends on the end of
    // its stream, not on its bound.
    if let Some(conn) = &pump.conn {
        conn.shutdown(std::net::Shutdown::Write).expect("hang up");
    }
    let mut sink = |_: DeviceEvent| {};
    s.close(&mut Events::new(&mut sink, &flags)).expect("close");
    while !pump.ended {
        pump.pump();
    }
    let sent: Vec<String> = device.received().iter().map(|p| p.json.clone()).collect();
    assert_eq!(
        sent[sent.len() - 2..],
        [Request::MatchEnded.to_json(), Request::AppPause.to_json()]
    );
}

/// Each tick takes the hint's delay in virtual time, so the hint is due on
/// the first pass; the next pass raises no second one, and the stop on the
/// fourth tick ends discovery.
#[test]
fn discovery_without_a_device_hints_once_and_stops_at_the_stop() {
    let tuning = Tuning {
        discovery_port: free_udp_port(),
        ..Tuning::default()
    };
    let mut driver = ExPutt::with_tuning(Config::default(), tuning).expect("driver");
    let mut host = TestHost::new()
        .tick_takes(DISCOVERY_HINT_AFTER)
        .stop_after_ticks(4);
    let error = host.connect(&mut driver).unwrap_err();
    assert!(matches!(error, Error::Stopped), "{error:?}");
    let seen = host.take();
    let alerts: Vec<_> = seen
        .iter()
        .filter_map(|e| match e {
            DeviceEvent::Frp(FrpBody::BusAlert(a)) => Some((a.severity, a.message.as_str())),
            _ => None,
        })
        .collect();
    assert_eq!(alerts, [(Severity::Warn, DISCOVERY_HINT)]);
}
