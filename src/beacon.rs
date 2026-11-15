//! Device discovery over UDP broadcast.
//!
//! In mobile mode the device broadcasts a JSON beacon about once a second to
//! UDP port 9999:
//!
//! ```json
//! {"_serverUUID":"00000000-0000-4000-8000-000000000001",
//!  "_message":"Message UUID: 00000000-0000-4000-8000-000000000002",
//!  "_ipAddress":"10.77.0.131","_serverUDPPort":9999,
//!  "_appVersion":"1.21.2176","_autoReconnectFlag":false,"_appLanguage":2}
//! ```

use std::net::{IpAddr, SocketAddr};
#[cfg(feature = "driver")]
use std::net::{Ipv4Addr, UdpSocket};

use serde::Deserialize;
#[cfg(feature = "driver")]
use socket2::{Domain, Protocol, Socket, Type};

use crate::error::{Error, Result};

/// UDP port the device broadcasts its beacon to.
pub const DISCOVERY_PORT: u16 = 9999;

/// TCP port of the device's protocol socket.
pub const DEVICE_PORT: u16 = 8889;

/// A discovery beacon.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Beacon {
    /// Device address: the advertised `_ipAddress`, or the datagram's source
    /// address if that is missing or invalid.
    pub ip: IpAddr,
    /// Device instance UUID (`_serverUUID`).
    pub server_uuid: Option<String>,
    /// Per-beacon message text (`_message`).
    pub message: Option<String>,
    /// UDP port the device broadcasts on (`_serverUDPPort`).
    pub udp_port: Option<u16>,
    /// Device software version (`_appVersion`).
    pub app_version: Option<String>,
    /// `_autoReconnectFlag`.
    pub auto_reconnect: Option<bool>,
    /// Device UI language code (`_appLanguage`).
    pub app_language: Option<i64>,
}

impl Beacon {
    /// Parse a beacon datagram received from `source`.
    ///
    /// # Errors
    ///
    /// [`Error::Beacon`] if the payload is not a JSON object with beacon
    /// fields.
    pub fn parse(payload: &[u8], source: IpAddr) -> Result<Self> {
        let wire: Wire =
            serde_json::from_slice(payload).map_err(|e| Error::Beacon(e.to_string()))?;
        if wire.server_uuid.is_none() && wire.ip_address.is_none() {
            return Err(Error::Beacon("no device address or id".into()));
        }
        let ip = wire
            .ip_address
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(source);
        Ok(Self {
            ip,
            server_uuid: wire.server_uuid,
            message: wire.message,
            udp_port: wire.server_udp_port,
            app_version: wire.app_version,
            auto_reconnect: wire.auto_reconnect,
            app_language: wire.app_language,
        })
    }

    /// The device's TCP protocol address (`ip:8889`).
    #[must_use]
    pub fn device_addr(&self) -> SocketAddr {
        SocketAddr::new(self.ip, DEVICE_PORT)
    }
}

#[derive(Deserialize)]
struct Wire {
    #[serde(rename = "_serverUUID")]
    server_uuid: Option<String>,
    #[serde(rename = "_message")]
    message: Option<String>,
    #[serde(rename = "_ipAddress")]
    ip_address: Option<String>,
    #[serde(rename = "_serverUDPPort")]
    server_udp_port: Option<u16>,
    #[serde(rename = "_appVersion")]
    app_version: Option<String>,
    #[serde(rename = "_autoReconnectFlag")]
    auto_reconnect: Option<bool>,
    #[serde(rename = "_appLanguage")]
    app_language: Option<i64>,
}

/// The discovery socket: `0.0.0.0:port` with `SO_REUSEADDR`, so other
/// listeners on the same port do not block discovery, and non-blocking.
#[cfg(feature = "driver")]
#[derive(Debug)]
pub(crate) struct Listener {
    socket: UdpSocket,
}

#[cfg(feature = "driver")]
impl Listener {
    /// Listen on `0.0.0.0:port`.
    pub(crate) fn bind(port: u16) -> Result<Self> {
        let socket = bind_reusable(port)?;
        socket.set_nonblocking(true)?;
        Ok(Self { socket })
    }

    /// The first valid beacon waiting, if any. Datagrams that are not
    /// beacons are dropped.
    pub(crate) fn try_recv(&self) -> Result<Option<Beacon>> {
        let mut buf = [0u8; 2048];
        loop {
            match self.socket.recv_from(&mut buf) {
                Ok((n, from)) => {
                    if let Ok(beacon) = Beacon::parse(&buf[..n], from.ip()) {
                        return Ok(Some(beacon));
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
}

#[cfg(feature = "driver")]
fn bind_reusable(port: u16) -> Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    socket.set_broadcast(true)?;
    socket.bind(&SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port).into())?;
    Ok(socket.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    const BEACON: &str = r#"{"_serverUUID":"00000000-0000-4000-8000-000000000001","_message":"Message UUID: 00000000-0000-4000-8000-000000000002","_ipAddress":"10.77.0.131","_serverUDPPort":9999,"_appVersion":"1.21.2176","_autoReconnectFlag":false,"_appLanguage":2}"#;

    #[test]
    fn parses_beacon() {
        let b = Beacon::parse(BEACON.as_bytes(), "10.77.0.1".parse().unwrap()).unwrap();
        assert_eq!(b.ip, "10.77.0.131".parse::<IpAddr>().unwrap());
        assert_eq!(
            b.server_uuid.as_deref(),
            Some("00000000-0000-4000-8000-000000000001")
        );
        assert_eq!(
            b.message.as_deref(),
            Some("Message UUID: 00000000-0000-4000-8000-000000000002")
        );
        assert_eq!(b.udp_port, Some(9999));
        assert_eq!(b.app_version.as_deref(), Some("1.21.2176"));
        assert_eq!(b.auto_reconnect, Some(false));
        assert_eq!(b.app_language, Some(2));
        assert_eq!(b.device_addr(), "10.77.0.131:8889".parse().unwrap());
    }

    #[test]
    fn falls_back_to_source_ip() {
        let json = BEACON.replace("10.77.0.131", "not-an-ip");
        let src: IpAddr = "192.168.4.20".parse().unwrap();
        assert_eq!(Beacon::parse(json.as_bytes(), src).unwrap().ip, src);
        let json = r#"{"_serverUUID":"x"}"#;
        assert_eq!(Beacon::parse(json.as_bytes(), src).unwrap().ip, src);
    }

    #[test]
    fn rejects_non_beacons() {
        let src: IpAddr = "10.0.0.1".parse().unwrap();
        assert!(Beacon::parse(b"hello", src).is_err());
        assert!(Beacon::parse(b"{}", src).is_err());
        assert!(Beacon::parse(b"[1,2]", src).is_err());
    }

    /// A beacon waiting on the socket is found, junk before it skipped;
    /// nothing waiting is no error.
    #[cfg(feature = "driver")]
    #[test]
    fn the_listener_takes_the_first_beacon_waiting() {
        let port = UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let listener = Listener::bind(port).unwrap();
        assert_eq!(listener.try_recv().unwrap(), None);
        let tx = UdpSocket::bind("127.0.0.1:0").unwrap();
        tx.send_to(b"junk", ("127.0.0.1", port)).unwrap();
        tx.send_to(BEACON.as_bytes(), ("127.0.0.1", port)).unwrap();
        // Loopback delivers before the send returns; spin on the event,
        // never on a duration.
        let beacon = (0..1_000_000)
            .find_map(|_| listener.try_recv().unwrap())
            .expect("the beacon");
        assert_eq!(beacon.ip, "10.77.0.131".parse::<IpAddr>().unwrap());
    }
}
