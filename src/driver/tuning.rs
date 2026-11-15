//! The device's ports, which tests replace.

use crate::beacon::{DEVICE_PORT, DISCOVERY_PORT};

/// The device's ports. [`Default`] is what the driver uses with a real
/// device; tests that run the driver's own
/// [`connect`](flightrelay::driver::FrpDriver::connect) against local sockets
/// change them. Every timer of the driver runs on the `now` a host steps it
/// with, so none is here.
#[derive(Debug, Clone)]
pub(crate) struct Tuning {
    /// The device's TCP port. Default 8889.
    pub(crate) port: u16,
    /// The UDP port discovery listens on. Default 9999.
    pub(crate) discovery_port: u16,
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            port: DEVICE_PORT,
            discovery_port: DISCOVERY_PORT,
        }
    }
}
