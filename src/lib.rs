//! Client for the ExPutt RG putting device over WiFi.
//!
//! The device announces itself with a UDP broadcast beacon and serves a
//! ZeroMQ (ZMTP 2.0) socket on TCP port 8889 that carries JSON packets. This
//! crate implements discovery, the framing, the packets and the session flow
//! with plain blocking `std::net` sockets and threads: no async runtime and no
//! ZeroMQ library.
//!
#![cfg_attr(
    feature = "net",
    doc = "- [`discover`] waits for a [`Beacon`] (or connect straight to a known IP)."
)]
#![cfg_attr(
    not(feature = "net"),
    doc = "- `discover` (feature `net`) waits for a [`Beacon`] (or connect straight to a known IP)."
)]
//! - [`Session`] connects, runs the setup, keeps the connection alive and
//!   delivers [`Event`]s, including each [`PuttResult`], over a channel.
//! - [`Connection`] is the lower layer: send a [`Request`], receive an
//!   [`Event`].
//!
//! # Features
//!
//! - **`net`** (default): `discover`, which needs the `socket2` crate.
//! - **`config`**: `config::Config`, the driver's settings, with no
//!   network dependency (it builds for `wasm32-unknown-unknown` with
//!   `--no-default-features`).
//! - **`driver`**: `driver::ExPutt`, a [flightrelay](https://docs.rs/flightrelay)
//!   `DeviceDriver` for the device: discovery, the session, putts as FRP
//!   shots, and reconnects paced for a device that stalls.
//! - **`frp`**: the `threeputt-frp` binary, which serves the driver to Flight
//!   Relay Protocol controllers.
//! - **`testing`**: `testing`, an in-process mock device.
//!
//! Reverse engineered for interoperability under DMCA §1201(f). No licensing is
//! circumvented, no proprietary code is reproduced, and no features are unlocked.
//!
//! # Example
//!
//! ```no_run
//! use std::time::Duration;
//! use threeputt::{Event, Session, SessionOptions, discover};
//!
//! # fn main() -> threeputt::Result<()> {
//! let beacon = discover(Duration::from_secs(30))?;
//! let session = Session::start(beacon.ip, &SessionOptions::default())?;
//! for event in session.events() {
//!     if let Event::PuttResult(putt) = event {
//!         println!(
//!             "{:.2} m/s, {:+.1} deg",
//!             putt.ball_speed_mps, putt.launch_direction_deg
//!         );
//!     }
//! }
//! # Ok(())
//! # }
//! ```

pub mod beacon;
#[cfg(feature = "config")]
pub mod config;
pub mod conn;
#[cfg(feature = "driver")]
pub mod driver;
pub mod error;
pub mod event;
pub mod packet;
pub mod session;
#[cfg(feature = "testing")]
pub mod testing;
pub mod zmtp;

#[cfg(feature = "net")]
pub use beacon::discover;
pub use beacon::{Beacon, DEVICE_PORT, DISCOVERY_PORT};
pub use conn::{Connection, EventReader, PacketWriter};
pub use error::{Error, Result};
pub use event::{Event, NOT_MEASURED, PuttResult};
pub use packet::{Hand, Request};
pub use session::{Session, SessionOptions};
