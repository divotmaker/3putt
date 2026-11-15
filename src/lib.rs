//! Client for the ExPutt RG putting device over WiFi.
//!
//! The device announces itself with a UDP broadcast beacon and serves a
//! ZeroMQ (ZMTP 2.0) socket on TCP port 8889 that carries JSON packets. This
//! crate implements the beacon, the framing, the packets and the session
//! flow as code its caller steps: no threads, no async runtime and no ZeroMQ
//! library.
//!
//! - [`Client`] runs a session over a non-blocking stream: the handshake,
//!   the mat search, keepalives, putts as [`Event`]s (each
//!   [`PuttResult`]), new rounds and the goodbye. Each
//!   [`poll_at`](Client::poll_at) does what is due at the time it is given.
//! - [`Beacon::parse`] reads the device's discovery broadcast.
//!
//! # Features
//!
//! - **`net`** (default): the discovery socket the driver listens on, which
//!   needs the `socket2` crate.
//! - **`config`**: `config::Config`, the driver's settings, with no
//!   network dependency (it builds for `wasm32-unknown-unknown` with
//!   `--no-default-features`).
//! - **`driver`**: `driver::ExPutt`, a [flightrelay](https://docs.rs/flightrelay)
//!   `FrpDriver` for the device: discovery, the session, putts as FRP
//!   shots, and reconnects paced for a device that stalls.
//! - **`frp`**: the `threeputt-frp` binary, which runs the driver as a launch
//!   monitor actor on a Flight Relay Protocol bus.
//! - **`testing`**: `testing`, an in-memory device for stepped tests, and
//!   sample putts.
//!
//! Reverse engineered for interoperability under DMCA §1201(f). No licensing is
//! circumvented, no proprietary code is reproduced, and no features are unlocked.
//!
//! # Example
//!
//! ```no_run
//! use std::net::TcpStream;
//! use std::time::Duration;
//! use threeputt::{Client, DEVICE_PORT, Event, Hand};
//!
//! # fn main() -> threeputt::Result<()> {
//! let stream = TcpStream::connect(("192.168.1.40", DEVICE_PORT))?;
//! stream.set_nonblocking(true)?;
//! let mut client = Client::new(stream, Hand::Right);
//! loop {
//!     match client.poll()? {
//!         Some(Event::PuttResult(putt)) => println!(
//!             "{:.2} m/s, {:+.1} deg",
//!             putt.ball_speed_mps, putt.launch_direction_deg
//!         ),
//!         Some(_) => {}
//!         None => std::thread::sleep(Duration::from_millis(5)),
//!     }
//! }
//! # }
//! ```

#![warn(missing_docs)]

mod beacon;
mod client;
#[cfg(feature = "config")]
pub mod config;
#[cfg(feature = "driver")]
pub mod driver;
mod error;
mod event;
mod packet;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
mod zmtp;

pub use beacon::{Beacon, DEVICE_PORT, DISCOVERY_PORT};
pub use client::Client;
#[cfg(feature = "config")]
pub use config::Config;
pub use error::{Error, Result};
pub use event::{Event, NOT_MEASURED, PUTT_RESULT_FIELDS, PuttResult};
/// The flightrelay SDK this crate was built with, so callers name its types at the same version.
#[cfg(feature = "config")]
pub use flightrelay;
pub use packet::{Hand, Request};
