//! The ExPutt RG served to Flight Relay Protocol controllers.
//!
//! ```text
//! threeputt-frp [--config <path>] [--target <listen-addr | ws://host:port/frp>]
//!               [--set <key>=<value>]... [--log <info|debug|trace>] [--help]
//! ```

use std::process::ExitCode;

fn main() -> ExitCode {
    flightrelay::run::<threeputt::driver::ExPutt>()
}
