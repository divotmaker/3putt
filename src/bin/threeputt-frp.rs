//! The ExPutt RG as a launch monitor actor on a Flight Relay Protocol bus.
//!
//! ```text
//! threeputt-frp [--config <path>] [--target <listen-addr | ws://host:port/frp>]
//!               [--set <key>=<value>]... [--log <info|debug|trace>] [--help]
//! ```

use std::process::ExitCode;

fn main() -> ExitCode {
    flightrelay::run::<threeputt::driver::ExPutt>()
}
