# 3putt

[![CI](https://github.com/divotmaker/3putt/actions/workflows/ci.yml/badge.svg)](https://github.com/divotmaker/3putt/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/threeputt.svg)](https://crates.io/crates/threeputt)
[![docs.rs](https://docs.rs/threeputt/badge.svg)](https://docs.rs/threeputt)

## Disclaimer

This project is not affiliated with or endorsed by Invant Inc.
ExPutt and ExPutt RG are trademarks of Invant Inc.

## Description

Rust client library (crate `threeputt`) for the ExPutt RG putting device over
WiFi. It delivers each measured putt (launch direction, ball speed, and putter path and face
angle when the putter is tracked) to your program. The device reports ball
speed and direction only; distance is left to the consumer (for example a
simulator).

- **`Client`** runs a session over a non-blocking stream (the device's TCP
  port, set non-blocking): the handshake, the mat search, keepalives, putts
  as `Event`s, new rounds (`Client::find_mat`, `Client::restart_match`,
  which locate the mat again) and the goodbye. Each `poll_at(now)` does what
  is due at the time it is given.
- **`Beacon::parse`** reads the device's discovery broadcast.

No threads, no blocking calls, no async runtime, no ZeroMQ library.

## Legal Basis — DMCA Section 1201(f)

This project is an exercise of the interoperability exception under
[17 U.S.C. § 1201(f)](https://www.law.cornell.edu/uscode/text/17/1201):

> (f) Reverse Engineering.—
>
> (1) Notwithstanding the provisions of subsection (a)(1)(A), a person who has
> lawfully obtained the right to use a copy of a computer program may
> circumvent a technological measure that effectively controls access to a
> particular portion of that program for the sole purpose of identifying and
> analyzing those elements of the program that are necessary to achieve
> interoperability of an independently created computer program with other
> programs, and that have not previously been readily made available to the
> person engaging in the circumvention, to the extent any such acts of
> identification and analysis do not constitute infringement under this title.
>
> (2) Notwithstanding the provisions of subsections (a)(2) and (b), a person
> may develop and employ technological means to circumvent a technological
> measure, or to circumvent protection afforded by a technological measure, in
> order to enable the identification and analysis described in paragraph (1),
> or for the purpose of enabling interoperability of an independently created
> computer program with other programs, if such means are necessary to achieve
> such interoperability, to the extent that doing so does not constitute
> infringement under this title.

The ExPutt RG uses a proprietary protocol over WiFi to communicate putt data
(ball speed, launch direction, putter path and face angle, etc.) to its mobile
app. Invant does not publish this protocol or provide an SDK for third-party
integration. This project exists solely to enable interoperability with
third-party golf simulation software.

No Invant code is reproduced here; the protocol is described and implemented in
our own words and names. No access controls are circumvented — the device
accepts unencrypted connections on the local network and its session handshake
carries no secret.

## Acceptable Use

This project exists solely to enable interoperability between the ExPutt RG
and third-party golf simulation software.

**It must not be used to:**

- Circumvent licensing or subscription requirements on Invant products
- Unlock paid features without purchase
- Bypass any access controls on Invant software or services

Issues or discussions proposing circumvention of licensing will be closed and
the user blocked.

## License

Licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

at your option.

## Status

**Beta** (`0.x`). Verified against one ExPutt RG (device software
1.21.2176). The API may change between releases. Several values in a putt
result are not yet identified; see the table below.

## Quick start

Put the device in mobile mode (from its TV login screen) and join the same
network, then, with its IP address (the device's beacon carries it;
[`examples/monitor.rs`](examples/monitor.rs) shows a listener):

```rust,no_run
use std::net::TcpStream;
use std::time::Duration;
use threeputt::{Client, DEVICE_PORT, Event, Hand};

fn main() -> threeputt::Result<()> {
    let stream = TcpStream::connect(("192.168.1.40", DEVICE_PORT))?;
    stream.set_nonblocking(true)?;
    let mut client = Client::new(stream, Hand::Right);
    loop {
        match client.poll()? {
            Some(Event::PuttResult(putt)) => println!(
                "{:.2} m/s, {:+.1} deg",
                putt.ball_speed_mps, putt.launch_direction_deg
            ),
            Some(_) => {}
            None => std::thread::sleep(Duration::from_millis(5)),
        }
    }
}
```

Or run the example monitor, which prints every event:

```sh
cargo run --example monitor -- [--ip IP] [--left] [--frames DIR]
```

`--frames DIR` saves the camera images the device streams while it locates
the mat (`Client::set_camera`; without it the session still starts and
stops the stream but drops the frames). The images are upside down; rotate
them 180° to view. Ctrl-C ends the session cleanly.

## Features

No feature is on by default.

| Feature | Adds |
|---|---|
| `config` | `threeputt::config::Config`, the driver's settings, with no network code: it builds for `wasm32-unknown-unknown` with `--no-default-features --features config` |
| `driver` | `threeputt::driver::ExPutt`, a [flightrelay](https://crates.io/crates/flightrelay) `FrpDriver`, with discovery (the UDP beacon socket, via `socket2`) |
| `frp` | the `threeputt-frp` binary |
| `testing` | `threeputt::testing::FakeDevice`, an in-memory device for stepped tests, and sample putts |

## FRP driver and `threeputt-frp` (beta)

`threeputt::driver::ExPutt` serves the device to any host of flightrelay's
`FrpDriver` contract, and `threeputt-frp` is that driver as a standalone
[Flight Relay Protocol](https://github.com/flightrelay/spec) (FRP 0.3.0)
launch monitor actor: it listens for peers on the FRP bus (or dials a hub)
and sends each putt as a shot.

```sh
cargo install threeputt --features frp
threeputt-frp [--config <path>] [--target <listen-addr | ws://host:port/frp>] \
              [--set <key>=<value>]... [--log <info|debug|trace>] [--help]
```

- `--target`: an address to listen on (default `0.0.0.0:5880`), or a `ws://`
  URL of a hub to dial.
- `--set address=192.168.1.40`: the device's IP address. Without it the
  device is found by its discovery beacon. Setting the address makes
  reconnects dependable: a device that has had a client stops broadcasting.
  Without it the driver tries the last device's address first and falls back
  to discovery. `--config` takes a TOML file with
  the same keys (`address = "192.168.1.40"`), and the runner's own settings
  in an `[frp]` table: `actor_id` (the actor's id on the bus, generated and
  written into the file on the first run so it stays the same across
  restarts) and `raw_frames` (default `false`).
- `--help` lists every config key with its default.

Ctrl-C closes the session (the device is told the client is leaving) and
exits 0. An invalid value exits 2.

What goes on the bus:

- The actor's `bus.actor`: a `device` block with model `ExPutt RG`,
  `source` `measured` and the device software version as `firmware` when the
  device was discovered; the `not_armed` blocker while there is no mat (none
  found yet, or a mat search is running or waiting) or the device is silent
  (no blockers otherwise; putting never sets it); and the telemetry key
  `device_state` (`finding mat`, `mat found`, `ready`, `ball detected`;
  `unknown` until the device names a state). `device_state` reads
  `ball detected` from a ball being set down to the putt's result, then
  `ready`.
- Per putt: `shot.trigger` (no timestamp: the device does not report the
  stroke), `shot.launch` with ball speed and launch direction (no
  vertical launch and no spin: the device measures neither), `shot.club`
  with putter path and face angle when the putter was tracked, and
  `shot.finished`.
- A `golfer.turn` whose handedness differs from the device's hand starts a
  new round with that hand; a turn without one keeps the hand (right-handed
  before any). Its mode and club do not apply (the device only measures
  putts).
- `shot.raw` frames when `raw_frames` is on: each message as JSON text,
  labeled with its name. Camera images and unrecognized messages are never
  sent.

A host steps the session with its own clock (flightrelay's `step(now)`): each
step handles at most one device message, and every timer below runs on that
`now`.

The driver gives a device that stops answering time to recover: a session may
be silent for 90 s (120 s while a ball is detected) and it reports `not_armed` from
10 s of silence; after a silent device the next attempt waits at least 15 s; mat
searches (a hand change, Find mat) are at least 10 s apart, with requests in
between folded into one. After two minutes of failed attempts (the device
turned off, out of reach or not answering), one `bus.alert` says so and the
per-attempt alerts stop until a session succeeds. A session with no putt and
no mat search for 5 minutes runs a mat search by itself, because an idle
device stops detecting putts. The first connect after a session its host
ended (a reconnect, a card turned off and on) waits until 3 s have passed
since the close, and its failure raises no alert.

A stop ends a connect at once: discovery, the TCP connect and the handshake
check for one on every pass. The session's mat search runs in its first step.
Closing says goodbye and waits at most half a second for the device to hang
up.

## Putt result

Each `PuttResult` event carries 16 values. A value the device did not measure
is `-9999` on the wire and `None` in the typed fields. When the putter is not
tracked, values 3–16 are all unmeasured and only the ball values are present.

| # | Meaning | Unit, sign | Field | Status |
|---|---|---|---|---|
| 1 | Launch direction | degrees, + = right | `launch_direction_deg` | confirmed |
| 2 | Ball speed | m/s | `ball_speed_mps` | confirmed |
| 3 | unknown | | `raw[2]` | unknown |
| 4 | Putter speed | m/s | `club_speed_mps_unconfirmed` | **unconfirmed** |
| 5 | Putter path | degrees, + = right | `path_deg` | confirmed |
| 6 | Face angle at impact | degrees, + = right | `face_deg` | confirmed |
| 7–16 | unknown | | `raw[6..]` | unknown |

"Confirmed" means the value matches what the device's own display shows for
the same putt. All 16 values are always available in `raw`.

## Events

| Device comment | `Event` |
|---|---|
| `N_OK` | `Ack` |
| `Test` | `Heartbeat` (about once a second) |
| `FindMat` | `FindingMat` |
| `MatOK` | `MatFound` |
| `PuttReady` | `PuttReady` |
| `PuttShot` | `BallDetected` (a ball set down on the mat; can fire without a putt following) |
| `IndiCator` / `IndiCatorOut` | `Indicator` / `IndicatorOut` |
| `PuttResult` | `PuttResult` |
| camera image | `CameraFrame` (JPEG bytes) |
| anything else | `Unknown { packet_id, comment }` |

Unrecognized packets are never errors.

## Protocol overview

- **Discovery:** in mobile mode the device broadcasts a JSON beacon about once
  a second to UDP port 9999, including its IP address.
- **Transport:** TCP port 8889, ZeroMQ ZMTP 2.0. The device is a `ROUTER`, the
  client a `DEALER`. Every message is one frame holding one JSON object.
- **Session:** identify (`N_VERIFY_ID`), join (`N_REQ_CONNECT`), locate the mat
  (`sendImage`, `resyncToMat`, then `stopSendImage` once the mat is found:
  `resyncToMat` makes the device stream camera frames, about 10 per second,
  until told to stop), set the putting hand, start measuring
  (`PM_API_MatchBegin`). The client sends a keepalive
  (`N_ALIVE`) every 2 s. On close it sends `PM_API_MatchEnded` and
  `N_APP_PAUSE`.
- **Events:** device events arrive as packets whose comment is
  `Name,args...`; putt results are `PuttResult,` followed by the 16 values.

## Testing

The `testing` feature adds `threeputt::testing::FakeDevice`, an in-memory
device a test steps itself (no sockets, no threads), plus sample putt results,
for testing code built on this crate without hardware.
