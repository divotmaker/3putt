# 3putt

[![CI](https://github.com/divotmaker/3putt/actions/workflows/ci.yml/badge.svg)](https://github.com/divotmaker/3putt/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/threeputt.svg)](https://crates.io/crates/threeputt)
[![docs.rs](https://docs.rs/threeputt/badge.svg)](https://docs.rs/threeputt)

## Disclaimer

This project is not affiliated with or endorsed by Invant Inc.
ExPutt and ExPutt RG are trademarks of Invant Inc.

## Description

Rust client library (crate `threeputt`) for the ExPutt RG putting device over
WiFi. It talks to the device the way the device's mobile app does and delivers
each measured putt (launch direction, ball speed, and putter path and face
angle when the putter is tracked) to your program. The device reports ball
speed and direction only; distance is left to the consumer (for example a
simulator).

- **`discover`** waits for the device's UDP beacon, or connect straight to a
  known IP.
- **`Session`** connects, runs the setup (handshake, mat sync, putting hand,
  start measuring), sends keepalives, and delivers `Event`s over a channel.
  `Session::find_mat` locates the mat again on a live session, without
  reconnecting. Every new round, including a hand change
  (`Session::restart_match`), locates the mat again too.
- **`Connection`** is the lower layer: send a `Request`, receive an `Event`.

Plain blocking `std::net` sockets and threads. No async runtime, no ZeroMQ
library.

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
integration. The protocol was worked out from WiFi traffic between the
researcher's own lawfully purchased device and the official ExPutt app, and
from analysis of that app, solely to enable interoperability with third-party
golf simulation software.

No Invant code is reproduced here; the protocol is described and implemented in
our own words and names. No access controls were circumvented — the device
accepts unencrypted connections on the local network and its session handshake
carries no secret, and all traffic was captured from the researcher's own
hardware.

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

**Beta** (`0.0.x`). Verified against one ExPutt RG (device software
1.21.2176). The API may change between releases. Several values in a putt
result are not yet identified; see the table below.

## Quick start

Put the device in mobile mode (from its TV login screen) and join the same
network, then:

```rust,no_run
use std::time::Duration;
use threeputt::{Event, Session, SessionOptions, discover};

fn main() -> threeputt::Result<()> {
    let beacon = discover(Duration::from_secs(30))?;
    let session = Session::start(beacon.ip, &SessionOptions::default())?;
    for event in session.events() {
        if let Event::PuttResult(putt) = event {
            println!(
                "{:+.1}°, {:.2} m/s",
                putt.launch_direction_deg, putt.ball_speed_mps
            );
        }
    }
    Ok(())
}
```

Or run the example monitor, which prints every event:

```sh
cargo run --example monitor -- [--ip IP] [--left] [--frames DIR]
```

`--frames DIR` saves the camera images the device streams while it locates
the mat (`SessionOptions::camera`; without it the session still starts and
stops the stream but drops the frames). The images are upside down; rotate
them 180° to view. Ctrl-C ends the session cleanly.

## Features

| Feature | Adds |
|---|---|
| `net` (default) | `discover` (the UDP beacon socket, via `socket2`); a build with `default-features = false` that calls `discover` turns `net` on |
| `config` | `threeputt::config::Config`, the driver's settings, with no network code: it builds for `wasm32-unknown-unknown` with `--no-default-features --features config` |
| `driver` | `threeputt::driver::ExPutt`, a [flightrelay](https://crates.io/crates/flightrelay) `DeviceDriver` |
| `frp` | the `threeputt-frp` binary |
| `testing` | `threeputt::testing::MockDevice`, and test timings for the driver |

## FRP driver and `threeputt-frp` (beta)

`threeputt::driver::ExPutt` serves the device to any host of flightrelay's
`DeviceDriver` contract, and `threeputt-frp` is that driver as a standalone
[Flight Relay Protocol](https://github.com/flightrelay/spec) (FRP 0.2.0)
device: it listens for FRP controllers (or dials one) and sends each putt as
a shot.

```sh
cargo install threeputt --features frp
threeputt-frp [--config <path>] [--target <listen-addr | ws://host:port/frp>] \
              [--set <key>=<value>]... [--log <info|debug|trace>] [--help]
```

- `--target`: an address to listen on (default `0.0.0.0:5880`), or a `ws://`
  URL to dial.
- `--set address=192.168.1.40`: the device's IP address. Without it the
  device is found by its discovery beacon. `--config` takes a TOML file with
  the same keys (`address = "192.168.1.40"`).
- `--help` lists every config key with its default.

Ctrl-C closes the session (the device is told the client is leaving) and
exits 0. An invalid value exits 2.

What a controller receives:

- `device_telemetry` with model `ExPutt RG`, `source` `measured`, the device
  software version as `firmware` when the device was discovered, and the
  keys `ready` and `device_state` (`finding mat`, `mat found`, `ready`,
  `ball struck`).
- Per putt: `shot_trigger` (with the strike time when the device reported
  the strike), `launch` with ball speed and launch direction (no vertical
  launch and no spin: the device measures neither), `club_path` with putter
  path and face angle when the putter was tracked, and `shot_finished`.
- A controller's `set_detection_mode` with a hand starts a new round with
  that hand; its mode and club do not apply (the device only measures putts).
- `raw_frame`s, for a controller that asks for them: each message as JSON
  text, labeled with its name. Camera images and unrecognized messages are
  never sent.

The driver gives a device that stops answering time to recover: a session may
be silent for 90 s (120 s after a strike) and `ready` is false from 10 s of
silence; after a silent device the next attempt waits at least 15 s; mat
searches (a hand change, Find mat) are at least 10 s apart, with requests in
between folded into one. After two minutes of attempts the device accepts but
never answers, one alert says so and the per-attempt alerts stop until a
session succeeds.

A stop during a session start can take up to 18 s: the connect, greeting,
handshake and mat search can't be cut short (3 s, 3 s, 2 × 3 s and 5 s, plus a
second to close).

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
| `Test` | `Heartbeat` (about once a second) |
| `FindMat` | `FindingMat` |
| `MatOK` | `MatFound` |
| `PuttReady` | `PuttReady` |
| `PuttShot` | `BallStruck` (can fire without a putt following) |
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

The `testing` feature adds `threeputt::testing::MockDevice`, an in-process
mock device on a local TCP port, plus sample putt results, for testing code
built on this crate without hardware.
