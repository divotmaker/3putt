# 3putt

[![CI](https://github.com/divotmaker/3putt/actions/workflows/ci.yml/badge.svg)](https://github.com/divotmaker/3putt/actions/workflows/ci.yml)

## Disclaimer

This project is not affiliated with or endorsed by Invant Inc.
ExPutt is a trademark of its respective owner.

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

Unrecognised packets are never errors.

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

## Acceptable use

This project exists solely to let ExPutt RG owners use their device with
third-party software. It does not unlock features, bypass licensing or access
controls, or reproduce any vendor code; the protocol is implemented in our own
words and names.

## License

Licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

at your option.
