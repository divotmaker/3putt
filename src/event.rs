//! Packets received from the device.
//!
//! Device packets are JSON objects with `PacketID`, `RequestPacketID` and
//! `Comment`. Replies to [`Request::VerifyId`](crate::Request::VerifyId) and
//! [`Request::Connect`](crate::Request::Connect) use `PacketID` `N_OK`. Every
//! other device event uses `PacketID` `N_FAIL` with a comma-separated
//! `Comment` of the form `Name,args...`:
//!
//! | `Comment` | [`Event`] |
//! |---|---|
//! | `Test,yy` | [`Event::Heartbeat`] (about once a second) |
//! | `FindMat,101` | [`Event::FindingMat`] |
//! | `MatOK,102` | [`Event::MatFound`] |
//! | `PuttReady,106` | [`Event::PuttReady`] |
//! | `PuttShot,107` | [`Event::BallStruck`] |
//! | `IndiCator,109` | [`Event::Indicator`] |
//! | `IndiCatorOut,110` | [`Event::IndicatorOut`] |
//! | `PuttResult,<16 numbers>` | [`Event::PuttResult`] |
//! | base64 JPEG (`/9j/...`) | [`Event::CameraFrame`] |
//!
//! Anything else becomes [`Event::Unknown`]; parsing never fails.

use base64::Engine as _;
use serde::Deserialize;

/// Sentinel the device sends for a value it did not measure.
pub const NOT_MEASURED: f64 = -9999.0;

/// Number of values in a [`PuttResult`].
pub const PUTT_RESULT_FIELDS: usize = 16;

/// An event from the device.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Event {
    /// `N_OK`: the device accepted a request.
    Ack {
        /// `PacketID` of the accepted request, e.g. `N_VERIFY_ID`.
        request: String,
        /// Reply comment (`_INVANT_EXPUTT` in practice).
        comment: Option<String>,
    },
    /// `Test`: periodic heartbeat from the device.
    Heartbeat,
    /// `FindMat`: the device is looking for the mat.
    FindingMat,
    /// `MatOK`: the device has located the mat.
    MatFound,
    /// `PuttReady`: ready for a putt.
    PuttReady,
    /// `PuttShot`: the ball was struck. Can fire without a putt following.
    BallStruck,
    /// `IndiCator`: device indicator signal.
    Indicator,
    /// `IndiCatorOut`: device indicator cleared. Also sent in answer to
    /// match begin/end.
    IndicatorOut,
    /// `PuttResult`: a measured putt.
    PuttResult(PuttResult),
    /// One camera image (JPEG bytes, decoded from the base64 comment). The
    /// device streams these from `resyncToMat` until `stopSendImage`. The
    /// image is upside down; rotate it 180° for display.
    CameraFrame(Vec<u8>),
    /// Any packet not listed above, kept verbatim.
    Unknown {
        /// `PacketID`, or empty if the body was not a JSON packet.
        packet_id: String,
        /// `Comment`, or the raw body if it was not a JSON packet.
        comment: Option<String>,
    },
}

/// Whether a comment is a base64 JPEG (a camera frame).
fn is_frame_comment(comment: &str) -> bool {
    comment.starts_with("/9j/")
}

impl Event {
    /// Parse one message body from the device.
    #[must_use]
    pub fn parse(body: &[u8]) -> Self {
        match serde_json::from_slice::<Incoming>(body) {
            Ok(packet) => Self::from_incoming(packet),
            Err(_) => Self::not_json(body),
        }
    }

    /// As [`parse`](Self::parse), but `None` for a camera frame unless
    /// `frames` is set; the frame is then not base64-decoded.
    pub(crate) fn parse_filtered(body: &[u8], frames: bool) -> Option<Self> {
        let Ok(packet) = serde_json::from_slice::<Incoming>(body) else {
            return Some(Self::not_json(body));
        };
        if !frames
            && packet.packet_id.as_deref() == Some("N_FAIL")
            && packet.comment.as_deref().is_some_and(is_frame_comment)
        {
            return None;
        }
        Some(Self::from_incoming(packet))
    }

    fn not_json(body: &[u8]) -> Self {
        Self::Unknown {
            packet_id: String::new(),
            comment: Some(String::from_utf8_lossy(body).into_owned()),
        }
    }

    fn from_incoming(packet: Incoming) -> Self {
        let packet_id = packet.packet_id.unwrap_or_default();
        match packet_id.as_str() {
            "N_OK" => Self::Ack {
                request: packet.request_packet_id.unwrap_or_default(),
                comment: packet.comment,
            },
            "N_FAIL" => match packet.comment {
                Some(comment) => Self::from_comment(&comment).unwrap_or(Self::Unknown {
                    packet_id,
                    comment: Some(comment),
                }),
                None => Self::Unknown {
                    packet_id,
                    comment: None,
                },
            },
            _ => Self::Unknown {
                packet_id,
                comment: packet.comment,
            },
        }
    }

    /// Parse an event comment (`Name,args...` or a base64 JPEG).
    fn from_comment(comment: &str) -> Option<Self> {
        if is_frame_comment(comment) {
            return base64::engine::general_purpose::STANDARD
                .decode(comment)
                .ok()
                .map(Self::CameraFrame);
        }
        let (name, args) = comment.split_once(',').unwrap_or((comment, ""));
        Some(match name {
            "Test" => Self::Heartbeat,
            "FindMat" => Self::FindingMat,
            "MatOK" => Self::MatFound,
            "PuttReady" => Self::PuttReady,
            "PuttShot" => Self::BallStruck,
            "IndiCator" => Self::Indicator,
            "IndiCatorOut" => Self::IndicatorOut,
            "PuttResult" => Self::PuttResult(PuttResult::parse(args)?),
            _ => return None,
        })
    }
}

/// Wire shape of a device packet.
#[derive(Deserialize)]
struct Incoming {
    #[serde(rename = "PacketID")]
    packet_id: Option<String>,
    #[serde(rename = "RequestPacketID")]
    request_packet_id: Option<String>,
    #[serde(rename = "Comment")]
    comment: Option<String>,
}

/// A measured putt.
///
/// The device sends 16 values. Fields 1, 2, 5 and 6 are confirmed against the
/// device's own display; the rest are kept in [`raw`](Self::raw). When the
/// putter is not tracked, fields 3–16 are all "not measured" and only the ball
/// fields are present.
///
/// | # | meaning | field |
/// |---|---|---|
/// | 1 | launch direction, degrees, + = right | [`launch_direction_deg`](Self::launch_direction_deg) |
/// | 2 | ball speed, m/s | [`ball_speed_mps`](Self::ball_speed_mps) |
/// | 3 | unknown | `raw[2]` |
/// | 4 | putter speed, m/s (**unconfirmed**) | [`club_speed_mps_unconfirmed`](Self::club_speed_mps_unconfirmed) |
/// | 5 | putter path, degrees, + = right | [`path_deg`](Self::path_deg) |
/// | 6 | face angle at impact, degrees, + = right | [`face_deg`](Self::face_deg) |
/// | 7–16 | unknown | `raw[6..]` |
///
/// The result has no roll distance.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PuttResult {
    /// Launch direction in degrees, positive to the right.
    pub launch_direction_deg: f64,
    /// Ball speed in m/s.
    pub ball_speed_mps: f64,
    /// Putter path in degrees, positive to the right.
    pub path_deg: Option<f64>,
    /// Face angle at impact in degrees, positive to the right.
    pub face_deg: Option<f64>,
    /// Field 4. Tracks ball speed and is most likely putter head speed in
    /// m/s, but this is not confirmed.
    pub club_speed_mps_unconfirmed: Option<f64>,
    /// All 16 values as sent, including [`NOT_MEASURED`] sentinels.
    pub raw: [f64; PUTT_RESULT_FIELDS],
}

impl PuttResult {
    /// Parse the 16 comma-separated values that follow `PuttResult,`.
    ///
    /// Returns `None` unless there are exactly 16 numbers and both ball
    /// fields (1 and 2) were measured.
    #[must_use]
    pub fn parse(args: &str) -> Option<Self> {
        let mut raw = [0.0; PUTT_RESULT_FIELDS];
        let mut parts = args.split(',');
        for slot in &mut raw {
            *slot = parts.next()?.trim().parse().ok()?;
        }
        if parts.next().is_some() {
            return None;
        }
        let measured = |v: f64| (v != NOT_MEASURED && v.is_finite()).then_some(v);
        Some(Self {
            launch_direction_deg: measured(raw[0])?,
            ball_speed_mps: measured(raw[1])?,
            club_speed_mps_unconfirmed: measured(raw[3]),
            path_deg: measured(raw[4]),
            face_deg: measured(raw[5]),
            raw,
        })
    }

    /// Field `n` (1-based, as in the table above), or `None` if out of range
    /// or not measured.
    #[must_use]
    pub fn field(&self, n: usize) -> Option<f64> {
        let v = *self.raw.get(n.checked_sub(1)?)?;
        (v != NOT_MEASURED).then_some(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FULL: &str = "PuttResult,1.43502296839333,2.50345148852703,1531.8,1.50199074589741,0.55270159796981,1.48146093525773,105,101,101,201,400,599,2.15371070126487,1.48146093525773,1.3619127672204,51";
    const BALL_ONLY: &str = "PuttResult,0.561243876918884,1.80518847921913,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999,-9999";
    const PULLED: &str = "PuttResult,-10.2790290384019,3.789423622279,965.7,2.04021538616595,-5.16989038221816,-10.5479310729379,90,105,123,166,365,563,-12.8369117616901,-10.5479310729379,-13.4288407664039,51";

    fn device(packet_id: &str, request: &str, comment: &str) -> Vec<u8> {
        serde_json::json!({
            "PacketID": packet_id,
            "RequestPacketID": request,
            "Comment": comment,
        })
        .to_string()
        .into_bytes()
    }

    fn event(comment: &str) -> Event {
        Event::parse(&device("N_FAIL", "N_REQ_CONNECT", comment))
    }

    fn putt(comment: &str) -> PuttResult {
        match event(comment) {
            Event::PuttResult(p) => p,
            other => panic!("expected PuttResult, got {other:?}"),
        }
    }

    #[test]
    fn ack() {
        let body =
            br#"{"PacketID":"N_OK","RequestPacketID":"N_VERIFY_ID","Comment":"_INVANT_EXPUTT"}"#;
        assert_eq!(
            Event::parse(body),
            Event::Ack {
                request: "N_VERIFY_ID".into(),
                comment: Some("_INVANT_EXPUTT".into()),
            }
        );
        assert_eq!(
            Event::parse(&device("N_OK", "N_REQ_CONNECT", "_INVANT_EXPUTT")),
            Event::Ack {
                request: "N_REQ_CONNECT".into(),
                comment: Some("_INVANT_EXPUTT".into()),
            }
        );
    }

    #[test]
    fn status_events() {
        assert_eq!(event("Test,yy"), Event::Heartbeat);
        assert_eq!(event("FindMat,101"), Event::FindingMat);
        assert_eq!(event("MatOK,102"), Event::MatFound);
        assert_eq!(event("PuttReady,106"), Event::PuttReady);
        assert_eq!(event("PuttShot,107"), Event::BallStruck);
        assert_eq!(event("IndiCator,109"), Event::Indicator);
        assert_eq!(event("IndiCatorOut,110"), Event::IndicatorOut);
    }

    #[test]
    fn camera_frame() {
        let jpeg = [0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, b'J', b'F', b'I', b'F'];
        let b64 = base64::engine::general_purpose::STANDARD.encode(jpeg);
        assert!(b64.starts_with("/9j/"));
        assert_eq!(event(&b64), Event::CameraFrame(jpeg.to_vec()));
    }

    #[test]
    fn filtered_parse_drops_only_camera_frames() {
        let frame = device("N_FAIL", "N_REQ_CONNECT", "/9j/4AAQSkZJRg==");
        assert_eq!(Event::parse_filtered(&frame, false), None);
        assert!(matches!(
            Event::parse_filtered(&frame, true),
            Some(Event::CameraFrame(_))
        ));
        let heartbeat = device("N_FAIL", "N_REQ_CONNECT", "Test,yy");
        assert_eq!(
            Event::parse_filtered(&heartbeat, false),
            Some(Event::Heartbeat)
        );
    }

    #[test]
    fn unknown_packets() {
        assert_eq!(
            event("SomethingNew,1,2"),
            Event::Unknown {
                packet_id: "N_FAIL".into(),
                comment: Some("SomethingNew,1,2".into()),
            }
        );
        assert_eq!(
            Event::parse(&device("N_WHAT", "", "x")),
            Event::Unknown {
                packet_id: "N_WHAT".into(),
                comment: Some("x".into()),
            }
        );
        assert_eq!(
            Event::parse(b"not json"),
            Event::Unknown {
                packet_id: String::new(),
                comment: Some("not json".into()),
            }
        );
        assert_eq!(
            Event::parse(br#"{"PacketID":"N_FAIL","Comment":null}"#),
            Event::Unknown {
                packet_id: "N_FAIL".into(),
                comment: None,
            }
        );
        // Truncated or malformed results are kept, not dropped.
        assert!(matches!(event("PuttResult,1,2,3"), Event::Unknown { .. }));
        assert!(matches!(event("/9j/!!!"), Event::Unknown { .. }));
    }

    #[test]
    fn putt_result_full() {
        let p = putt(FULL);
        assert!((p.launch_direction_deg - 1.435_022_968_393_33).abs() < 1e-12);
        assert!((p.ball_speed_mps - 2.503_451_488_527_03).abs() < 1e-12);
        assert_eq!(p.club_speed_mps_unconfirmed, Some(1.501_990_745_897_41));
        assert_eq!(p.path_deg, Some(0.552_701_597_969_81));
        assert_eq!(p.face_deg, Some(1.481_460_935_257_73));
        assert!((p.raw[2] - 1531.8).abs() < 1e-9);
        assert!((p.raw[15] - 51.0).abs() < 1e-9);
        assert_eq!(p.field(3), Some(1531.8));
        assert_eq!(p.field(0), None);
        assert_eq!(p.field(17), None);
    }

    #[test]
    fn putt_result_ball_only() {
        let p = putt(BALL_ONLY);
        assert!((p.launch_direction_deg - 0.561_243_876_918_884).abs() < 1e-12);
        assert!((p.ball_speed_mps - 1.805_188_479_219_13).abs() < 1e-12);
        assert_eq!(p.path_deg, None);
        assert_eq!(p.face_deg, None);
        assert_eq!(p.club_speed_mps_unconfirmed, None);
        assert_eq!(p.field(3), None);
        assert!(p.raw[2..].iter().all(|&v| v == NOT_MEASURED));
    }

    #[test]
    fn putt_result_pulled_left() {
        let p = putt(PULLED);
        assert!(p.launch_direction_deg < -10.0);
        assert!((p.ball_speed_mps - 3.789_423_622_279).abs() < 1e-12);
        assert_eq!(p.path_deg, Some(-5.169_890_382_218_16));
        assert_eq!(p.face_deg, Some(-10.547_931_072_937_9));
        assert_eq!(p.club_speed_mps_unconfirmed, Some(2.040_215_386_165_95));
    }

    #[test]
    fn putt_result_rejects_bad_counts() {
        let fifteen = FULL.rsplit_once(',').unwrap().0;
        assert!(PuttResult::parse(fifteen.trim_start_matches("PuttResult,")).is_none());
        let seventeen = format!("{},1", FULL.trim_start_matches("PuttResult,"));
        assert!(PuttResult::parse(&seventeen).is_none());
        let no_ball = BALL_ONLY.replace("1.80518847921913", "-9999");
        assert!(PuttResult::parse(no_ball.trim_start_matches("PuttResult,")).is_none());
    }
}
