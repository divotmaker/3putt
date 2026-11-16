//! Which of the session's messages may leave the machine as raw frames.

use serde::Deserialize;

use crate::client::Direction;
use crate::event::Event;
use crate::packet::{Hand, Request};

/// The label of a putt result's frame.
pub(super) const PUTT_RESULT_LABEL: &str = "PuttResult";

/// Whether a message may leave the machine as a raw frame, and its label.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Frame {
    /// Carries no device or user identity: a raw frame.
    Shared(&'static str),
    /// A camera image (the player's surroundings), or a message the crate
    /// does not recognize, which it cannot vouch for: an audit line only.
    Private(&'static str),
}

/// The crate's identity-exclusion list. Everything this client sends is a
/// fixed packet; of what the device sends, the known events are shared and
/// camera frames and unrecognized messages are not.
pub(super) fn classify(dir: Direction, body: &[u8]) -> Frame {
    match dir {
        Direction::Tx => Frame::Shared(sent_label(body)),
        Direction::Rx => match Event::parse_filtered(body, false) {
            None => Frame::Private("camera frame"),
            Some(event) => match event {
                Event::Ack { .. } => Frame::Shared("N_OK"),
                Event::Heartbeat => Frame::Shared("Test"),
                Event::FindingMat => Frame::Shared("FindMat"),
                Event::MatFound => Frame::Shared("MatOK"),
                Event::PuttReady => Frame::Shared("PuttReady"),
                Event::BallDetected => Frame::Shared("PuttShot"),
                Event::Indicator => Frame::Shared("IndiCator"),
                Event::IndicatorOut => Frame::Shared("IndiCatorOut"),
                Event::PuttResult(_) => Frame::Shared(PUTT_RESULT_LABEL),
                Event::CameraFrame(_) => Frame::Private("camera frame"),
                Event::Unknown { .. } => Frame::Private("unknown"),
            },
        },
    }
}

/// The name of a packet this client sent: the command of an `N_TEST`, else
/// the packet type.
pub(super) fn sent_label(body: &[u8]) -> &'static str {
    const SENT: [Request; 11] = [
        Request::VerifyId,
        Request::Connect,
        Request::StartCamera,
        Request::ResyncToMat,
        Request::StopCamera,
        Request::SetHand(Hand::Right),
        Request::SetHand(Hand::Left),
        Request::MatchBegin,
        Request::MatchEnded,
        Request::Alive,
        Request::AppPause,
    ];
    #[derive(Deserialize)]
    struct Head {
        #[serde(rename = "PacketID")]
        packet_id: Option<String>,
        #[serde(rename = "Comment")]
        comment: Option<String>,
    }
    let Ok(head) = serde_json::from_slice::<Head>(body) else {
        return "unknown";
    };
    SENT.iter()
        .find(|r| {
            Some(r.packet_id()) == head.packet_id.as_deref()
                && r.comment() == head.comment.as_deref()
        })
        .map_or("unknown", |r| match (r.packet_id(), r.comment()) {
            ("N_TEST", Some(command)) => command,
            (packet_id, _) => packet_id,
        })
}
