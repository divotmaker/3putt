//! The raw-frame tap: the session's messages queued for the device thread,
//! and which of them may leave the machine as raw frames.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};

use flightrelay::utc_ms;
use serde::Deserialize;

use crate::conn::{Direction, Tap};
use crate::event::Event;
use crate::packet::{Hand, Request};

/// One message the tap saw.
#[derive(Debug)]
pub(super) struct Message {
    pub(super) dir: Direction,
    /// When it crossed the socket (UTC ms).
    pub(super) at: Option<u64>,
    pub(super) body: Vec<u8>,
}

/// Queues the session's messages for the device thread while a host wants
/// raw frames or trace lines.
#[derive(Debug, Clone)]
pub(super) struct RawTap {
    wanted: Arc<AtomicBool>,
    queue: Sender<Message>,
}

impl RawTap {
    pub(super) fn new() -> (Self, Receiver<Message>) {
        let (queue, rx) = mpsc::channel();
        (
            Self {
                wanted: Arc::new(AtomicBool::new(false)),
                queue,
            },
            rx,
        )
    }

    pub(super) fn want(&self, wanted: bool) {
        self.wanted.store(wanted, Ordering::Relaxed);
    }

    /// The tap the session calls on its reader and writer threads.
    pub(super) fn tap(&self) -> Tap {
        let this = self.clone();
        Arc::new(move |dir, body: &[u8]| {
            if this.wanted.load(Ordering::Relaxed) {
                let _ = this.queue.send(Message {
                    dir,
                    at: utc_ms(),
                    body: body.to_vec(),
                });
            }
        })
    }
}

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
                Event::BallStruck => Frame::Shared("PuttShot"),
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
