//! Packets sent from this client to the device.
//!
//! Every client packet is a JSON object of the same shape, with the fields in
//! this order:
//!
//! ```json
//! {"PacketID":"N_TEST","RequestPacketID":null,
//!  "DataInfo":{"FilePath":null,"NumOfParts":0,"PartIndex":0,"Length":0},
//!  "Comment":"PM_API_MatchBegin"}
//! ```
//!
//! `PacketID` selects the packet type; `N_TEST` packets carry a command name
//! in `Comment`.

use serde::Serialize;

/// Which hand the player putts with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Hand {
    /// Right-handed.
    #[default]
    Right,
    /// Left-handed.
    Left,
}

/// A packet this client can send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Request {
    /// `N_VERIFY_ID`: first packet of a session. The device answers `N_OK`.
    VerifyId,
    /// `N_REQ_CONNECT` (comment `FALSE`): join the session. The device answers
    /// `N_OK`.
    Connect,
    /// `N_TEST sendImage`: start streaming camera frames.
    StartCamera,
    /// `N_TEST resyncToMat`: locate the mat again. The device also starts
    /// streaming camera frames, until [`Request::StopCamera`].
    ResyncToMat,
    /// `N_TEST stopSendImage`: stop streaming camera frames.
    StopCamera,
    /// `N_TEST setDexterityRight` / `setDexterityLeft`.
    SetHand(Hand),
    /// `N_TEST PM_API_MatchBegin`: start measuring putts.
    MatchBegin,
    /// `N_TEST PM_API_MatchEnded`: stop measuring putts.
    MatchEnded,
    /// `N_ALIVE`: keepalive, sent every 2 s for the whole connection.
    Alive,
    /// `N_APP_PAUSE`: sent before the client drops the connection.
    AppPause,
}

impl Request {
    /// The `PacketID` field.
    #[must_use]
    pub fn packet_id(self) -> &'static str {
        match self {
            Self::VerifyId => "N_VERIFY_ID",
            Self::Connect => "N_REQ_CONNECT",
            Self::StartCamera
            | Self::ResyncToMat
            | Self::StopCamera
            | Self::SetHand(_)
            | Self::MatchBegin
            | Self::MatchEnded => "N_TEST",
            Self::Alive => "N_ALIVE",
            Self::AppPause => "N_APP_PAUSE",
        }
    }

    /// The `Comment` field.
    #[must_use]
    pub fn comment(self) -> Option<&'static str> {
        match self {
            Self::VerifyId | Self::Alive | Self::AppPause => None,
            Self::Connect => Some("FALSE"),
            Self::StartCamera => Some("sendImage"),
            Self::ResyncToMat => Some("resyncToMat"),
            Self::StopCamera => Some("stopSendImage"),
            Self::SetHand(Hand::Right) => Some("setDexterityRight"),
            Self::SetHand(Hand::Left) => Some("setDexterityLeft"),
            Self::MatchBegin => Some("PM_API_MatchBegin"),
            Self::MatchEnded => Some("PM_API_MatchEnded"),
        }
    }

    /// The JSON body of this packet.
    #[must_use]
    pub fn to_json(self) -> String {
        let wire = Wire {
            packet_id: self.packet_id(),
            request_packet_id: None,
            data_info: DataInfo {
                file_path: None,
                num_of_parts: 0,
                part_index: 0,
                length: 0,
            },
            comment: self.comment(),
        };
        // Serializing plain strings, integers and nulls cannot fail.
        serde_json::to_string(&wire).unwrap_or_default()
    }
}

/// Wire shape of a client packet. Field order is the serialization order.
#[derive(Serialize)]
struct Wire<'a> {
    #[serde(rename = "PacketID")]
    packet_id: &'a str,
    #[serde(rename = "RequestPacketID")]
    request_packet_id: Option<&'a str>,
    #[serde(rename = "DataInfo")]
    data_info: DataInfo<'a>,
    #[serde(rename = "Comment")]
    comment: Option<&'a str>,
}

#[derive(Serialize)]
struct DataInfo<'a> {
    #[serde(rename = "FilePath")]
    file_path: Option<&'a str>,
    #[serde(rename = "NumOfParts")]
    num_of_parts: u32,
    #[serde(rename = "PartIndex")]
    part_index: u32,
    #[serde(rename = "Length")]
    length: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zmtp::encode_frame;

    fn expected(packet_id: &str, comment: Option<&str>) -> String {
        let comment = comment.map_or_else(|| "null".to_owned(), |c| format!("\"{c}\""));
        format!(
            "{{\"PacketID\":\"{packet_id}\",\"RequestPacketID\":null,\"DataInfo\":{{\"FilePath\":null,\"NumOfParts\":0,\"PartIndex\":0,\"Length\":0}},\"Comment\":{comment}}}"
        )
    }

    #[test]
    fn verify_id_exact_bytes() {
        let json = Request::VerifyId.to_json();
        assert_eq!(
            json,
            r#"{"PacketID":"N_VERIFY_ID","RequestPacketID":null,"DataInfo":{"FilePath":null,"NumOfParts":0,"PartIndex":0,"Length":0},"Comment":null}"#
        );
        let frame = encode_frame(json.as_bytes());
        assert_eq!(&frame[..2], &[0x00, 0x85]);
        assert_eq!(frame.len(), 2 + 0x85);
    }

    #[test]
    fn connect_exact_bytes() {
        let json = Request::Connect.to_json();
        assert_eq!(
            json,
            r#"{"PacketID":"N_REQ_CONNECT","RequestPacketID":null,"DataInfo":{"FilePath":null,"NumOfParts":0,"PartIndex":0,"Length":0},"Comment":"FALSE"}"#
        );
        assert_eq!(encode_frame(json.as_bytes())[..2], [0x00, 0x8A]);
    }

    #[test]
    fn every_request_exact_bytes() {
        let cases = [
            (Request::VerifyId, "N_VERIFY_ID", None),
            (Request::Connect, "N_REQ_CONNECT", Some("FALSE")),
            (Request::StartCamera, "N_TEST", Some("sendImage")),
            (Request::ResyncToMat, "N_TEST", Some("resyncToMat")),
            (Request::StopCamera, "N_TEST", Some("stopSendImage")),
            (
                Request::SetHand(Hand::Right),
                "N_TEST",
                Some("setDexterityRight"),
            ),
            (
                Request::SetHand(Hand::Left),
                "N_TEST",
                Some("setDexterityLeft"),
            ),
            (Request::MatchBegin, "N_TEST", Some("PM_API_MatchBegin")),
            (Request::MatchEnded, "N_TEST", Some("PM_API_MatchEnded")),
            (Request::Alive, "N_ALIVE", None),
            (Request::AppPause, "N_APP_PAUSE", None),
        ];
        for (req, id, comment) in cases {
            assert_eq!(req.to_json(), expected(id, comment), "{req:?}");
            // Every client packet fits a short frame.
            assert!(req.to_json().len() < 256);
        }
    }

    #[test]
    fn alive_exact_bytes() {
        assert_eq!(
            Request::Alive.to_json(),
            r#"{"PacketID":"N_ALIVE","RequestPacketID":null,"DataInfo":{"FilePath":null,"NumOfParts":0,"PartIndex":0,"Length":0},"Comment":null}"#
        );
    }
}
