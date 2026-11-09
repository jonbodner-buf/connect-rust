//! The Connect-over-WebSocket wire format shared by the server session and
//! the client transport: message markers, the Leading-Metadata JSON object,
//! the names a client may not set, and the close codes.

use std::collections::VecDeque;
use std::fmt;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use futures::{Sink, SinkExt};
use http::{HeaderMap, HeaderName, HeaderValue};
use serde::de::{Deserializer, MapAccess, Visitor};
use tokio_tungstenite::tungstenite::protocol::frame::Utf8Bytes;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

use crate::codec::CodecFormat;
use crate::error::{ConnectError, ErrorCode};

/// The single byte that opens every message and names its type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Marker {
    Body,
    Metadata,
    ServerEnd,
    ClientEnd,
}

impl Marker {
    pub(crate) fn byte(self) -> u8 {
        match self {
            Self::Body => b'B',
            Self::Metadata => b'M',
            Self::ServerEnd => b'S',
            Self::ClientEnd => b'C',
        }
    }
}

/// Which side of the connection is reading, so a marker valid only in the
/// other direction is rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Side {
    Server,
    #[cfg_attr(not(feature = "client"), allow(dead_code))]
    Client,
}

/// What kind of framing mistake a peer made. It selects the close code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fault {
    Protocol,
    FrameType,
    TooLarge,
}

impl Fault {
    /// The close code the reading side sends for this fault: the 10xx code
    /// from a server, the 31xx code (which browsers can send) from a client.
    pub(crate) fn close_code(self, side: Side) -> u16 {
        let base = match self {
            Self::Protocol => close_code::PROTOCOL_ERROR,
            Self::FrameType => close_code::UNSUPPORTED_DATA,
            Self::TooLarge => close_code::MESSAGE_TOO_BIG,
        };
        match side {
            Side::Server => base,
            Side::Client => base + 2100,
        }
    }
}

/// A peer's framing mistake and the error that reports it.
#[derive(Debug)]
pub(crate) struct WireError {
    pub(crate) fault: Fault,
    pub(crate) error: ConnectError,
}

impl WireError {
    /// A fault found by `side`. A server reports the client's mistake as
    /// `invalid_argument`; a client reports the server's as `internal`,
    /// matching how Connect clients treat a malformed HTTP response.
    pub(crate) fn new(side: Side, fault: Fault, message: impl Into<String>) -> Self {
        let code = match (fault, side) {
            (Fault::TooLarge, _) => ErrorCode::ResourceExhausted,
            (_, Side::Server) => ErrorCode::InvalidArgument,
            (_, Side::Client) => ErrorCode::Internal,
        };
        Self {
            fault,
            error: ConnectError::new(code, message),
        }
    }
}

pub(crate) mod close_code {
    pub(crate) const NORMAL: u16 = 1000;
    pub(crate) const PROTOCOL_ERROR: u16 = 1002;
    pub(crate) const UNSUPPORTED_DATA: u16 = 1003;
    pub(crate) const MESSAGE_TOO_BIG: u16 = 1009;
    pub(crate) const INTERNAL_ERROR: u16 = 1011;
    #[cfg_attr(not(feature = "client"), allow(dead_code))]
    pub(crate) const CLIENT_INTERNAL_ERROR: u16 = 3111;
}

/// One application message with its marker removed.
#[derive(Debug)]
pub(crate) struct WireMessage {
    pub(crate) marker: Marker,
    pub(crate) payload: Bytes,
}

/// Classify a data message read by `side` under `codec`. `Ok(None)` is a
/// control message (Ping, Pong, Close, raw frame), which carries no marker.
///
/// Every rule of the spec's §5 and §6.1 that can be checked without
/// connection state is checked here: the marker, the direction, the frame
/// type for the marker and codec, and the empty payloads that are not JSON.
pub(crate) fn classify(
    message: Message,
    side: Side,
    codec: CodecFormat,
) -> Result<Option<WireMessage>, WireError> {
    let (data, binary) = match message {
        Message::Text(text) => (Bytes::from(text), false),
        Message::Binary(data) => (data, true),
        _ => return Ok(None),
    };
    let protocol_error = |message: String| WireError::new(side, Fault::Protocol, message);
    let Some(&first) = data.first() else {
        return Err(protocol_error("empty message has no marker".into()));
    };
    if first >= 0x80 {
        return Err(protocol_error(format!(
            "marker byte 0x{first:02x} sets the reserved high bit"
        )));
    }
    let marker = match (first, side) {
        (b'B', _) => Marker::Body,
        (b'M', _) => Marker::Metadata,
        (b'S', Side::Client) => Marker::ServerEnd,
        (b'C', Side::Server) => Marker::ClientEnd,
        (b'S' | b'C', _) => {
            return Err(protocol_error(format!(
                "marker '{}' is not valid in this direction",
                first as char
            )));
        }
        _ => {
            return Err(protocol_error(format!("unknown marker 0x{first:02x}")));
        }
    };
    let payload = data.slice(1..);
    let codec_binary = codec == CodecFormat::Proto;
    match marker {
        Marker::Metadata | Marker::ServerEnd => {
            if binary {
                return Err(protocol_error(format!(
                    "'{}' message must be a text frame",
                    first as char
                )));
            }
            if payload.is_empty() {
                return Err(protocol_error(format!(
                    "'{}' message has no JSON payload",
                    first as char
                )));
            }
        }
        Marker::Body | Marker::ClientEnd => {
            if binary != codec_binary {
                return Err(WireError::new(
                    side,
                    Fault::FrameType,
                    format!(
                        "'{}' message arrived as a {} frame, but the negotiated codec is {codec}",
                        first as char,
                        if binary { "binary" } else { "text" },
                    ),
                ));
            }
            if marker == Marker::Body && !binary && payload.is_empty() {
                return Err(protocol_error(
                    "text 'B' message has no payload; an empty JSON message is {}".into(),
                ));
            }
        }
    }
    Ok(Some(WireMessage { marker, payload }))
}

/// Build the message for `marker` with `payload`: a text frame for JSON, a
/// binary frame for a Protobuf body.
///
/// # Errors
///
/// `internal` when a text payload is not UTF-8, which a peer must reject.
pub(crate) fn encode(
    marker: Marker,
    payload: &[u8],
    binary: bool,
) -> Result<Message, ConnectError> {
    let mut buf = BytesMut::with_capacity(payload.len() + 1);
    buf.put_u8(marker.byte());
    buf.extend_from_slice(payload);
    let data = buf.freeze();
    if binary {
        return Ok(Message::Binary(data));
    }
    Utf8Bytes::try_from(data)
        .map(Message::Text)
        .map_err(|_| ConnectError::internal("a JSON message is not valid UTF-8"))
}

/// A body (or final `C` body) message under `codec`.
pub(crate) fn encode_body(
    marker: Marker,
    payload: &[u8],
    codec: CodecFormat,
) -> Result<Message, ConnectError> {
    encode(marker, payload, codec == CodecFormat::Proto)
}

/// A JSON message (`M` or `S`) built from JSON this crate serialized, which
/// is always UTF-8, so the lossy conversion never changes it.
pub(crate) fn encode_json(marker: Marker, payload: &[u8]) -> Message {
    let mut text = String::with_capacity(payload.len() + 1);
    text.push(marker.byte() as char);
    text.push_str(&String::from_utf8_lossy(payload));
    Message::Text(text.into())
}

/// The messages waiting to be written, driven from a `select!` branch so a
/// stalled write never blocks reads, the deadline, or keep-alive. Polling is
/// cancel-safe: a message leaves the queue only once the sink accepts it.
#[derive(Default)]
pub(crate) struct Outbound {
    queue: VecDeque<Message>,
    flushing: bool,
}

impl Outbound {
    /// Whether nothing is queued, so the caller may produce more.
    pub(crate) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Whether there is anything to write or flush.
    pub(crate) fn is_busy(&self) -> bool {
        !self.queue.is_empty() || self.flushing
    }

    pub(crate) fn push(&mut self, message: Message) {
        self.queue.push_back(message);
    }

    /// Drop what has not been handed to the sink yet.
    #[cfg_attr(not(feature = "client"), allow(dead_code))]
    pub(crate) fn clear(&mut self) {
        self.queue.clear();
    }

    /// Write and flush everything queued.
    pub(crate) fn poll_write<S>(
        &mut self,
        sink: &mut S,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), WsError>>
    where
        S: Sink<Message, Error = WsError> + Unpin,
    {
        while !self.queue.is_empty() {
            ready!(sink.poll_ready_unpin(cx))?;
            if let Some(message) = self.queue.pop_front() {
                sink.start_send_unpin(message)?;
                self.flushing = true;
            }
        }
        ready!(sink.poll_flush_unpin(cx))?;
        self.flushing = false;
        Poll::Ready(Ok(()))
    }

    pub(crate) async fn write<S>(&mut self, sink: &mut S) -> Result<(), WsError>
    where
        S: Sink<Message, Error = WsError> + Unpin,
    {
        std::future::poll_fn(|cx| self.poll_write(sink, cx)).await
    }
}

/// Frame `payload` as a Connect envelope with `flag`.
///
/// # Errors
///
/// `resource_exhausted` for a payload too long for the 32-bit length.
pub(crate) fn envelope(flag: u8, payload: &[u8]) -> Result<Bytes, ConnectError> {
    let len = u32::try_from(payload.len()).map_err(|_| {
        ConnectError::resource_exhausted("a message is too large for a Connect envelope")
    })?;
    let mut buf = BytesMut::with_capacity(crate::envelope::HEADER_SIZE + payload.len());
    buf.put_u8(flag);
    buf.put_u32(len);
    buf.extend_from_slice(payload);
    Ok(buf.freeze())
}

// ============================================================================
// Leading-Metadata
// ============================================================================

/// Decode a Leading-Metadata payload into header entries, enforcing §7.3.1:
/// an object of string arrays, valid field names and values, no two keys
/// that fold to the same name, and valid base64 under a `-bin` key.
///
/// An entry with no values is returned too, since it still replaces what
/// the handshake carried for that key.
pub(crate) fn decode_metadata(
    payload: &[u8],
    side: Side,
) -> Result<Vec<(HeaderName, Vec<HeaderValue>)>, WireError> {
    let fault = |message: String| WireError::new(side, Fault::Protocol, message);
    let mut deserializer = serde_json::Deserializer::from_slice(payload);
    let raw = deserializer
        .deserialize_map(EntriesVisitor)
        .and_then(|raw| deserializer.end().map(|()| raw))
        .map_err(|e| fault(format!("invalid metadata message: {e}")))?;

    let mut entries: Vec<(HeaderName, Vec<HeaderValue>)> = Vec::with_capacity(raw.len());
    for (key, values) in raw {
        let name = HeaderName::from_bytes(key.as_bytes())
            .map_err(|_| fault(format!("invalid metadata key {key:?}")))?;
        if entries.iter().any(|(seen, _)| *seen == name) {
            return Err(fault(format!(
                "metadata key {key:?} appears more than once"
            )));
        }
        let binary = name.as_str().ends_with("-bin");
        let mut header_values = Vec::with_capacity(values.len());
        for value in values {
            if binary && crate::error::detail_b64::decode_lenient(&value).is_err() {
                return Err(fault(format!(
                    "metadata value for {key:?} is not valid base64"
                )));
            }
            let value = HeaderValue::from_str(&value)
                .map_err(|_| fault(format!("invalid metadata value for {key:?}")))?;
            header_values.push(value);
        }
        entries.push((name, header_values));
    }
    Ok(entries)
}

/// Encode `headers` as a Leading-Metadata (or `EndStreamResponse.metadata`)
/// object, skipping names `skip` rejects and values that are not text.
pub(crate) fn encode_metadata(headers: &HeaderMap, skip: impl Fn(&HeaderName) -> bool) -> Bytes {
    let mut object = serde_json::Map::new();
    for name in headers.keys() {
        if skip(name) {
            continue;
        }
        let values: Vec<serde_json::Value> = headers
            .get_all(name)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .map(|value| serde_json::Value::String(value.to_owned()))
            .collect();
        object.insert(name.as_str().to_owned(), serde_json::Value::Array(values));
    }
    Bytes::from(serde_json::Value::Object(object).to_string())
}

/// Collects a JSON object's entries in order, keeping duplicate keys so they
/// can be rejected instead of silently overwritten.
struct EntriesVisitor;

impl<'de> Visitor<'de> for EntriesVisitor {
    type Value = Vec<(String, Vec<String>)>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an object whose values are arrays of strings")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut entries = Vec::with_capacity(map.size_hint().unwrap_or(0));
        while let Some(entry) = map.next_entry::<String, Vec<String>>()? {
            entries.push(entry);
        }
        Ok(entries)
    }
}

// ============================================================================
// Reserved request metadata names (§7.3.2)
// ============================================================================

/// The default infrastructure deny list: what a proxy in front of the server
/// sets and a client must not be able to forge.
pub(crate) const DEFAULT_INFRASTRUCTURE_HEADERS: &[&str] =
    &["forwarded", "x-forwarded-*", "x-real-ip"];

const FORBIDDEN_REQUEST_HEADERS: &[&str] = &[
    "accept-charset",
    "accept-encoding",
    "access-control-request-headers",
    "access-control-request-method",
    "connection",
    "content-length",
    "cookie",
    "cookie2",
    "date",
    "dnt",
    "expect",
    "host",
    "keep-alive",
    "origin",
    "referer",
    "set-cookie",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "via",
    "x-http-method",
    "x-http-method-override",
    "x-method-override",
];

/// Names the protocol itself controls, which neither side carries as
/// metadata.
const PROTOCOL_HEADERS: &[&str] = &[
    "connect-protocol-version",
    "connect-timeout-ms",
    "content-type",
    "content-encoding",
    "connect-content-encoding",
    "connect-accept-encoding",
];

/// Whether a client may not set `name` in its Leading-Metadata message.
/// `name` is already lower-case, as `HeaderName` always is.
pub(crate) fn is_reserved_request_key<S: AsRef<str>>(name: &str, infrastructure: &[S]) -> bool {
    FORBIDDEN_REQUEST_HEADERS.contains(&name)
        || PROTOCOL_HEADERS.contains(&name)
        || name.starts_with("proxy-")
        || name.starts_with("sec-")
        || infrastructure
            .iter()
            .any(|pattern| matches_pattern(name, pattern.as_ref()))
}

/// Whether a name is one the protocol controls, so a client leaves it out
/// of the metadata it sends and a server out of the metadata it echoes.
pub(crate) fn is_protocol_header(name: &str) -> bool {
    PROTOCOL_HEADERS.contains(&name) || name == "accept-encoding"
}

/// Match a lower-case name against a deny-list entry, where a trailing `*`
/// matches any suffix.
fn matches_pattern(name: &str, pattern: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => {
            name.len() >= prefix.len() && name[..prefix.len()].eq_ignore_ascii_case(prefix)
        }
        None => name.eq_ignore_ascii_case(pattern),
    }
}

// ============================================================================
// Handshake helpers
// ============================================================================

/// The subprotocol token selecting the Protobuf binary codec.
pub(crate) const SUBPROTOCOL_PROTO: &str = "connectrpc.1+proto";
/// The subprotocol token selecting the Protobuf JSON codec.
pub(crate) const SUBPROTOCOL_JSON: &str = "connectrpc.1+json";

/// The query parameter that carries a client's deadline.
pub(crate) const TIMEOUT_PARAM: &str = "connect-timeout-ms";

pub(crate) fn subprotocol_token(codec: CodecFormat) -> &'static str {
    match codec {
        CodecFormat::Json => SUBPROTOCOL_JSON,
        _ => SUBPROTOCOL_PROTO,
    }
}

/// Whether the offered subprotocols name this protocol at all, any version or
/// codec. Without a prefix, only such an upgrade is Connect over WebSocket;
/// any other is left to the HTTP path (§4.2).
pub(crate) fn offers_connect_subprotocol(headers: &HeaderMap) -> bool {
    headers
        .get_all(http::header::SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|token| {
            let token = token.trim();
            token.len() >= 10 && token[..10].eq_ignore_ascii_case("connectrpc")
        })
}

/// Map an offered subprotocol header to its codec. A header offering more
/// than one token, or any token but the two Connect ones, is `None`.
pub(crate) fn codec_for_subprotocol(header: &str) -> Option<CodecFormat> {
    let mut tokens = header.split(',').map(str::trim).filter(|t| !t.is_empty());
    let token = tokens.next()?;
    if tokens.next().is_some() {
        return None;
    }
    if token.eq_ignore_ascii_case(SUBPROTOCOL_PROTO) {
        Some(CodecFormat::Proto)
    } else if token.eq_ignore_ascii_case(SUBPROTOCOL_JSON) {
        Some(CodecFormat::Json)
    } else {
        None
    }
}

/// Whether a comma-separated header contains `token`, ignoring case.
pub(crate) fn header_has_token(headers: &HeaderMap, name: &HeaderName, token: &str) -> bool {
    headers.get_all(name).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value
                .split(',')
                .any(|t| t.trim().eq_ignore_ascii_case(token))
        })
    })
}

/// Parse the `connect-timeout-ms` query parameter (§11). Absent is `Ok(None)`;
/// anything but one to ten decimal digits, or a repeated parameter, is
/// `invalid_argument`.
pub(crate) fn parse_timeout_param(query: Option<&str>) -> Result<Option<Duration>, ConnectError> {
    let mut found = None;
    for pair in query.unwrap_or("").split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key != TIMEOUT_PARAM {
            continue;
        }
        if found.is_some() {
            return Err(ConnectError::invalid_argument(format!(
                "{TIMEOUT_PARAM} appears more than once"
            )));
        }
        if value.is_empty() || value.len() > 10 || !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err(ConnectError::invalid_argument(format!(
                "invalid {TIMEOUT_PARAM} value {value:?}: want 1 to 10 decimal digits"
            )));
        }
        let millis: u64 = value.parse().map_err(|_| {
            ConnectError::invalid_argument(format!("invalid {TIMEOUT_PARAM} value {value:?}"))
        })?;
        found = Some(Duration::from_millis(millis));
    }
    Ok(found)
}

/// Whether an `Origin` names the same host and port as `Host` (§4.4). The
/// scheme is ignored and default ports are not removed.
pub(crate) fn is_same_host(origin: &str, host: &str) -> bool {
    let Some((_, authority)) = origin.split_once("://") else {
        return false;
    };
    let authority = authority.split('/').next().unwrap_or(authority);
    !authority.is_empty() && authority.eq_ignore_ascii_case(host)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(s: &str) -> Message {
        Message::Text(s.into())
    }

    fn binary(data: &'static [u8]) -> Message {
        Message::Binary(Bytes::from_static(data))
    }

    #[test]
    fn classify_accepts_each_marker_in_its_direction() {
        let m = classify(text("M{}"), Side::Server, CodecFormat::Json)
            .unwrap()
            .unwrap();
        assert_eq!(m.marker, Marker::Metadata);
        assert_eq!(&m.payload[..], b"{}");
        let c = classify(binary(b"C"), Side::Server, CodecFormat::Proto)
            .unwrap()
            .unwrap();
        assert_eq!(c.marker, Marker::ClientEnd);
        assert!(c.payload.is_empty());
        let s = classify(text("S{}"), Side::Client, CodecFormat::Proto)
            .unwrap()
            .unwrap();
        assert_eq!(s.marker, Marker::ServerEnd);
        let b = classify(binary(b"B"), Side::Client, CodecFormat::Proto)
            .unwrap()
            .unwrap();
        assert_eq!(b.marker, Marker::Body);
        assert!(b.payload.is_empty());
    }

    #[test]
    fn classify_ignores_control_messages() {
        let ping = Message::Ping(Bytes::new());
        assert!(
            classify(ping, Side::Server, CodecFormat::Proto)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn classify_rejects_markers_from_the_wrong_direction() {
        let err = classify(text("S{}"), Side::Server, CodecFormat::Json).unwrap_err();
        assert_eq!(err.fault, Fault::Protocol);
        assert_eq!(err.error.code, ErrorCode::InvalidArgument);
        let err = classify(text("C"), Side::Client, CodecFormat::Json).unwrap_err();
        assert_eq!(err.fault, Fault::Protocol);
        assert_eq!(err.error.code, ErrorCode::Internal);
    }

    #[test]
    fn classify_rejects_unknown_reserved_and_missing_markers() {
        for message in [text("X{}"), binary(b"\x80abc"), binary(b""), text("")] {
            let err = classify(message, Side::Server, CodecFormat::Proto).unwrap_err();
            assert_eq!(err.fault, Fault::Protocol);
        }
    }

    #[test]
    fn classify_checks_the_frame_type() {
        let err = classify(text("B{}"), Side::Server, CodecFormat::Proto).unwrap_err();
        assert_eq!(err.fault, Fault::FrameType);
        let err = classify(binary(b"B\x08\x01"), Side::Server, CodecFormat::Json).unwrap_err();
        assert_eq!(err.fault, Fault::FrameType);
        let err = classify(binary(b"M{}"), Side::Server, CodecFormat::Proto).unwrap_err();
        assert_eq!(err.fault, Fault::Protocol);
        let err = classify(text("C"), Side::Server, CodecFormat::Proto).unwrap_err();
        assert_eq!(err.fault, Fault::FrameType);
    }

    #[test]
    fn classify_rejects_empty_json_payloads() {
        for message in [text("B"), text("M"), text("S")] {
            let side = Side::Client;
            let err = classify(message, side, CodecFormat::Json).unwrap_err();
            assert_eq!(err.fault, Fault::Protocol);
        }
        // A bare `C` carries no payload and is valid in either codec.
        assert!(classify(text("C"), Side::Server, CodecFormat::Json).is_ok());
    }

    #[test]
    fn encode_picks_the_frame_type() {
        assert!(matches!(
            encode_body(Marker::Body, b"\x08\x01", CodecFormat::Proto),
            Ok(Message::Binary(_))
        ));
        let Ok(Message::Text(text)) = encode_body(Marker::Body, b"{}", CodecFormat::Json) else {
            panic!("expected text");
        };
        assert_eq!(text.as_str(), "B{}");
        assert!(encode_body(Marker::Body, b"\xff", CodecFormat::Json).is_err());
    }

    #[test]
    fn close_codes_follow_the_side() {
        assert_eq!(Fault::Protocol.close_code(Side::Server), 1002);
        assert_eq!(Fault::Protocol.close_code(Side::Client), 3102);
        assert_eq!(Fault::FrameType.close_code(Side::Server), 1003);
        assert_eq!(Fault::FrameType.close_code(Side::Client), 3103);
        assert_eq!(Fault::TooLarge.close_code(Side::Server), 1009);
        assert_eq!(Fault::TooLarge.close_code(Side::Client), 3109);
    }

    #[test]
    fn decode_metadata_accepts_valid_objects() {
        let entries = decode_metadata(
            br#"{"Acme-Tenant":["t-42"],"empty":[],"x-bin":["AAEC","AAECAw=="]}"#,
            Side::Server,
        )
        .unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, "acme-tenant");
        assert_eq!(entries[0].1, vec![HeaderValue::from_static("t-42")]);
        assert!(entries[1].1.is_empty());
        assert_eq!(entries[2].1.len(), 2);
    }

    #[test]
    fn decode_metadata_rejects_malformed_objects() {
        let cases: &[&[u8]] = &[
            b"",
            b"[]",
            br#"{"a":"bare"}"#,
            br#"{"a":[1]}"#,
            br#"{"a":["x"],"A":["y"]}"#,
            br#"{"a":["x"],"a":["y"]}"#,
            br#"{"bad key":["x"]}"#,
            br#"{"a":["line\nbreak"]}"#,
            br#"{"a":["nul\u0000"]}"#,
            br#"{"a-bin":["not base64!"]}"#,
            br#"{} trailing"#,
        ];
        for case in cases {
            let err = decode_metadata(case, Side::Server).unwrap_err();
            assert_eq!(
                err.fault,
                Fault::Protocol,
                "{}",
                String::from_utf8_lossy(case)
            );
        }
    }

    #[test]
    fn encode_metadata_groups_values_and_skips() {
        let mut headers = HeaderMap::new();
        headers.append("x-a", HeaderValue::from_static("1"));
        headers.append("x-a", HeaderValue::from_static("2"));
        headers.append("content-type", HeaderValue::from_static("application/json"));
        let json = encode_metadata(&headers, |name| is_protocol_header(name.as_str()));
        let value: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(value, serde_json::json!({"x-a": ["1", "2"]}));
    }

    #[test]
    fn reserved_names_cover_all_three_classes() {
        let infra: Vec<String> = DEFAULT_INFRASTRUCTURE_HEADERS
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        for name in [
            "cookie",
            "host",
            "sec-websocket-key",
            "proxy-authorization",
            "x-http-method-override",
            "forwarded",
            "x-forwarded-for",
            "x-real-ip",
            "connect-timeout-ms",
            "content-type",
        ] {
            assert!(is_reserved_request_key(name, &infra), "{name}");
        }
        for name in ["authorization", "acme-tenant", "user-agent", "x-forwarded"] {
            assert!(!is_reserved_request_key(name, &infra), "{name}");
        }
        assert!(!is_reserved_request_key::<&str>("x-real-ip", &[]));
    }

    #[test]
    fn subprotocol_requires_exactly_one_connect_token() {
        assert_eq!(
            codec_for_subprotocol("connectrpc.1+proto"),
            Some(CodecFormat::Proto)
        );
        assert_eq!(
            codec_for_subprotocol(" ConnectRPC.1+JSON "),
            Some(CodecFormat::Json)
        );
        assert_eq!(codec_for_subprotocol("connectrpc.1+thrift"), None);
        assert_eq!(
            codec_for_subprotocol("connectrpc.1+proto, connectrpc.1+json"),
            None
        );
        assert_eq!(codec_for_subprotocol(""), None);
    }

    #[test]
    fn timeout_param_parsing() {
        assert_eq!(parse_timeout_param(None).unwrap(), None);
        assert_eq!(parse_timeout_param(Some("a=b")).unwrap(), None);
        assert_eq!(
            parse_timeout_param(Some("connect-timeout-ms=1500")).unwrap(),
            Some(Duration::from_millis(1500))
        );
        assert_eq!(
            parse_timeout_param(Some("x=1&connect-timeout-ms=0")).unwrap(),
            Some(Duration::ZERO)
        );
        assert_eq!(
            parse_timeout_param(Some("connect-timeout-ms=9999999999")).unwrap(),
            Some(Duration::from_millis(9_999_999_999))
        );
        for bad in [
            "connect-timeout-ms=",
            "connect-timeout-ms=-1",
            "connect-timeout-ms=12345678901",
            "connect-timeout-ms=1.5",
            "connect-timeout-ms=1&connect-timeout-ms=2",
        ] {
            let err = parse_timeout_param(Some(bad)).unwrap_err();
            assert_eq!(err.code, ErrorCode::InvalidArgument, "{bad}");
        }
    }

    #[test]
    fn same_host_ignores_scheme_but_not_port() {
        assert!(is_same_host("https://example.com", "example.com"));
        assert!(is_same_host("http://Example.com:8080", "example.COM:8080"));
        assert!(!is_same_host("https://example.com", "example.com:443"));
        assert!(!is_same_host("https://evil.com", "example.com"));
        assert!(!is_same_host("null", "example.com"));
    }
}
