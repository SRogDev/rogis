//! Hand-rolled RESP parser/encoder (RESP2, plus the RESP3 aggregate types
//! `Map` and `Push` that the `HELLO` handshake and RESP3 pub/sub need).
//!
//! Owns the wire format for Rogis. `redis-protocol` is used only as a test
//! oracle (dev-dependency) — never in production code.

/// A decoded RESP frame (RESP2 plus the RESP3 aggregate types we need:
///
/// - `Map` (`%`): key/value pairs, used by the `HELLO` reply.
/// - `Push` (`>`): out-of-band data, used for pub/sub frames in RESP3 mode.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// `+OK\r\n`
    Simple(String),
    /// `-ERR msg\r\n` — the `String` holds the text *after* the leading `-`.
    Error(String),
    /// `:1000\r\n`
    Integer(i64),
    /// Bulk string; `None` is the null bulk string (`$-1\r\n` on RESP2,
    /// `_\r\n` on RESP3 — see [`encode_with_proto`]).
    Bulk(Option<Vec<u8>>),
    /// Array; `None` is the null array (`*-1\r\n` on RESP2, `_\r\n` on RESP3).
    Array(Option<Vec<Frame>>),
    /// Map of key/value pairs, encoded as `%<n>\r\n` followed by the pairs.
    Map(Vec<(Frame, Frame)>),
    /// Push (out-of-band) data, encoded as `><n>\r\n` followed by the items.
    Push(Vec<Frame>),
}

/// Decode failure modes.
#[derive(Debug, PartialEq)]
pub enum RespError {
    /// The buffer holds a strict prefix of a valid frame — feed more bytes.
    Incomplete,
    /// The buffer cannot become a valid frame no matter what follows.
    Invalid(String),
}

/// Decode one frame from the front of `buf`.
///
/// Returns the frame plus the number of bytes consumed, so pipelined input
/// can be decoded by advancing the offset. Returns [`RespError::Incomplete`]
/// on truncated input. Never panics.
pub fn decode(buf: &[u8]) -> Result<(Frame, usize), RespError> {
    decode_frame(buf, 0, 0)
}

/// Append the wire encoding of `frame` to `out` (RESP2 shapes).
///
/// This is the proto-2 path: nils encode as `$-1\r\n` / `*-1\r\n`. Frames
/// written to a RESP3 connection must go through [`encode_with_proto`].
pub fn encode(frame: &Frame, out: &mut Vec<u8>) {
    encode_inner(frame, out, false);
}

/// Append the wire encoding of `frame` to `out`, choosing the null shape by
/// protocol version: RESP2 (`proto == 2`) uses `$-1\r\n` / `*-1\r\n`; RESP3
/// (`proto == 3`) uses the null type `_\r\n`. Any other `proto` value
/// behaves like RESP2.
///
/// RESP3 clients (notably redis-py 8.x, which speaks RESP3 by default)
/// block forever on `$-1` — their parser waits for the declared payload
/// bytes — so every frame written to a RESP3 connection must go through
/// this function, never [`encode`].
pub fn encode_with_proto(frame: &Frame, out: &mut Vec<u8>, proto: u8) {
    encode_inner(frame, out, proto == 3);
}

fn encode_inner(frame: &Frame, out: &mut Vec<u8>, resp3: bool) {
    match frame {
        Frame::Simple(s) => {
            out.push(b'+');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Frame::Error(s) => {
            out.push(b'-');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Frame::Integer(i) => {
            out.push(b':');
            out.extend_from_slice(i.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Frame::Bulk(None) => {
            if resp3 {
                out.extend_from_slice(b"_\r\n");
            } else {
                out.extend_from_slice(b"$-1\r\n");
            }
        }
        Frame::Bulk(Some(bytes)) => {
            out.push(b'$');
            out.extend_from_slice(bytes.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(bytes);
            out.extend_from_slice(b"\r\n");
        }
        Frame::Array(None) => {
            if resp3 {
                out.extend_from_slice(b"_\r\n");
            } else {
                out.extend_from_slice(b"*-1\r\n");
            }
        }
        Frame::Array(Some(frames)) => {
            out.push(b'*');
            out.extend_from_slice(frames.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for f in frames {
                encode_inner(f, out, resp3);
            }
        }
        Frame::Map(pairs) => {
            out.push(b'%');
            out.extend_from_slice(pairs.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for (k, v) in pairs {
                encode_inner(k, out, resp3);
                encode_inner(v, out, resp3);
            }
        }
        Frame::Push(frames) => {
            out.push(b'>');
            out.extend_from_slice(frames.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for f in frames {
                encode_inner(f, out, resp3);
            }
        }
    }
}

/// Sanity cap on bulk-string length (matches Redis' `proto-max-bulk-len`).
/// Anything larger is treated as invalid rather than "still arriving".
const MAX_BULK_LEN: i64 = 512 * 1024 * 1024;

/// Sanity cap on aggregate element count (arrays, maps, pushes); real
/// commands never approach it.
const MAX_ARRAY_LEN: i64 = 1_048_576;

/// Maximum nesting depth for arrays; deeper input is invalid, not a stack overflow.
const MAX_DEPTH: usize = 128;

fn decode_frame(buf: &[u8], pos: usize, depth: usize) -> Result<(Frame, usize), RespError> {
    if depth > MAX_DEPTH {
        return Err(RespError::Invalid(
            "array nesting exceeds limit".to_string(),
        ));
    }
    let tag = *buf.get(pos).ok_or(RespError::Incomplete)?;
    match tag {
        b'+' => {
            let (line, next) = read_line(buf, pos + 1)?;
            Ok((Frame::Simple(read_text(line, "simple string")?), next))
        }
        b'-' => {
            let (line, next) = read_line(buf, pos + 1)?;
            Ok((Frame::Error(read_text(line, "error string")?), next))
        }
        b':' => {
            let (line, next) = read_line(buf, pos + 1)?;
            Ok((Frame::Integer(parse_int(line)?), next))
        }
        b'$' => decode_bulk(buf, pos),
        b'*' => decode_array(buf, pos, depth),
        b'%' => decode_map(buf, pos, depth),
        b'>' => decode_push(buf, pos, depth),
        b'_' => decode_null(buf, pos),
        _ => Err(RespError::Invalid(format!(
            "unknown RESP type byte: 0x{tag:02x}"
        ))),
    }
}

/// Read a `\r\n`-terminated line starting at `pos`; return the line bytes
/// (without CRLF) and the offset just past the CRLF.
fn read_line(buf: &[u8], pos: usize) -> Result<(&[u8], usize), RespError> {
    let rest = buf.get(pos..).ok_or(RespError::Incomplete)?;
    let nl = rest
        .iter()
        .position(|&b| b == b'\n')
        .ok_or(RespError::Incomplete)?;
    if nl == 0 || rest[nl - 1] != b'\r' {
        return Err(RespError::Invalid("expected CRLF line ending".to_string()));
    }
    Ok((&rest[..nl - 1], pos + nl + 1))
}

/// Parse a non-empty ASCII decimal integer with optional leading `-`.
fn parse_int(line: &[u8]) -> Result<i64, RespError> {
    let invalid = || RespError::Invalid(format!("invalid integer: {line:?}"));
    let (negative, digits) = match line.first() {
        Some(b'-') => (true, &line[1..]),
        _ => (false, line),
    };
    if digits.is_empty() || !digits.iter().all(|b| b.is_ascii_digit()) {
        return Err(invalid());
    }
    // Accumulate the magnitude as u64 so that i64::MIN (-9223372036854775808)
    // — whose magnitude does not fit in i64 — still parses.
    let mut magnitude: u64 = 0;
    for &d in digits {
        magnitude = magnitude
            .checked_mul(10)
            .and_then(|v| v.checked_add((d - b'0') as u64))
            .ok_or_else(invalid)?;
    }
    const I64_MAX_AS_U64: u64 = i64::MAX as u64;
    if negative {
        if magnitude == I64_MAX_AS_U64 + 1 {
            Ok(i64::MIN)
        } else if magnitude <= I64_MAX_AS_U64 {
            Ok(-(magnitude as i64))
        } else {
            Err(invalid())
        }
    } else if magnitude <= I64_MAX_AS_U64 {
        Ok(magnitude as i64)
    } else {
        Err(invalid())
    }
}

/// Decode UTF-8 text for simple/error strings (RESP defines them as text).
fn read_text(line: &[u8], what: &str) -> Result<String, RespError> {
    std::str::from_utf8(line)
        .map(str::to_owned)
        .map_err(|_| RespError::Invalid(format!("invalid UTF-8 in {what}")))
}

fn decode_bulk(buf: &[u8], pos: usize) -> Result<(Frame, usize), RespError> {
    let (line, mut p) = read_line(buf, pos + 1)?;
    let len = parse_int(line)?;
    if len == -1 {
        return Ok((Frame::Bulk(None), p));
    }
    if len < -1 {
        return Err(RespError::Invalid(format!("invalid bulk length: {len}")));
    }
    if len > MAX_BULK_LEN {
        return Err(RespError::Invalid(format!(
            "bulk length {len} exceeds limit"
        )));
    }
    let len = len as usize;
    // Need `len` payload bytes plus the trailing CRLF before deciding.
    let remaining = buf.len() - p;
    if remaining < len.saturating_add(2) {
        return Err(RespError::Incomplete);
    }
    let end = p
        .checked_add(len)
        .ok_or_else(|| RespError::Invalid(format!("bulk length {len} overflows buffer offset")))?;
    if buf[end] != b'\r' || buf[end + 1] != b'\n' {
        return Err(RespError::Invalid(
            "bulk string not followed by CRLF".to_string(),
        ));
    }
    let data = buf[p..end].to_vec();
    p = end + 2;
    Ok((Frame::Bulk(Some(data)), p))
}

fn decode_array(buf: &[u8], pos: usize, depth: usize) -> Result<(Frame, usize), RespError> {
    let (mut p, count) = read_count(buf, pos, "array")?;
    if count == -1 {
        return Ok((Frame::Array(None), p));
    }
    let mut frames = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (frame, next) = decode_frame(buf, p, depth + 1)?;
        frames.push(frame);
        p = next;
    }
    Ok((Frame::Array(Some(frames)), p))
}

/// Decode a RESP3 map (`%<n>\r\n` + n key/value pairs).
fn decode_map(buf: &[u8], pos: usize, depth: usize) -> Result<(Frame, usize), RespError> {
    let (mut p, count) = read_count(buf, pos, "map")?;
    if count == -1 {
        // Our model has no null-map shape; a null map on the wire is a
        // protocol error here rather than a silent coercion.
        return Err(RespError::Invalid(
            "null map (%-1) not supported".to_string(),
        ));
    }
    let mut pairs = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (key, next) = decode_frame(buf, p, depth + 1)?;
        let (value, next) = decode_frame(buf, next, depth + 1)?;
        pairs.push((key, value));
        p = next;
    }
    Ok((Frame::Map(pairs), p))
}

/// Decode a RESP3 push (`><n>\r\n` + n items).
fn decode_push(buf: &[u8], pos: usize, depth: usize) -> Result<(Frame, usize), RespError> {
    let (mut p, count) = read_count(buf, pos, "push")?;
    if count == -1 {
        return Err(RespError::Invalid(
            "null push (>-1) not supported".to_string(),
        ));
    }
    let mut frames = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (frame, next) = decode_frame(buf, p, depth + 1)?;
        frames.push(frame);
        p = next;
    }
    Ok((Frame::Push(frames), p))
}

/// Decode a RESP3 null (`_\r\n`). The null type carries no payload; it
/// decodes as [`Frame::Bulk(None)`] — null is null, regardless of which
/// shape the encoder would have emitted for this protocol version.
fn decode_null(buf: &[u8], pos: usize) -> Result<(Frame, usize), RespError> {
    let (line, next) = read_line(buf, pos + 1)?;
    if !line.is_empty() {
        return Err(RespError::Invalid("null type takes no payload".to_string()));
    }
    Ok((Frame::Bulk(None), next))
}

/// Read a `<count>\r\n` line after an aggregate type byte; returns the new
/// offset and the count. Negative counts other than `-1` and counts over the
/// sanity cap are invalid.
fn read_count(buf: &[u8], pos: usize, what: &str) -> Result<(usize, i64), RespError> {
    let (line, p) = read_line(buf, pos + 1)?;
    let count = parse_int(line)?;
    if count < -1 {
        return Err(RespError::Invalid(format!(
            "invalid {what} length: {count}"
        )));
    }
    if count > MAX_ARRAY_LEN {
        return Err(RespError::Invalid(format!(
            "{what} length {count} exceeds limit"
        )));
    }
    Ok((p, count))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode then decode; assert the frame survives and all bytes are consumed.
    fn roundtrip(frame: &Frame) -> Vec<u8> {
        let mut out = Vec::new();
        encode(frame, &mut out);
        let (decoded, consumed) = decode(&out).expect("roundtrip must decode");
        assert_eq!(&decoded, frame, "frame must survive encode->decode");
        assert_eq!(consumed, out.len(), "decode must consume exactly the frame");
        out
    }

    #[test]
    fn simple_roundtrip() {
        let wire = roundtrip(&Frame::Simple("OK".into()));
        assert_eq!(wire, b"+OK\r\n");
    }

    #[test]
    fn simple_empty() {
        let wire = roundtrip(&Frame::Simple(String::new()));
        assert_eq!(wire, b"+\r\n");
    }

    #[test]
    fn error_roundtrip() {
        let wire = roundtrip(&Frame::Error("ERR unknown command".into()));
        assert_eq!(wire, b"-ERR unknown command\r\n");
    }

    #[test]
    fn integer_roundtrip() {
        for i in [0, 1, -1, 42, -42, 1000, i64::MAX, i64::MIN] {
            let wire = roundtrip(&Frame::Integer(i));
            assert_eq!(wire, format!(":{i}\r\n").into_bytes());
        }
    }

    #[test]
    fn bulk_roundtrip() {
        let wire = roundtrip(&Frame::Bulk(Some(b"hello".into())));
        assert_eq!(wire, b"$5\r\nhello\r\n");
    }

    #[test]
    fn bulk_empty_is_not_null() {
        let wire = roundtrip(&Frame::Bulk(Some(Vec::new())));
        assert_eq!(wire, b"$0\r\n\r\n");
    }

    #[test]
    fn bulk_binary_safe() {
        // Payload containing CRLF and non-UTF8 bytes must survive verbatim.
        let payload = vec![0x00, 0xff, b'\r', b'\n', 0x01, b'a'];
        let wire = roundtrip(&Frame::Bulk(Some(payload.clone())));
        let mut expected = b"$6\r\n".to_vec();
        expected.extend_from_slice(&payload);
        expected.extend_from_slice(b"\r\n");
        assert_eq!(wire, expected);
    }

    #[test]
    fn bulk_nil() {
        let wire = roundtrip(&Frame::Bulk(None));
        assert_eq!(wire, b"$-1\r\n");
    }

    // --- Proto-aware encoding (RESP3) -------------------------------------

    #[test]
    fn proto3_nil_bulk_encodes_as_null() {
        let mut out = Vec::new();
        encode_with_proto(&Frame::Bulk(None), &mut out, 3);
        assert_eq!(out, b"_\r\n");
    }

    #[test]
    fn proto3_nil_array_encodes_as_null() {
        let mut out = Vec::new();
        encode_with_proto(&Frame::Array(None), &mut out, 3);
        assert_eq!(out, b"_\r\n");
    }

    #[test]
    fn proto2_nil_shapes_are_unchanged_by_encode_with_proto() {
        let mut out = Vec::new();
        encode_with_proto(&Frame::Bulk(None), &mut out, 2);
        assert_eq!(out, b"$-1\r\n");
        out.clear();
        encode_with_proto(&Frame::Array(None), &mut out, 2);
        assert_eq!(out, b"*-1\r\n");
    }

    #[test]
    fn proto3_applies_recursively_to_nested_nils() {
        let frame = Frame::Array(Some(vec![
            Frame::Bulk(None),
            Frame::Bulk(Some(b"a".to_vec())),
        ]));
        let mut out3 = Vec::new();
        encode_with_proto(&frame, &mut out3, 3);
        assert_eq!(out3, b"*2\r\n_\r\n$1\r\na\r\n");
        // The plain encode() path stays RESP2, untouched.
        let mut out2 = Vec::new();
        encode(&frame, &mut out2);
        assert_eq!(out2, b"*2\r\n$-1\r\n$1\r\na\r\n");
    }

    #[test]
    fn decode_null_type_byte() {
        assert_eq!(decode(b"_\r\n"), Ok((Frame::Bulk(None), 3)));
        // Strict prefixes are Incomplete, never Invalid.
        assert_eq!(decode(b"_"), Err(RespError::Incomplete));
        assert_eq!(decode(b"_\r"), Err(RespError::Incomplete));
        // Lone-LF is a protocol error, like every other frame type.
        assert!(matches!(decode(b"_\n"), Err(RespError::Invalid(_))));
    }

    #[test]
    fn array_roundtrip() {
        let frame = Frame::Array(Some(vec![
            Frame::Bulk(Some(b"SET".into())),
            Frame::Bulk(Some(b"key".into())),
            Frame::Integer(7),
        ]));
        let wire = roundtrip(&frame);
        assert_eq!(wire, b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n:7\r\n");
    }

    #[test]
    fn array_empty() {
        let wire = roundtrip(&Frame::Array(Some(vec![])));
        assert_eq!(wire, b"*0\r\n");
    }

    #[test]
    fn array_nil() {
        let wire = roundtrip(&Frame::Array(None));
        assert_eq!(wire, b"*-1\r\n");
    }

    #[test]
    fn array_nested() {
        let frame = Frame::Array(Some(vec![
            Frame::Array(Some(vec![Frame::Integer(1), Frame::Integer(2)])),
            Frame::Simple("OK".into()),
            Frame::Array(None),
        ]));
        roundtrip(&frame);
    }

    #[test]
    fn every_strict_prefix_is_incomplete() {
        // A strict parser must never call a prefix of a *valid* frame Invalid.
        let frames: Vec<Vec<u8>> = vec![
            b"+OK\r\n".to_vec(),
            b"-ERR bad\r\n".to_vec(),
            b":12345\r\n".to_vec(),
            b"$11\r\nhello world\r\n".to_vec(),
            b"$-1\r\n".to_vec(),
            b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n".to_vec(),
            b"*0\r\n".to_vec(),
            b"%2\r\n$6\r\nserver\r\n$5\r\nrogis\r\n$5\r\nproto\r\n:3\r\n".to_vec(),
            b">3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n".to_vec(),
            b"%0\r\n".to_vec(),
            b">0\r\n".to_vec(),
        ];
        for wire in &frames {
            for prefix_len in 0..wire.len() {
                let prefix = &wire[..prefix_len];
                assert_eq!(
                    decode(prefix),
                    Err(RespError::Incomplete),
                    "prefix {prefix_len} of {wire:?} must be Incomplete"
                );
            }
            // The full wire must decode cleanly.
            assert!(decode(wire).is_ok(), "full frame {wire:?} must decode");
        }
    }

    #[test]
    fn invalid_inputs_are_errors_not_panics() {
        let cases: &[&[u8]] = &[
            b"*xyz\r\n",                     // non-numeric array length
            b"$-2\r\n",                      // negative bulk length other than -1
            b"*-2\r\n",                      // negative array length other than -1
            b"?hello\r\n",                   // unknown type byte
            b"\r\n",                         // unknown type byte (CR)
            b"+OK\n",                        // lone LF instead of CRLF
            b"+\n",                          // lone LF, empty simple string
            b":\r\n",                        // empty integer
            b":12x\r\n",                     // non-numeric integer
            b":+42\r\n",                     // explicit plus not allowed
            b"$x\r\n",                       // non-numeric bulk length
            b":99999999999999999999999\r\n", // integer overflow
            b"$3\r\nabcXX",                  // bulk payload not followed by CRLF
            b"$3\r\nabcd\r\n",               // 4 payload bytes where 3 declared, then CRLF
            b"$536870913\r\n",               // 512MiB+1: over the bulk sanity cap
            b"$999999999999\r\n",            // huge declared length, tiny buffer
            b"*1048577\r\n",                 // over the array-count sanity cap
            b"%x\r\n",                       // non-numeric map length
            b">x\r\n",                       // non-numeric push length
            b"%-1\r\n",                      // no null maps: negative length
            b">-1\r\n",                      // no null pushes: negative length
            b"%1048577\r\n",                 // over the map-count sanity cap
            b">1048577\r\n",                 // over the push-count sanity cap
            b"+\xff\r\n",                    // invalid UTF-8 in simple string
            b"-\xff\xfe\r\n",                // invalid UTF-8 in error string
            b"hello",                        // inline garbage (no type byte)
        ];
        for input in cases {
            match decode(input) {
                Err(RespError::Invalid(_)) => {}
                other => panic!("{input:?} must be Invalid, got {other:?}"),
            }
        }
    }

    #[test]
    fn pipelined_decodes_sequentially() {
        let mut buf = Vec::new();
        encode(&Frame::Simple("OK".into()), &mut buf);
        encode(&Frame::Integer(42), &mut buf);
        encode(&Frame::Bulk(Some(b"PING".into())), &mut buf);

        let mut pos = 0;
        let (f1, n1) = decode(&buf[pos..]).unwrap();
        pos += n1;
        let (f2, n2) = decode(&buf[pos..]).unwrap();
        pos += n2;
        let (f3, n3) = decode(&buf[pos..]).unwrap();
        pos += n3;

        assert_eq!(f1, Frame::Simple("OK".into()));
        assert_eq!(f2, Frame::Integer(42));
        assert_eq!(f3, Frame::Bulk(Some(b"PING".into())));
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn deep_nesting_is_rejected_not_a_stack_overflow() {
        let mut buf = Vec::new();
        for _ in 0..200 {
            buf.extend_from_slice(b"*1\r\n");
        }
        buf.extend_from_slice(b":1\r\n");
        match decode(&buf) {
            Err(RespError::Invalid(_)) => {}
            other => panic!("200-deep nesting must be Invalid, got {other:?}"),
        }
    }

    /// Deterministic xorshift32 — no extra dev-deps for the fuzz-ish test.
    fn xorshift(state: &mut u32) -> u32 {
        *state ^= *state << 13;
        *state ^= *state >> 17;
        *state ^= *state << 5;
        *state
    }

    #[test]
    fn fuzz_garbage_never_panics() {
        let mut state = 0x1234_5678u32;
        let alphabet: &[u8] = b"+-:$*\r\n0123456789abcdef\xff\x00";
        for _ in 0..20_000 {
            let len = (xorshift(&mut state) % 48) as usize;
            let mut input = Vec::with_capacity(len);
            for _ in 0..len {
                input.push(alphabet[(xorshift(&mut state) as usize) % alphabet.len()]);
            }
            // Must return Ok or Err — a panic fails the test.
            let _ = decode(&input);
        }
    }

    #[test]
    fn map_roundtrip_and_wire_shape() {
        let frame = Frame::Map(vec![
            (
                Frame::Bulk(Some(b"server".to_vec())),
                Frame::Bulk(Some(b"rogis".to_vec())),
            ),
            (Frame::Bulk(Some(b"proto".to_vec())), Frame::Integer(3)),
        ]);
        let wire = roundtrip(&frame);
        assert_eq!(
            wire,
            b"%2\r\n$6\r\nserver\r\n$5\r\nrogis\r\n$5\r\nproto\r\n:3\r\n"
        );
    }

    #[test]
    fn map_empty() {
        let wire = roundtrip(&Frame::Map(vec![]));
        assert_eq!(wire, b"%0\r\n");
    }

    #[test]
    fn map_nested_in_array() {
        let frame = Frame::Array(Some(vec![Frame::Map(vec![(
            Frame::Bulk(Some(b"k".to_vec())),
            Frame::Integer(1),
        )])]));
        roundtrip(&frame);
    }

    #[test]
    fn push_roundtrip_and_wire_shape() {
        let frame = Frame::Push(vec![
            Frame::Bulk(Some(b"subscribe".to_vec())),
            Frame::Bulk(Some(b"news".to_vec())),
            Frame::Integer(1),
        ]);
        let wire = roundtrip(&frame);
        assert_eq!(wire, b">3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n");
    }

    #[test]
    fn push_empty() {
        let wire = roundtrip(&Frame::Push(vec![]));
        assert_eq!(wire, b">0\r\n");
    }

    #[test]
    fn map_deep_nesting_is_rejected() {
        let mut buf = Vec::new();
        for _ in 0..200 {
            buf.extend_from_slice(b"%1\r\n");
        }
        buf.extend_from_slice(b":1\r\n");
        match decode(&buf) {
            Err(RespError::Invalid(_)) => {}
            other => panic!("200-deep map nesting must be Invalid, got {other:?}"),
        }
    }

    #[test]
    fn push_deep_nesting_is_rejected() {
        let mut buf = Vec::new();
        for _ in 0..200 {
            buf.extend_from_slice(b">1\r\n");
        }
        buf.extend_from_slice(b":1\r\n");
        match decode(&buf) {
            Err(RespError::Invalid(_)) => {}
            other => panic!("200-deep push nesting must be Invalid, got {other:?}"),
        }
    }

    #[test]
    fn fuzz_mutated_valid_frames_never_panic() {
        let base = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n".to_vec();
        let mut state = 0x9e37_79b9u32;
        for _ in 0..20_000 {
            let mut input = base.clone();
            let mutations = (xorshift(&mut state) % 4) as usize;
            for _ in 0..mutations {
                if input.is_empty() {
                    break;
                }
                let idx = (xorshift(&mut state) as usize) % input.len();
                match xorshift(&mut state) % 3 {
                    0 => input[idx] = (xorshift(&mut state) % 256) as u8,
                    1 => {
                        input.remove(idx);
                    }
                    _ => input.truncate(idx),
                }
            }
            let _ = decode(&input);
        }
    }
}
