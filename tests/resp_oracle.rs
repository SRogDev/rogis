//! Cross-check `rogis::resp` against the `redis-protocol` crate.
//!
//! `redis-protocol` is a dev-dependency used ONLY as a test oracle — it never
//! appears in production code (`src/`).
//!
//! Direction (a): encode with OUR encoder, decode with the ORACLE.
//! Direction (b): encode with the ORACLE, decode with OUR decoder.
//! Both directions must agree on every covered frame shape.

use redis_protocol::resp2::decode::decode as oracle_decode;
use redis_protocol::resp2::encode::encode as oracle_encode;
use redis_protocol::resp2::types::OwnedFrame;
use redis_protocol::resp3::decode::complete::decode as oracle3_decode;
use redis_protocol::resp3::encode::complete::encode as oracle3_encode;
use redis_protocol::resp3::types::{FrameMap, OwnedFrame as Owned3};
use rogis::resp::{decode as our_decode, encode as our_encode, Frame};

/// Map our frame onto the oracle's owned frame for semantic comparison.
fn to_owned(frame: &Frame) -> OwnedFrame {
    match frame {
        Frame::Simple(s) => OwnedFrame::SimpleString(s.as_bytes().to_vec()),
        Frame::Error(s) => OwnedFrame::Error(s.clone()),
        Frame::Integer(i) => OwnedFrame::Integer(*i),
        Frame::Bulk(None) => OwnedFrame::Null,
        Frame::Bulk(Some(b)) => OwnedFrame::BulkString(b.clone()),
        Frame::Array(None) => OwnedFrame::Null,
        Frame::Array(Some(frames)) => OwnedFrame::Array(frames.iter().map(to_owned).collect()),
        // Map/Push are RESP3-only: they never appear in the RESP2 `cases()`.
        Frame::Map(_) | Frame::Push(_) => panic!("RESP3-only frame in RESP2 oracle"),
    }
}

fn cases() -> Vec<Frame> {
    vec![
        Frame::Simple("OK".into()),
        Frame::Simple(String::new()),
        Frame::Error("ERR unknown command".into()),
        Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
        Frame::Integer(0),
        Frame::Integer(-1),
        Frame::Integer(1000),
        Frame::Integer(-1000),
        Frame::Integer(i64::MAX),
        Frame::Integer(i64::MIN),
        Frame::Bulk(None),
        Frame::Bulk(Some(Vec::new())),
        Frame::Bulk(Some(b"hello".to_vec())),
        // Binary payload containing CRLF — must survive verbatim.
        Frame::Bulk(Some(vec![0x00, 0xff, b'\r', b'\n', 0x01, b'z'])),
        Frame::Array(None),
        Frame::Array(Some(vec![])),
        Frame::Array(Some(vec![
            Frame::Bulk(Some(b"SET".into())),
            Frame::Bulk(Some(b"key".into())),
            Frame::Bulk(Some(b"value".into())),
        ])),
        Frame::Array(Some(vec![
            Frame::Integer(1),
            Frame::Simple("OK".into()),
            Frame::Error("ERR boom".into()),
            Frame::Bulk(None),
            Frame::Array(Some(vec![
                Frame::Integer(2),
                Frame::Bulk(Some(b"x".into())),
            ])),
            Frame::Array(None),
        ])),
    ]
}

/// (a) Our encoder -> oracle decoder: semantic equality + full consumption.
#[test]
fn oracle_decodes_our_encoding() {
    for frame in cases() {
        let mut wire = Vec::new();
        our_encode(&frame, &mut wire);
        let (owned, consumed) = oracle_decode(&wire)
            .expect("oracle decode must not error")
            .expect("oracle decode must not report incomplete on our output");
        assert_eq!(
            consumed,
            wire.len(),
            "oracle must consume all bytes: {frame:?}"
        );
        assert_eq!(owned, to_owned(&frame), "semantic mismatch: {frame:?}");
    }
}

/// RESP3 map/push cross-checks (direction a + b).
///
/// Note: on the wire, RESP3 `$` is a blob string, so our `Frame::Bulk`
/// encodes to the oracle's `BlobString` and our `Frame::Integer` to its
/// `Number`. Map key order is not guaranteed by the oracle (`FrameMap` is a
/// `HashMap` without the `index-map` feature), so maps compare as key-sorted
/// pairs, not as ordered lists.
///
/// Semantic projection of our frame onto the oracle's RESP3 owned frame,
/// ignoring attributes and map ordering.
fn to_owned3(frame: &Frame) -> Owned3 {
    match frame {
        Frame::Simple(s) => Owned3::SimpleString {
            data: s.as_bytes().to_vec(),
            attributes: None,
        },
        Frame::Error(s) => Owned3::SimpleError {
            data: s.clone(),
            attributes: None,
        },
        Frame::Integer(i) => Owned3::Number {
            data: *i,
            attributes: None,
        },
        Frame::Bulk(None) => Owned3::Null,
        Frame::Bulk(Some(b)) => Owned3::BlobString {
            data: b.clone(),
            attributes: None,
        },
        Frame::Array(None) => Owned3::Null,
        Frame::Array(Some(frames)) => Owned3::Array {
            data: frames.iter().map(to_owned3).collect(),
            attributes: None,
        },
        Frame::Map(pairs) => Owned3::Map {
            data: pairs
                .iter()
                .map(|(k, v)| (to_owned3(k), to_owned3(v)))
                .collect(),
            attributes: None,
        },
        Frame::Push(frames) => Owned3::Push {
            data: frames.iter().map(to_owned3).collect(),
            attributes: None,
        },
    }
}

/// Flatten an oracle RESP3 map into key-sorted (key_bytes, value) pairs so
/// ordering differences can't flake the comparison.
fn sorted_map_pairs(map: &Owned3) -> Vec<(Vec<u8>, Owned3)> {
    let Owned3::Map { data, .. } = map else {
        panic!("expected a Map, got {map:?}");
    };
    let mut pairs: Vec<(Vec<u8>, Owned3)> = data
        .iter()
        .map(|(k, v)| match k {
            Owned3::BlobString { data, .. } => (data.clone(), v.clone()),
            other => panic!("map key must be a blob string, got {other:?}"),
        })
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    pairs
}

fn hello_style_map() -> Frame {
    Frame::Map(vec![
        (
            Frame::Bulk(Some(b"server".to_vec())),
            Frame::Bulk(Some(b"rogis".to_vec())),
        ),
        (
            Frame::Bulk(Some(b"version".to_vec())),
            Frame::Bulk(Some(b"0.1.0".to_vec())),
        ),
        (Frame::Bulk(Some(b"proto".to_vec())), Frame::Integer(3)),
        (
            Frame::Bulk(Some(b"modules".to_vec())),
            Frame::Array(Some(vec![])),
        ),
    ])
}

/// (a3) Our encoder -> oracle RESP3 decoder: semantic equality + full consumption.
#[test]
fn oracle_resp3_decodes_our_map_and_push() {
    let push = Frame::Push(vec![
        Frame::Bulk(Some(b"message".to_vec())),
        Frame::Bulk(Some(b"news".to_vec())),
        Frame::Bulk(Some(b"hello".to_vec())),
    ]);
    for frame in [hello_style_map(), push] {
        let mut wire = Vec::new();
        our_encode(&frame, &mut wire);
        let (owned, consumed) = oracle3_decode(&wire)
            .expect("oracle decode must not error")
            .expect("oracle decode must not report incomplete on our output");
        assert_eq!(consumed, wire.len(), "oracle must consume all bytes");
        match (&owned, &frame) {
            (Owned3::Push { data, .. }, Frame::Push(items)) => {
                let expected: Vec<Owned3> = items.iter().map(to_owned3).collect();
                assert_eq!(*data, expected, "push semantic mismatch");
            }
            (map @ Owned3::Map { .. }, Frame::Map(pairs)) => {
                let mut expected: Vec<(Vec<u8>, Owned3)> = pairs
                    .iter()
                    .map(|(k, v)| match k {
                        Frame::Bulk(Some(b)) => (b.clone(), to_owned3(v)),
                        other => panic!("map key must be bulk, got {other:?}"),
                    })
                    .collect();
                expected.sort_by(|a, b| a.0.cmp(&b.0));
                assert_eq!(sorted_map_pairs(map), expected, "map semantic mismatch");
            }
            _ => panic!("shape mismatch: oracle={owned:?} ours={frame:?}"),
        }
    }
}

/// (b3) Oracle RESP3 encoder -> our decoder: frame equality + full consumption.
///
/// Maps compare as key-sorted pairs because the oracle's `FrameMap` does not
/// preserve insertion order.
#[test]
fn we_decode_oracle_resp3_map_and_push() {
    let owned_map = Owned3::Map {
        data: FrameMap::from([
            (
                Owned3::BlobString {
                    data: b"server".to_vec(),
                    attributes: None,
                },
                Owned3::BlobString {
                    data: b"rogis".to_vec(),
                    attributes: None,
                },
            ),
            (
                Owned3::BlobString {
                    data: b"proto".to_vec(),
                    attributes: None,
                },
                Owned3::Number {
                    data: 3,
                    attributes: None,
                },
            ),
        ]),
        attributes: None,
    };
    let owned_push = Owned3::Push {
        data: vec![
            Owned3::BlobString {
                data: b"subscribe".to_vec(),
                attributes: None,
            },
            Owned3::BlobString {
                data: b"news".to_vec(),
                attributes: None,
            },
            Owned3::Number {
                data: 1,
                attributes: None,
            },
        ],
        attributes: None,
    };
    for owned in [owned_map, owned_push] {
        let mut buf = vec![0u8; 4096];
        let n = oracle3_encode(&mut buf, &owned, false).expect("oracle encode must not error");
        let wire = &buf[..n];
        let (decoded, consumed) = our_decode(wire).expect("our decode must accept oracle output");
        assert_eq!(consumed, wire.len(), "we must consume all bytes");
        match (&decoded, &owned) {
            (Frame::Push(items), Owned3::Push { data, .. }) => {
                let expected: Vec<Owned3> = items.iter().map(to_owned3).collect();
                assert_eq!(*data, expected, "push semantic mismatch");
            }
            (Frame::Map(pairs), map @ Owned3::Map { .. }) => {
                let mut ours: Vec<(Vec<u8>, Owned3)> = pairs
                    .iter()
                    .map(|(k, v)| match k {
                        Frame::Bulk(Some(b)) => (b.clone(), to_owned3(v)),
                        other => panic!("map key must be bulk, got {other:?}"),
                    })
                    .collect();
                ours.sort_by(|a, b| a.0.cmp(&b.0));
                assert_eq!(sorted_map_pairs(map), ours, "map semantic mismatch");
            }
            _ => panic!("shape mismatch: ours={decoded:?} oracle={owned:?}"),
        }
    }
}
/// (b) Oracle encoder -> our decoder: frame equality + full consumption.
///
/// Note: the oracle's `OwnedFrame` collapses both null shapes into `Null`,
/// which it encodes as `$-1\r\n`. So here the oracle side is built natively
/// and `Null` is expected to decode as `Frame::Bulk(None)` on our side.
#[test]
fn we_decode_oracle_encoding() {
    let pairs: Vec<(OwnedFrame, Frame)> = vec![
        (
            OwnedFrame::SimpleString(b"OK".to_vec()),
            Frame::Simple("OK".into()),
        ),
        (
            OwnedFrame::SimpleString(Vec::new()),
            Frame::Simple(String::new()),
        ),
        (
            OwnedFrame::Error("ERR unknown command".into()),
            Frame::Error("ERR unknown command".into()),
        ),
        (OwnedFrame::Integer(0), Frame::Integer(0)),
        (OwnedFrame::Integer(-1), Frame::Integer(-1)),
        (OwnedFrame::Integer(i64::MAX), Frame::Integer(i64::MAX)),
        (OwnedFrame::Integer(i64::MIN), Frame::Integer(i64::MIN)),
        // The oracle's Null encodes as $-1\r\n, i.e. our Bulk(None).
        (OwnedFrame::Null, Frame::Bulk(None)),
        (
            OwnedFrame::BulkString(Vec::new()),
            Frame::Bulk(Some(Vec::new())),
        ),
        (
            OwnedFrame::BulkString(b"hello".to_vec()),
            Frame::Bulk(Some(b"hello".to_vec())),
        ),
        (
            OwnedFrame::BulkString(vec![0x00, 0xff, b'\r', b'\n', 0x01, b'z']),
            Frame::Bulk(Some(vec![0x00, 0xff, b'\r', b'\n', 0x01, b'z'])),
        ),
        (OwnedFrame::Array(vec![]), Frame::Array(Some(vec![]))),
        (
            OwnedFrame::Array(vec![
                OwnedFrame::BulkString(b"SET".to_vec()),
                OwnedFrame::BulkString(b"key".to_vec()),
                OwnedFrame::BulkString(b"value".to_vec()),
            ]),
            Frame::Array(Some(vec![
                Frame::Bulk(Some(b"SET".to_vec())),
                Frame::Bulk(Some(b"key".to_vec())),
                Frame::Bulk(Some(b"value".to_vec())),
            ])),
        ),
        (
            OwnedFrame::Array(vec![
                OwnedFrame::Integer(1),
                OwnedFrame::SimpleString(b"OK".to_vec()),
                OwnedFrame::Error("ERR boom".into()),
                OwnedFrame::Null,
                OwnedFrame::Array(vec![
                    OwnedFrame::Integer(2),
                    OwnedFrame::BulkString(b"x".to_vec()),
                ]),
            ]),
            Frame::Array(Some(vec![
                Frame::Integer(1),
                Frame::Simple("OK".into()),
                Frame::Error("ERR boom".into()),
                Frame::Bulk(None),
                Frame::Array(Some(vec![
                    Frame::Integer(2),
                    Frame::Bulk(Some(b"x".to_vec())),
                ])),
            ])),
        ),
    ];
    for (owned, expected) in pairs {
        let mut buf = vec![0u8; 4096];
        let n = oracle_encode(&mut buf, &owned, false).expect("oracle encode must not error");
        let wire = &buf[..n];
        let (decoded, consumed) = our_decode(wire).expect("our decode must accept oracle output");
        assert_eq!(consumed, wire.len(), "we must consume all bytes: {owned:?}");
        assert_eq!(decoded, expected, "roundtrip mismatch: {owned:?}");
    }
}
