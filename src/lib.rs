//! A small library that takes a protobuf serialized message and sorts fields inside,
//! so that the field order becomes deterministic.

use std::borrow::Cow;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("failed to parse protobuf message")]
pub struct ParseError;

/// Checks if the given serialized protobuf message has "canonical" fields order,
/// i.e. ordered by field ID.
pub fn is_protobuf_message_sorted(msg: &[u8]) -> Result<bool, ParseError> {
    let chunks = parse_message(msg)?;
    Ok(is_sorted(&chunks))
}

/// Sort fields in the given protobuf message in the "canonical" order (by field ID).
pub fn sort_protobuf_message(msg: &[u8]) -> Result<Cow<[u8]>, ParseError> {
    let mut chunks = parse_message(msg)?;
    if is_sorted(&chunks) {
        Ok(Cow::Borrowed(msg))
    } else {
        let sorted = do_sort(&mut chunks, msg);
        Ok(Cow::Owned(sorted))
    }
}

/// Sort fields in the given protobuf message in the "canonical" order (by field ID).
pub fn sort_protobuf_message_inplace(msg: &mut [u8]) -> Result<(), ParseError> {
    let mut chunks = parse_message(msg)?;
    if !is_sorted(&chunks) {
        let sorted = do_sort(&mut chunks, msg);
        msg.copy_from_slice(&sorted);
    }
    Ok(())
}

struct Chunk {
    id: u128,
    offset: usize,
    length: usize,
}

// Function [T]::is_sorted() is still unstable (as of Rust 1.61), so need this
fn is_sorted(chunks: &[Chunk]) -> bool {
    if chunks.is_empty() {
        return true;
    }
    let mut prev = chunks[0].id;
    for i in 1..chunks.len() {
        let cur = chunks[i].id;
        if cur < prev {
            return false;
        }
        prev = cur;
    }
    true
}

fn do_sort(chunks: &mut [Chunk], msg: &[u8]) -> Vec<u8> {
    chunks.sort_by_key(|ck| ck.id);
    let mut sorted = Vec::with_capacity(msg.len());
    for ck in chunks.iter() {
        let start = ck.offset;
        let end = ck.offset + ck.length;
        let chunk = &msg[start..end];
        sorted.extend_from_slice(chunk);
    }
    sorted
}

/// Parse protobuf message and split it into chunks that can be reordered
fn parse_message(msg: &[u8]) -> Result<Vec<Chunk>, ParseError> {
    let mut chunks = Vec::new();
    let mut bytes = msg;
    let mut offset = 0;
    while bytes.len() > 0 {
        let (key, len) = read_varint(bytes)?;
        let (field_id, wire_type) = (key >> 3, key & 0x7);
        let field_length = match wire_type {
            0 => {
                // Varint
                let (_, len) = read_varint(&bytes[len..])?;
                len
            }
            1 => {
                // 64-bit
                8
            }
            2 => {
                // Length-delimited
                let (value, len) = read_varint(&bytes[len..])?;
                if value > usize::MAX as u128 {
                    // Too big data length
                    return Err(ParseError);
                }
                value as usize + len
            }
            3 | 4 => {
                // Deprecated stuff, not supported
                return Err(ParseError);
            }
            5 => {
                // 32-bit
                4
            }
            _ => {
                // Unrecognized wire type
                return Err(ParseError);
            }
        };
        let total_length = len + field_length;
        let chunk = Chunk {
            id: field_id,
            offset,
            length: total_length,
        };
        offset += total_length;
        bytes = &msg[offset..];
        chunks.push(chunk);
    }
    Ok(chunks)
}

#[inline]
fn read_varint(bytes: &[u8]) -> Result<(u128, usize), ParseError> {
    let buf_size = bytes.len();
    if buf_size == 0 {
        // No data
        return Err(ParseError);
    }
    let (mut data, mut offset) = (0, 0);
    while offset < buf_size {
        let byte = bytes[offset];
        offset += 1;
        data = (data << 7) | (byte & 0x7F) as u128;
        if byte & 0x80 == 0 {
            // Last byte
            break;
        }
    }
    let varint_length = offset;
    Ok((data, varint_length))
}

//noinspection SpellCheckingInspection
#[cfg(test)]
mod tests {
    use super::{is_protobuf_message_sorted, parse_message, sort_protobuf_message, sort_protobuf_message_inplace};
    use lazy_static::lazy_static;

    lazy_static! {
        /// This protobuf message has fields ordered by ID (consider this canonical)
        static ref CANONICAL_FIELD_ORDER: Vec<u8> = hex::decode(concat!(
            "08541a206519a0dd8255be656014fc1e89efad6871a111bc0837ec13b886c94b",
            "d08cf41a22220a203cc289d22a301557d04e3e88b76b1299785a1dee92c1ccb2",
            "334dc86c98501bc1280130904e38904e40f7b7df81923048f787b2b097305204",
            "10e0a71258046a41c95509f78a317a01e39c9fbf5a7541f04b47181ac46a3e12",
            "a31c0489d14bddd54cb71b13554b1acae37ffecc936bd448db604d2796a94c81",
            "2482133e343a9d0e1b7002"
        ))
        .expect("bad test data");

        /// This protobuf message has non-canonically ordered fields
        static ref NON_CANONICAL_FIELD_ORDER: Vec<u8> = hex::decode(concat!(
            "08546a41c95509f78a317a01e39c9fbf5a7541f04b47181ac46a3e12a31c0489",
            "d14bddd54cb71b13554b1acae37ffecc936bd448db604d2796a94c812482133e",
            "343a9d0e1b1a206519a0dd8255be656014fc1e89efad6871a111bc0837ec13b8",
            "86c94bd08cf41a22220a203cc289d22a301557d04e3e88b76b1299785a1dee92",
            "c1ccb2334dc86c98501bc1280130904e38904e40f7b7df81923048f787b2b097",
            "30520410e0a71258047002"
        ))
        .expect("bad test data");
    }

    #[test]
    fn test_parse_message() {
        assert!(parse_message(&[]).is_ok());
        assert!(parse_message(&CANONICAL_FIELD_ORDER).is_ok());
        assert!(parse_message(&NON_CANONICAL_FIELD_ORDER).is_ok());
    }

    #[test]
    fn test_is_message_sorted() {
        assert_eq!(is_protobuf_message_sorted(&[]).unwrap(), true);
        assert_eq!(is_protobuf_message_sorted(&CANONICAL_FIELD_ORDER).unwrap(), true);
        assert_eq!(is_protobuf_message_sorted(&NON_CANONICAL_FIELD_ORDER).unwrap(), false);
    }

    #[test]
    fn test_sort_message() {
        let test = |msg: &[u8], expected: &[u8]| {
            let sorted = sort_protobuf_message(msg).unwrap();
            assert!(is_protobuf_message_sorted(&sorted).unwrap());
            assert_eq!(sorted, expected.to_vec());
        };
        test(&[], &[]);
        test(&CANONICAL_FIELD_ORDER, &CANONICAL_FIELD_ORDER);
        test(&NON_CANONICAL_FIELD_ORDER, &CANONICAL_FIELD_ORDER);
    }

    #[test]
    fn test_sort_message_inplace() {
        let test = |msg: &[u8]| {
            let mut msg = Vec::from(msg);
            sort_protobuf_message_inplace(&mut msg).unwrap();
            assert!(is_protobuf_message_sorted(&msg).unwrap());
            msg
        };
        assert_eq!(test(&[]), &[]);
        assert_eq!(test(&CANONICAL_FIELD_ORDER), CANONICAL_FIELD_ORDER.to_vec());
        assert_eq!(test(&NON_CANONICAL_FIELD_ORDER), CANONICAL_FIELD_ORDER.to_vec());
    }
}
