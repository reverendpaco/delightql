// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The result digest: a checksum of the cells the result protocol delivered.
//!
//! Its evidence is the protocol cell — SQL NULL or present bytes — never a
//! rendering of it. Every road that
//! digests a result digests it here: the CLI's digest formats, the ball
//! runner, and the C ABI the embedding hosts call.
//!
//! THE PREIMAGE, version [`VERSION`]. A cell is framed as its bytes with every
//! `\` written `\\` and every `|` written `\|`; SQL NULL is framed as `\N`, an
//! escape no present cell can produce. Each framed cell is closed by `|`. A
//! row's digest is SHA-256 of its framed cells. The data digest is SHA-256 of
//! `ROWS:` followed by every row digest in lowercase hex, sorted, each closed
//! by `\n`: row order is not observed and row multiplicity is. The table
//! digest frames each heading name as a present cell after `COLUMNS:` and
//! closes the heading with `\n` before the same `ROWS:` part.
//!
//! A nonempty cell with no `\` or `|` frames as its own bytes. Length
//! prefixes would be injective too, but would move every pinned baseline.
//!
//! `vectors.json` pins this version's outputs for every host that checks
//! itself against it. A change to the preimage is a new version.

use sha2::{Digest as _, Sha256};

/// The name of the preimage framing this module computes.
pub const VERSION: &str = "dql-result-digest/1";

/// A finished digest.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Digest([u8; 32]);

impl Digest {
    /// Lowercase hexadecimal, the spelling the CLI prints.
    pub fn hex(&self) -> String {
        hex(&self.0)
    }

    /// The pinned spelling a baseline file holds: the first eight characters
    /// of the URL-safe base64 of the digest.
    pub fn pin(&self) -> String {
        const ALPHABET: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let b = &self.0;
        [
            b[0] >> 2,
            (b[0] & 0x03) << 4 | b[1] >> 4,
            (b[1] & 0x0f) << 2 | b[2] >> 6,
            b[2] & 0x3f,
            b[3] >> 2,
            (b[3] & 0x03) << 4 | b[4] >> 4,
            (b[4] & 0x0f) << 2 | b[5] >> 6,
            b[5] & 0x3f,
        ]
        .iter()
        .map(|&six| ALPHABET[six as usize] as char)
        .collect()
    }
}

/// The rows of one result, each reduced to its row digest as it arrives.
#[derive(Default, Debug)]
pub struct Observation {
    rows: Vec<[u8; 32]>,
}

impl Observation {
    pub fn new() -> Self {
        Self::default()
    }

    /// Observe one row: its cells in column order, `None` being SQL NULL.
    pub fn row<'a>(&mut self, cells: impl IntoIterator<Item = Option<&'a [u8]>>) {
        let mut preimage = Vec::new();
        for cell in cells {
            frame(&mut preimage, cell);
        }
        self.rows.push(Sha256::digest(&preimage).into());
    }

    /// Observe every row of a fetched batch.
    pub fn rows(&mut self, rows: &[Vec<Option<Vec<u8>>>]) {
        for row in rows {
            self.row(row.iter().map(Option::as_deref));
        }
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// The data digest: the rows alone.
    pub fn data(&self) -> Digest {
        let mut hasher = Sha256::new();
        self.hash_rows(&mut hasher);
        Digest(hasher.finalize().into())
    }

    /// The table digest: the heading, then the rows.
    pub fn table<S: AsRef<str>>(&self, heading: &[S]) -> Digest {
        let mut preimage = b"COLUMNS:".to_vec();
        for name in heading {
            frame(&mut preimage, Some(name.as_ref().as_bytes()));
        }
        preimage.push(b'\n');
        let mut hasher = Sha256::new();
        hasher.update(&preimage);
        self.hash_rows(&mut hasher);
        Digest(hasher.finalize().into())
    }

    fn hash_rows(&self, hasher: &mut Sha256) {
        // Byte order and lowercase-hex order agree, so sorting the raw
        // digests sorts the hex the preimage spells.
        let mut sorted = self.rows.clone();
        sorted.sort_unstable();
        hasher.update(b"ROWS:");
        for row in &sorted {
            hasher.update(hex(row).as_bytes());
            hasher.update(b"\n");
        }
    }
}

/// The data digest of whole rows.
pub fn data(rows: &[Vec<Option<Vec<u8>>>]) -> Digest {
    let mut observation = Observation::new();
    observation.rows(rows);
    observation.data()
}

fn frame(preimage: &mut Vec<u8>, cell: Option<&[u8]>) {
    match cell {
        None => preimage.extend_from_slice(b"\\N"),
        Some(bytes) => {
            for &byte in bytes {
                if byte == b'\\' || byte == b'|' {
                    preimage.push(b'\\');
                }
                preimage.push(byte);
            }
        }
    }
    preimage.push(b'|');
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    type Row = Vec<Option<Vec<u8>>>;

    fn cells(spec: &[Option<&str>]) -> Row {
        spec.iter()
            .map(|c| c.map(|s| s.as_bytes().to_vec()))
            .collect()
    }

    fn preimage(row: &Row) -> Vec<u8> {
        let mut out = Vec::new();
        for cell in row {
            frame(&mut out, cell.as_deref());
        }
        out
    }

    /// Reads a row preimage back into its cells. That this is a function —
    /// one preimage, one reading — is the framing's injectivity.
    fn read_back(mut bytes: &[u8]) -> Row {
        let mut row = Vec::new();
        while !bytes.is_empty() {
            if let Some(rest) = bytes.strip_prefix(b"\\N|") {
                row.push(None);
                bytes = rest;
                continue;
            }
            let mut cell = Vec::new();
            loop {
                match bytes {
                    [b'|', rest @ ..] => {
                        bytes = rest;
                        break;
                    }
                    [b'\\', escaped @ (b'\\' | b'|'), rest @ ..] => {
                        cell.push(*escaped);
                        bytes = rest;
                    }
                    [b'\\', ..] => panic!("an escape a present cell cannot write"),
                    [byte, rest @ ..] => {
                        cell.push(*byte);
                        bytes = rest;
                    }
                    [] => panic!("a cell left unclosed"),
                }
            }
            row.push(Some(cell));
        }
        row
    }

    /// Every row of up to three cells over the alphabet that can confuse a
    /// framing: NULL, empty, the separator, the escape, the letter a NULL
    /// escape spells, the text NULL, and an ordinary byte.
    fn confusable_rows() -> Vec<Row> {
        let alphabet: [Option<&str>; 8] = [
            None,
            Some(""),
            Some("|"),
            Some("\\"),
            Some("N"),
            Some("\\N"),
            Some("NULL"),
            Some("a"),
        ];
        let mut rows: Vec<Row> = vec![vec![]];
        let mut frontier: Vec<Row> = vec![vec![]];
        for _ in 0..3 {
            let mut next = Vec::new();
            for row in &frontier {
                for cell in alphabet {
                    let mut longer = row.clone();
                    longer.push(cell.map(|s| s.as_bytes().to_vec()));
                    next.push(longer);
                }
            }
            rows.extend(next.iter().cloned());
            frontier = next;
        }
        rows
    }

    #[test]
    fn a_row_preimage_reads_back_to_exactly_its_cells() {
        let rows = confusable_rows();
        assert_eq!(rows.len(), 1 + 8 + 64 + 512);
        let mut seen = std::collections::HashMap::new();
        for row in rows {
            let bytes = preimage(&row);
            assert_eq!(read_back(&bytes), row);
            if let Some(other) = seen.insert(bytes, row.clone()) {
                panic!("{row:?} and {other:?} share a preimage");
            }
        }
    }

    #[test]
    fn null_empty_and_the_text_null_are_three_cells() {
        let digests: Vec<Digest> = [None, Some(""), Some("NULL"), Some("\\N")]
            .iter()
            .map(|cell| data(&[cells(&[*cell])]))
            .collect();
        for (i, a) in digests.iter().enumerate() {
            for b in &digests[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }

    #[test]
    fn rows_are_a_multiset() {
        let a = cells(&[Some("a")]);
        let b = cells(&[Some("b")]);
        assert_eq!(data(&[a.clone(), b.clone()]), data(&[b.clone(), a.clone()]));
        assert_ne!(data(&[a.clone()]), data(&[a.clone(), a.clone()]));
        assert_ne!(
            data(&[a.clone(), b.clone()]),
            data(&[cells(&[Some("a"), Some("b")])])
        );
        assert_ne!(data(&[]), data(&[vec![]]));
    }

    #[test]
    fn a_blob_is_its_bytes_not_a_decoding() {
        // Both decode to U+FFFD; a lossy text road equates them.
        let ff = vec![vec![Some(vec![0xff])]];
        let fe = vec![vec![Some(vec![0xfe])]];
        assert_ne!(data(&ff), data(&fe));
        assert_ne!(
            data(&[vec![Some(vec![0x00, 0x01])]]),
            data(&[vec![Some(vec![0x00, 0x02])]])
        );
    }

    #[test]
    fn a_heading_name_is_framed_like_a_cell() {
        let mut observation = Observation::new();
        observation.row([Some(&b"1"[..]), Some(&b"2"[..])]);
        assert_ne!(
            observation.table(&["a|", "b"]),
            observation.table(&["a", "|b"])
        );
        assert_ne!(observation.table(&["a", "b"]), observation.data());
    }

    #[test]
    fn the_pin_is_the_leading_base64url_of_the_digest() {
        // The zero-row data digest every empty baseline once spelled.
        assert_eq!(data(&[]).pin(), "5yt78PzT");
    }

    /// The committed vectors are the contract every host checks itself
    /// against; this implementation answers them first.
    #[test]
    fn the_committed_vectors_hold() {
        let doc: serde_json::Value =
            serde_json::from_str(include_str!("digest/vectors.json")).unwrap();
        assert_eq!(doc["version"], VERSION);
        let vectors = doc["vectors"].as_array().unwrap();
        assert!(!vectors.is_empty());
        for vector in vectors {
            let name = vector["name"].as_str().unwrap();
            let mut observation = Observation::new();
            for row in vector["rows"].as_array().unwrap() {
                let row: Row = row
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|cell| cell.as_str().map(unhex))
                    .collect();
                observation.row(row.iter().map(Option::as_deref));
            }
            let heading: Vec<&str> = vector["heading"]
                .as_array()
                .unwrap()
                .iter()
                .map(|name| name.as_str().unwrap())
                .collect();
            let data = observation.data();
            assert_eq!(data.hex(), vector["data"], "{name}: data");
            assert_eq!(data.pin(), vector["pin"], "{name}: pin");
            assert_eq!(
                observation.table(&heading).hex(),
                vector["table"],
                "{name}: table"
            );
        }
    }

    fn unhex(text: &str) -> Vec<u8> {
        (0..text.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
            .collect()
    }
}
