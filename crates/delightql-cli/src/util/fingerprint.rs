// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The CLI's renderings of a result digest. The digest itself is
//! `delightql_protocol::digest`; this module chooses the heading a digest
//! reads and assembles the structured `-f fingerprint` record.

use delightql_protocol::digest::{self, Observation};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

/// `-f fingerprint`: an executed result's digests and shape as one record.
///
/// `dbhash` and `totalhash` keep the record's frozen shape. A result digest
/// reads no database file, so `dbhash` is always `NO_DB` and `totalhash`
/// binds `tablehash` to it.
#[derive(Debug, Serialize)]
pub struct Fingerprint {
    pub dbhash: &'static str,
    /// The data digest: rows only, the value `-f hash` prints.
    pub datahash: String,
    /// The table digest: heading and rows, the value `-f totalhash` prints.
    pub tablehash: String,
    pub dimensions: String,
    pub totalhash: String,
    /// The heading the table digest read.
    pub columns: Vec<String>,
    /// The preimage framing both digests were computed under.
    pub digest: &'static str,
}

const NO_DB: &str = "NO_DB";

impl Fingerprint {
    pub fn of(heading: Vec<String>, observation: &Observation) -> Self {
        let tablehash = observation.table(&heading).hex();
        let mut total = Sha256::new();
        total.update(b"RESULT:");
        total.update(tablehash.as_bytes());
        total.update(b"|DB:");
        total.update(NO_DB.as_bytes());
        Fingerprint {
            dbhash: NO_DB,
            datahash: observation.data().hex(),
            dimensions: format!("{}x{}", observation.row_count(), heading.len()),
            totalhash: total
                .finalize()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
            tablehash,
            columns: heading,
            digest: digest::VERSION,
        }
    }
}

/// The heading a digest reads: an authored name as it is, a minted one as
/// `<mint:N>`, numbering the minted columns left to right. A minted spelling
/// is drawn per compilation, so a digest that read it would never repeat;
/// the Header's naming, not the characters of the name, says which is which.
pub fn digest_heading(columns: &[String], naming: &[delightql_core::api::Naming]) -> Vec<String> {
    let mut minted = 0;
    columns
        .iter()
        .zip(naming)
        .map(|(name, naming)| match naming {
            delightql_core::api::Naming::Authored => name.clone(),
            delightql_core::api::Naming::Minted => {
                minted += 1;
                format!("<mint:{minted}>")
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_digest_heading_numbers_minted_columns_and_keeps_authored_ones() {
        use delightql_core::api::Naming::{Authored, Minted};
        let columns = ["id⊥80c43d5c2a8b6cd9", "name", "⊥expr_0b8b55a1dc6ca500"]
            .map(String::from)
            .to_vec();
        assert_eq!(
            digest_heading(&columns, &[Minted, Authored, Minted]),
            ["<mint:1>", "name", "<mint:2>"]
        );
    }

    /// The record's two digests are the shared digest's, over the same
    /// observation; a column rename moves only the one that reads names.
    #[test]
    fn a_fingerprint_reports_the_shared_digests() {
        let mut observation = Observation::new();
        observation.row([None, Some(&b""[..])]);
        let fingerprint = Fingerprint::of(vec!["a".into(), "b".into()], &observation);
        assert_eq!(fingerprint.datahash, observation.data().hex());
        assert_eq!(fingerprint.tablehash, observation.table(&["a", "b"]).hex());
        assert_eq!(fingerprint.dimensions, "1x2");
        assert_eq!(fingerprint.digest, digest::VERSION);
        let renamed = Fingerprint::of(vec!["a".into(), "c".into()], &observation);
        assert_eq!(renamed.datahash, fingerprint.datahash);
        assert_ne!(renamed.tablehash, fingerprint.tablehash);
    }
}
