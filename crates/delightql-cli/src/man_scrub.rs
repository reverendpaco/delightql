// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! troff → plain text for the HOUSE MAN DIALECT (feeds the
//! `man_page.plain` column).
//!
//! This is NOT a roff renderer. Roff in general is a Turing tar pit
//! (macro definitions, conditionals, registers); this module handles
//! the closed dialect our pages actually use — 16 macros, 5
//! escapes — and REFUSES everything else. The
//! `test_man_pages_stay_inside_the_house_dialect` bin test walks every
//! shipped page through here, so the dialect stays closed by
//! assertion: extend this scrubber or stay inside the dialect, never
//! silently degrade.
//!
//! Output quality bar: the LAST rung of the rendering chain (readable
//! when `man` and `groff` are both absent — curl --manual style).
//! Interactive rendering pipes the troff to `man -l -`, which owns
//! typesetting; this is deliberately simpler than that.

/// Scrub one troff page to plain text. `Err` names the first token
/// outside the house dialect.
pub fn scrub(troff: &str) -> Result<String, String> {
    let mut out = String::new();
    let mut in_example = false;
    // Current body indent (spaces). Section text sits at 3; .TP/.IP
    // bodies at 7, man-style-ish.
    let mut indent = 0usize;
    // Set by .TP: the next output line is the hanging tag at `indent`,
    // and following lines drop to indent + 4.
    let mut pending_tag = false;

    for line in troff.lines() {
        if in_example {
            if line == ".EE" {
                in_example = false;
                continue;
            }
            out.push_str(&" ".repeat(indent + 2));
            out.push_str(&unescape(line)?);
            out.push('\n');
            continue;
        }
        if let Some(rest) = line.strip_prefix('.') {
            let (name, raw_args) = rest.split_once(' ').unwrap_or((rest, ""));
            let args = split_quoted(raw_args);
            match name {
                "TH" => {
                    let page = args.first().cloned().unwrap_or_default();
                    let section = args.get(1).cloned().unwrap_or_default();
                    out.push_str(&format!("{}({})\n", page, section));
                }
                "SH" => {
                    out.push('\n');
                    out.push_str(&unescape(&args.join(" "))?);
                    out.push('\n');
                    indent = 3;
                    pending_tag = false;
                }
                "SS" => {
                    out.push('\n');
                    out.push_str("  ");
                    out.push_str(&unescape(&args.join(" "))?);
                    out.push('\n');
                    indent = 3;
                    pending_tag = false;
                }
                "TP" => {
                    out.push('\n');
                    indent = 3;
                    pending_tag = true;
                }
                "IP" => {
                    out.push('\n');
                    indent = 3;
                    // `.IP \(bu 3` style bullets; a bare .IP is a plain
                    // indented paragraph.
                    if let Some(first) = args.first() {
                        if first == "\\(bu" {
                            out.push_str(&" ".repeat(indent));
                            out.push_str("• ");
                            indent += 2;
                            continue;
                        }
                    }
                    indent += 2;
                }
                "PP" => {
                    out.push('\n');
                    indent = 3;
                    pending_tag = false;
                }
                "br" => {
                    // hard break: nothing to join; next line starts fresh
                }
                "EX" => {
                    in_example = true;
                    out.push('\n');
                }
                "EE" => {
                    in_example = false;
                }
                // Font macros: .B/.I join args with spaces; the
                // two-letter alternators concatenate their args
                // directly (that is what font alternation means).
                "B" | "I" => {
                    emit_text(&mut out, &mut indent, &mut pending_tag, &unescape(&args.join(" "))?);
                }
                "BI" | "BR" | "IR" | "RB" | "RI" | "IB" => {
                    emit_text(&mut out, &mut indent, &mut pending_tag, &unescape(&args.concat())?);
                }
                other => {
                    return Err(format!(
                        "outside the house man dialect: unknown macro '.{}' — \
                         extend man_scrub.rs or stay inside the dialect",
                        other
                    ));
                }
            }
        } else if let Some(comment) = line.strip_prefix(".\\\"") {
            let _ = comment; // troff comment
        } else {
            emit_text(&mut out, &mut indent, &mut pending_tag, &unescape(line)?);
        }
    }
    Ok(out)
}

/// Emit one logical text line at the current indent, handling the
/// .TP hanging-tag state.
fn emit_text(out: &mut String, indent: &mut usize, pending_tag: &mut bool, text: &str) {
    out.push_str(&" ".repeat(*indent));
    out.push_str(text);
    out.push('\n');
    if *pending_tag {
        *pending_tag = false;
        *indent += 4;
    }
}

/// Resolve the house escape set; refuse anything else.
fn unescape(s: &str) -> Result<String, String> {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('-') => out.push('-'),
            Some('e') => out.push('\\'),
            Some(' ') => out.push(' '), // non-breaking space
            Some('&') => {} // zero-width: suppresses interpretation
            Some('f') => {
                // font switch \fB \fI \fR \fP: styling, discard
                chars.next();
            }
            Some('(') => {
                let a = chars.next().unwrap_or(' ');
                let b = chars.next().unwrap_or(' ');
                match (a, b) {
                    ('e', 'm') => out.push('—'),
                    ('b', 'u') => out.push('•'),
                    ('d', 'q') => out.push('"'),
                    ('a', 'q') => out.push('\''),
                    ('s', 'c') => out.push('§'),
                    _ => {
                        return Err(format!(
                            "outside the house man dialect: unknown escape '\\({}{}'",
                            a, b
                        ))
                    }
                }
            }
            Some(other) => {
                return Err(format!(
                    "outside the house man dialect: unknown escape '\\{}'",
                    other
                ))
            }
            None => out.push('\\'),
        }
    }
    Ok(out)
}

/// Split macro arguments, honoring double quotes and `\ `-escaped
/// (non-breaking) spaces.
fn split_quoted(s: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut escaped = false;
    for c in s.chars() {
        if escaped {
            cur.push(c);
            escaped = false;
            continue;
        }
        match c {
            '\\' => {
                cur.push(c);
                escaped = true;
            }
            '"' => in_quotes = !in_quotes,
            ' ' if !in_quotes => {
                if !cur.is_empty() {
                    args.push(std::mem::take(&mut cur));
                }
            }
            _ => cur.push(c),
        }
    }
    if !cur.is_empty() {
        args.push(cur);
    }
    args
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scrubs_the_basic_shapes() {
        let troff = "\
.TH DQL-X 1 \"2026\" \"dql\" \"Manual\"
.SH NAME
dql-x \\- does a thing
.SH OPTIONS
.TP
.BI \\-\\-flag \" VALUE\"
Does the flag thing \\(em loudly.
.IP \\(bu 3
first bullet
.SH EXAMPLES
.EX
$ dql x --flag v
.EE
";
        let plain = scrub(troff).unwrap();
        assert!(plain.contains("DQL-X(1)"));
        assert!(plain.contains("NAME"));
        assert!(plain.contains("dql-x - does a thing"));
        assert!(plain.contains("--flag VALUE"));
        assert!(plain.contains("— loudly."));
        assert!(plain.contains("• first bullet") || plain.contains("•"));
        assert!(plain.contains("$ dql x --flag v"));
    }

    /// The dialect stays closed by assertion: every shipped page must
    /// scrub. A page using a new macro/escape turns this red — extend
    /// the scrubber or stay inside the dialect.
    #[test]
    fn all_shipped_pages_stay_inside_the_house_dialect() {
        let man_dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../assets/man/man1");
        let mut checked = 0;
        for entry in std::fs::read_dir(&man_dir).expect("assets/man/man1 must exist") {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) != Some("1") {
                continue;
            }
            let troff = std::fs::read_to_string(&path).unwrap();
            let plain = scrub(&troff).unwrap_or_else(|e| {
                panic!("{} is outside the house dialect: {e}", path.display())
            });
            assert!(
                plain.len() > 200,
                "{}: suspiciously short scrub output",
                path.display()
            );
            checked += 1;
        }
        assert!(checked >= 8, "expected all shipped pages, found {checked}");
    }

    #[test]
    fn refuses_outside_the_dialect() {
        assert!(scrub(".de MYMACRO").is_err());
        assert!(scrub("text with \\(zz unknown").is_err());
        assert!(scrub("bad \\z escape").is_err());
    }
}
