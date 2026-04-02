// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use rustyline::{Editor, Result};

/// Minimal REPL
fn main() -> Result<()> {
    env_logger::init();
    let mut rl = Editor::<()>::new()?;
    loop {
        let line = rl.readline("> ")?; // read
        println!("Line: {}", line); // eval / print
    } // loop
}
