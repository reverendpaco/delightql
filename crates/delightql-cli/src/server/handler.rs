// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// DQL Server — Per-connection handler
//
// Uses the trait API: DqlHandle.create_relay() returns Box<dyn ServerRelay>.
// The relay implements Handler (for protocol terms) and ServerRelay (for reset).

use std::os::unix::net::UnixStream;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use delightql_core::api::DqlHandle;
use delightql_protocol::socket::{read_client_message, write_server_message};
use delightql_protocol::{
    ClientMessage, ControlOp, ControlResult, ServerMessage, ServerTerm, TransportError,
};

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Serve one connection: read ClientMessages, dispatch to relay, write ServerMessages.
/// Returns when the connection closes or an IO error occurs.
#[stacksafe::stacksafe]
pub fn serve_connection(
    mut stream: UnixStream,
    handle: &mut dyn DqlHandle,
    last_activity: &AtomicU64,
    shutdown: &AtomicBool,
) {
    // Build the relay via the trait API — no SqlParty/protocol plumbing here
    let mut relay = match handle.create_relay() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("server: failed to create relay: {}", e);
            return;
        }
    };

    let mut buf = Vec::new();
    loop {
        let message = match read_client_message(&mut stream, &mut buf) {
            Ok(m) => m,
            Err(TransportError { message }) => {
                if message != "connection closed" {
                    eprintln!("server: read error: {}", message);
                }
                return;
            }
        };

        last_activity.store(now_secs(), Ordering::Relaxed);

        let response = match message {
            ClientMessage::Data(term) => {
                match panic::catch_unwind(AssertUnwindSafe(|| relay.handle(term))) {
                    Ok(server_term) => ServerMessage::Data(server_term),
                    Err(panic_info) => {
                        let msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                            format!("internal error (panic): {}", s)
                        } else if let Some(s) = panic_info.downcast_ref::<String>() {
                            format!("internal error (panic): {}", s)
                        } else {
                            "internal error (panic)".to_string()
                        };
                        eprintln!("server: worker caught panic: {}", msg);
                        ServerMessage::Data(ServerTerm::Error(delightql_protocol::WireError::of(
                            &delightql_types::diagnostic::Internal::Panic {
                                message: msg,
                                location: None,
                            }
                            .into(),
                        )))
                    }
                }
            }
            ClientMessage::Control(ControlOp::Reset) => {
                // A server session is a Core world plus what the client
                // mounts. Its handle was opened under the server profile,
                // which carries no client namespaces and no capability to
                // mount them, so a reset restores that profile and installs
                // nothing after it.
                match relay.handle_reset() {
                    Ok(()) => ServerMessage::Control(ControlResult::Ok),
                    Err(e) => ServerMessage::Control(ControlResult::Error(
                        delightql_protocol::WireError::of(&e),
                    )),
                }
            }
            // The session's own base directory, over the one the server
            // stated at boot; a reset returns it to the boot value.
            ClientMessage::Control(ControlOp::Cwd(path)) => {
                match relay.set_session_setting(delightql_core::api::BASE_DIRECTORY, Some(&path)) {
                    Ok(()) => ServerMessage::Control(ControlResult::Ok),
                    Err(e) => ServerMessage::Control(ControlResult::Error(
                        delightql_protocol::WireError::of(&e),
                    )),
                }
            }
            ClientMessage::Control(ControlOp::Shutdown) => {
                shutdown.store(true, Ordering::Relaxed);
                ServerMessage::Control(ControlResult::Ok)
            }
        };

        if let Err(e) = write_server_message(&mut stream, &response) {
            eprintln!("server: write error: {}", e.message);
            return;
        }
    }
}
