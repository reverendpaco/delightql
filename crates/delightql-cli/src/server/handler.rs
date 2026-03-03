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
                        ServerMessage::Data(ServerTerm::Error {
                            kind: delightql_protocol::ErrorKind::Connection,
                            identity: vec![],
                            message: msg.into_bytes(),
                        })
                    }
                }
            }
            ClientMessage::Control(ControlOp::Reset) => {
                delightql_core::session_cwd::set(None);
                match relay.handle_reset() {
                    Ok(()) => ServerMessage::Control(ControlResult::Ok),
                    Err(e) => ServerMessage::Control(ControlResult::Error {
                        message: e.to_string(),
                    }),
                }
            }
            ClientMessage::Control(ControlOp::Cwd(path)) => {
                delightql_core::session_cwd::set(Some(path));
                ServerMessage::Control(ControlResult::Ok)
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
