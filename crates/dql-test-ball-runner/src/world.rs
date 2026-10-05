// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The world a test runs in, and the only road to a session.
//!
//! ONE TEST, ONE WORLD. A session reaches a caller only through
//! [`Link::in_world`], and `in_world` establishes the world it was given
//! before it runs anything on it. There is no signature here that hands out a
//! session a previous test has already used, so no caller can inherit one by
//! forgetting to ask for a fresh one.
//!
//! The amortized world is the road this replaces: a session set up once and
//! reused while the mounted database stayed the same. A cell that published
//! an inline definition then served that definition to every cell that
//! followed it on the same connection, and the harness reported the
//! contamination as those cells' own results.

use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use delightql_protocol::socket::SocketTransport;
use delightql_protocol::{
    AgreedOrientation, Client, ControlResponse, FetchResponse, Orientation, Projection,
    QueryResponse, Session, VersionResult,
};

/// Everything a test needs established before its first statement: where its
/// relative paths resolve, and which database answers as `main`.
///
/// A world is DECLARED, never accumulated. Two tests that want the same
/// database still get two establishments, because the second one's world says
/// nothing about what the first one did to it.
pub(crate) struct World {
    /// The session working directory, or `None` to leave the process's.
    /// A side-effect-free test names no files, so it declares none — and
    /// `reset` clears whatever a predecessor set.
    pub(crate) cwd: Option<String>,
    /// The database mounted as `main`: absolute, or relative to `cwd`.
    pub(crate) mount: String,
}

/// Whether a failure means the SESSION is unusable, as opposed to the query
/// having been ANSWERED with a refusal.
///
/// A refusal is an answer: the frame arrived and the session is in step, so
/// the next request reads its own reply. A transport failure is not — a read
/// deadline leaves the abandoned request's response still to come, and the
/// next reader would take that late frame for its own. Reusing the session
/// across one is how a single silent query took the rest of a shard with it.
pub(crate) fn is_transport_failure(message: &str) -> bool {
    !message.starts_with("query error:")
}

/// What a caller is told when the link holds no session: the reconnect that
/// would have supplied one could not reach the server. It reads as a
/// transport failure, so the test that follows tries the reconnect again
/// instead of inheriting a dead link in silence.
const SESSION_LOST: &str = "session lost: the reconnect did not reach the server";

/// A connection to one `dql server`, and the means to replace it.
///
/// The session inside is PRIVATE to this module. Every phase holds one of
/// these rather than a bare session, because the recovery rule is the same
/// everywhere: on a transport failure the session is discarded, a new one is
/// taken, and the test's required state is established again on it.
pub(crate) struct Link {
    socket: PathBuf,
    query_timeout: Option<Duration>,
    /// Empty exactly while a replacement is being taken, and after a
    /// reconnect that failed. The option is what makes the poisoned session
    /// droppable BEFORE its replacement is opened; a plain field can only be
    /// overwritten after, which keeps the dead connection alive across the
    /// new handshake.
    session: Option<Session<SocketTransport>>,
    orientation: AgreedOrientation,
}

impl Link {
    pub(crate) fn connect(socket: &Path, query_timeout: Option<Duration>) -> Result<Self, String> {
        let (session, orientation) = open_session(socket, query_timeout)?;
        Ok(Link {
            socket: socket.to_path_buf(),
            query_timeout,
            session: Some(session),
            orientation,
        })
    }

    /// Establish `world` on this link and run one test in it.
    ///
    /// The establishment is not optional and not cached: it is the first half
    /// of this one call. A setup that could not be made to hold is THIS
    /// test's error — it was once a `?` that took every remaining test in the
    /// shard out of the reported totals with it — and a transport failure in
    /// the test itself renews the session before the next caller arrives.
    pub(crate) fn in_world<T>(
        &mut self,
        world: &World,
        run: impl FnOnce(&mut Session<SocketTransport>, AgreedOrientation) -> Result<T, String>,
    ) -> Result<T, String> {
        if let Err(e) = self.establish(world) {
            return Err(format!("session setup: {}", e));
        }
        let orientation = self.orientation;
        let outcome = match self.session.as_mut() {
            Some(session) => run(session, orientation),
            None => Err(SESSION_LOST.to_string()),
        };
        self.recover_if_poisoned(&outcome);
        outcome
    }

    /// Discard the poisoned session and take a fresh one.
    ///
    /// The old session is taken and dropped BEFORE the replacement is opened,
    /// closing its socket first. A server serves one connection per worker
    /// until that connection closes: hold the poisoned one open across the
    /// replacement's connect and handshake and the worker that must service
    /// the replacement is still owned by the connection being abandoned. The
    /// replacement then waits behind it for another deadline, and the late
    /// response is written into a stream that is still open to read it.
    fn renew(&mut self) -> Result<(&mut Session<SocketTransport>, AgreedOrientation), String> {
        drop(self.session.take());
        let (session, orientation) = open_session(&self.socket, self.query_timeout)?;
        self.orientation = orientation;
        Ok((self.session.insert(session), orientation))
    }

    /// Reset the session, point it at the test's directory, and mount the
    /// test's database — taking a fresh session if the current one has been
    /// poisoned.
    ///
    /// Setup is where a poisoned session shows itself: a late frame is what
    /// the next reset would read. Retrying ONCE on a new session is enough —
    /// a second failure is the server being gone, not a stale frame, and the
    /// caller records it against the test rather than abandoning the shard.
    fn establish(&mut self, world: &World) -> Result<(), String> {
        let setup = |session: &mut Session<SocketTransport>,
                     orientation: AgreedOrientation|
         -> Result<(), String> {
            send_reset(session)?;
            if let Some(cwd) = world.cwd.as_deref() {
                send_cwd(session, cwd)?;
            }
            send_mount(session, &world.mount, orientation)
        };

        let orientation = self.orientation;
        let first = match self.session.as_mut() {
            Some(session) => setup(session, orientation),
            None => Err(SESSION_LOST.to_string()),
        };
        match first {
            Ok(()) => Ok(()),
            Err(first) => {
                let (session, orientation) = self
                    .renew()
                    .map_err(|e| format!("{}; reconnect failed: {}", first, e))?;
                setup(session, orientation)
                    .map_err(|e| format!("{}; after reconnect: {}", first, e))
            }
        }
    }

    /// Renew the session when the failure was the transport's. The row is the
    /// caller's to record — exactly one, whether the query answered, refused,
    /// or went silent.
    fn recover_if_poisoned<T>(&mut self, outcome: &Result<T, String>) {
        if let Err(message) = outcome {
            if is_transport_failure(message) {
                eprintln!("runner: session lost ({}), reconnecting", message);
                if let Err(e) = self.renew() {
                    eprintln!("runner: reconnect failed: {}", e);
                }
            }
        }
    }
}

fn open_session(
    socket_path: &Path,
    query_timeout: Option<Duration>,
) -> Result<(Session<SocketTransport>, AgreedOrientation), String> {
    let stream = UnixStream::connect(socket_path)
        .map_err(|e| format!("connect to {}: {}", socket_path.display(), e))?;
    // A deadline on READS, which is what a silent server looks like from
    // here. It bounds the wait between bytes rather than the whole query, so
    // it stops a hang without cutting a slow-but-answering stream short.
    if let Some(timeout) = query_timeout {
        stream
            .set_read_timeout(Some(timeout))
            .map_err(|e| format!("set read timeout: {}", e))?;
    }
    let transport = SocketTransport::new(stream);
    let client = Client::new(transport);

    let session = match client
        .version(
            1_000_000,
            delightql_protocol::PROTOCOL_VERSION.to_vec(),
            300_000,
            vec![Orientation::Rows],
        )
        .map_err(|e| format!("version handshake: {}", e.message))?
    {
        VersionResult::Accepted(s) => s,
        VersionResult::Rejected(error) => {
            return Err(format!("version rejected: {}", error.message_str()));
        }
    };

    let rows_orientation = session
        .agreed_orientation(Orientation::Rows)
        .ok_or("server does not support Rows orientation")?;

    Ok((session, rows_orientation))
}

fn send_reset(session: &mut Session<SocketTransport>) -> Result<(), String> {
    match session
        .reset()
        .map_err(|e| format!("reset: {}", e.message))?
    {
        ControlResponse::Ok => Ok(()),
        ControlResponse::Error(error) => Err(format!(
            "reset: [{}] {}",
            error.identity_str(),
            error.message_str()
        )),
    }
}

fn send_cwd(session: &mut Session<SocketTransport>, path: &str) -> Result<(), String> {
    match session
        .cwd(path.to_string())
        .map_err(|e| format!("cwd: {}", e.message))?
    {
        ControlResponse::Ok => Ok(()),
        ControlResponse::Error(error) => Err(format!(
            "cwd: [{}] {}",
            error.identity_str(),
            error.message_str()
        )),
    }
}

fn send_mount(
    session: &mut Session<SocketTransport>,
    db_filename: &str,
    rows_orientation: AgreedOrientation,
) -> Result<(), String> {
    let mount_query = format!("mount!(\"{}\",\"main\")(*)", db_filename);
    let handle = match session
        .query(delightql_cst::prompt_wrap(&mount_query).into_owned().into_bytes())
        .map_err(|e| format!("mount: {}", e.message))?
    {
        QueryResponse::Header { handle, .. } => handle,
        QueryResponse::Error(error) => {
            return Err(format!(
                "mount error: {}",
                String::from_utf8_lossy(error.message())
            ));
        }
    };
    loop {
        match session
            .fetch(&handle, Projection::All, 10000, rows_orientation)
            .map_err(|e| format!("mount fetch: {}", e.message))?
        {
            FetchResponse::Data { .. } => continue,
            FetchResponse::End => break,
            FetchResponse::Error(error) => {
                return Err(format!(
                    "mount fetch error: {}",
                    String::from_utf8_lossy(error.message())
                ));
            }
        }
    }
    let _ = session.close(handle);
    Ok(())
}

/// Ask the server to stop, on a connection of its own.
///
/// The one session in this runner that is not a test's: it establishes no
/// world because it runs no test.
pub(crate) fn send_shutdown(
    socket_path: &Path,
    query_timeout: Option<Duration>,
) -> Result<(), String> {
    let stream = UnixStream::connect(socket_path).map_err(|e| format!("connect: {}", e))?;
    // Shutdown must not become the wait the query deadline just removed. A
    // server still working through a request the client abandoned answers
    // this handshake late or not at all, and the runner's last act would
    // otherwise be to block on it forever.
    if let Some(timeout) = query_timeout {
        let _ = stream.set_read_timeout(Some(timeout));
        let _ = stream.set_write_timeout(Some(timeout));
    }
    let transport = SocketTransport::new(stream);
    let client = Client::new(transport);
    let mut session = match client
        .version(
            1_000_000,
            delightql_protocol::PROTOCOL_VERSION.to_vec(),
            300_000,
            vec![Orientation::Rows],
        )
        .map_err(|e| format!("version: {}", e.message))?
    {
        VersionResult::Accepted(s) => s,
        VersionResult::Rejected(error) => {
            return Err(format!("rejected: {}", error.message_str()));
        }
    };
    let _ = session.shutdown();
    Ok(())
}
