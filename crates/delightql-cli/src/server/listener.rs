// DQL Server — Socket listener and thread pool
//
// Binds a Unix socket, accepts connections, and dispatches them to
// worker threads. Each worker has its own ConnectionManager + DqlHandle.

use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;

use anyhow::Result;

use crate::connection::ConnectionManager;
use crate::server::handler;

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Start the DQL server: bind socket, spawn workers, accept connections.
/// Blocks until shutdown is requested, idle timeout fires, or the process is killed.
pub fn start_server(
    db_path: Option<&str>,
    socket_path: &Path,
    num_workers: usize,
    idle_timeout: Option<u64>,
    socket_idle_timeout: Option<u64>,
) -> Result<()> {
    // Linux: auto-kill this process when our parent dies.
    // Prevents orphan server processes when the test harness crashes.
    #[cfg(target_os = "linux")]
    unsafe {
        libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
        // If our parent already died between fork and prctl, getppid()==1 (init).
        if libc::getppid() == 1 {
            eprintln!("dql server: parent already exited, shutting down");
            std::process::exit(0);
        }
    }

    // Clean up stale socket from a previous crashed run.
    // Try connecting first — if something is already listening, bail rather
    // than stealing the socket out from under a live server.
    if socket_path.exists() {
        match UnixStream::connect(socket_path) {
            Ok(_) => {
                return Err(anyhow::anyhow!(
                    "Another server is already listening on {}",
                    socket_path.display()
                ));
            }
            Err(_) => {
                // Nobody is listening — stale socket from a crashed process.
                std::fs::remove_file(socket_path)?;
            }
        }
    }

    let listener = UnixListener::bind(socket_path)
        .map_err(|e| anyhow::anyhow!("Failed to bind {}: {}", socket_path.display(), e))?;

    // Machine-readable socket path on stdout (for scripting)
    println!("{}", socket_path.display());

    eprintln!(
        "dql server: listening on {} ({} workers, db={}, idle_timeout={}, socket_idle_timeout={})",
        socket_path.display(),
        num_workers,
        db_path.unwrap_or("<none>"),
        idle_timeout.map_or("disabled".to_string(), |t| format!("{}s", t)),
        socket_idle_timeout.map_or("disabled".to_string(), |t| format!("{}s", t)),
    );

    // Shared state for idle timeout and shutdown
    let last_activity = Arc::new(AtomicU64::new(now_secs()));
    let shutdown = Arc::new(AtomicBool::new(false));
    let active_connections = Arc::new(AtomicU32::new(0));
    // Timestamp when active_connections last dropped to zero.
    // Initialized to now so the timer doesn't fire immediately on startup.
    let disconnected_since = Arc::new(AtomicU64::new(now_secs()));

    // Channel for dispatching connections to workers
    let (tx, rx) = mpsc::channel::<UnixStream>();
    let rx = Arc::new(std::sync::Mutex::new(rx));

    // Our resolver/refiner stack frames are large in debug builds (~100KB+).
    // Increase the red zone so stacker grows the stack before we overflow.
    stacksafe::set_minimum_stack_size(512 * 1024);

    // Spawn worker threads — each creates its own ConnectionManager + System
    let db_path_owned = db_path.map(|s| s.to_string());
    let mut workers = Vec::with_capacity(num_workers);

    for worker_id in 0..num_workers {
        let rx = Arc::clone(&rx);
        let db = db_path_owned.clone();
        let last_activity = Arc::clone(&last_activity);
        let shutdown = Arc::clone(&shutdown);
        let active_connections = Arc::clone(&active_connections);
        let disconnected_since = Arc::clone(&disconnected_since);

        let handle = std::thread::Builder::new()
            .name(format!("dql-worker-{}", worker_id))
            .spawn(move || {
                worker_loop(
                    worker_id,
                    db.as_deref(),
                    rx,
                    last_activity,
                    shutdown,
                    active_connections,
                    disconnected_since,
                );
            })?;
        workers.push(handle);
    }

    // Non-blocking accept loop with idle timeout and shutdown support
    listener.set_nonblocking(true)?;
    loop {
        if shutdown.load(Ordering::Relaxed) {
            eprintln!("dql server: shutdown requested");
            break;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                // On macOS, accept() on a non-blocking listener returns a
                // non-blocking stream.  Workers expect blocking I/O.
                let _ = stream.set_nonblocking(false);
                last_activity.store(now_secs(), Ordering::Relaxed);
                if tx.send(stream).is_err() {
                    break; // All workers crashed
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if let Some(timeout) = idle_timeout {
                    let elapsed = now_secs() - last_activity.load(Ordering::Relaxed);
                    if elapsed >= timeout {
                        eprintln!("dql server: idle timeout ({}s)", timeout);
                        break;
                    }
                }
                if let Some(timeout) = socket_idle_timeout {
                    if active_connections.load(Ordering::Relaxed) == 0 {
                        let elapsed = now_secs() - disconnected_since.load(Ordering::Relaxed);
                        if elapsed >= timeout {
                            eprintln!(
                                "dql server: socket idle timeout ({}s, no active connections)",
                                timeout
                            );
                            break;
                        }
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(200));
            }
            Err(e) => {
                eprintln!("server: accept error: {}", e);
            }
        }
    }

    // Drop sender so workers exit their recv loop
    drop(tx);
    for w in workers {
        let _ = w.join();
    }

    let _ = std::fs::remove_file(socket_path);
    eprintln!("dql server: stopped");

    Ok(())
}

/// Worker loop: initialize a dedicated ConnectionManager + System, then
/// pull connections from the channel and serve them one at a time.
fn worker_loop(
    worker_id: usize,
    db_path: Option<&str>,
    rx: Arc<std::sync::Mutex<mpsc::Receiver<UnixStream>>>,
    last_activity: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    active_connections: Arc<AtomicU32>,
    disconnected_since: Arc<AtomicU64>,
) {
    let conn_manager = match ConnectionManager::new_memory() {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "worker-{}: failed to create connection manager: {}",
                worker_id, e
            );
            return;
        }
    };

    let mut handle = match conn_manager.open_handle() {
        Ok(h) => h,
        Err(e) => {
            eprintln!("worker-{}: failed to init system: {}", worker_id, e);
            return;
        }
    };

    // If a db path was given, mount! it as "main" via a session
    if let Some(path) = db_path {
        match handle.session() {
            Ok(mut session) => {
                let mount_dql = format!("mount!(\"{}\", \"main\")", path);
                if let Err(e) = crate::exec_ng::run_dql_query(&mount_dql, &mut *session) {
                    eprintln!("worker-{}: failed to mount database: {}", worker_id, e);
                    return;
                }
            }
            Err(e) => {
                eprintln!(
                    "worker-{}: failed to create session for mount: {}",
                    worker_id, e
                );
                return;
            }
        }
    }

    loop {
        let stream = {
            let guard = rx.lock().unwrap();
            guard.recv()
        };
        match stream {
            Ok(stream) => {
                active_connections.fetch_add(1, Ordering::Relaxed);
                handler::serve_connection(stream, &mut *handle, &last_activity, &shutdown);
                let prev = active_connections.fetch_sub(1, Ordering::Relaxed);
                if prev == 1 {
                    // We were the last active connection — start the socket idle clock
                    disconnected_since.store(now_secs(), Ordering::Relaxed);
                }
            }
            Err(_) => break,
        }
    }
}
