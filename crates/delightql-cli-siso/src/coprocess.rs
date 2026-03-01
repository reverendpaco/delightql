use std::io::{BufRead, BufReader, BufWriter, Write};
use std::os::unix::process::CommandExt;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use crate::error::{PipeError, Result};
use crate::parse;
use crate::profile::{OutputFormat, PipeProfile, TargetMode};

/// A running coprocess with framed I/O.
pub struct Coprocess {
    child: Child,
    stdin: BufWriter<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    stderr_thread: Option<thread::JoinHandle<String>>,
}

impl Coprocess {
    /// Spawn a coprocess from a profile and optional target path.
    pub fn spawn(profile: &PipeProfile, target: Option<&str>) -> Result<Self> {
        let mut cmd = Command::new(&profile.binary);

        // Add CLI flags
        for flag in &profile.cli_flags {
            cmd.arg(flag);
        }

        // Add target based on mode
        match &profile.target_mode {
            TargetMode::None => {}
            TargetMode::Positional => {
                if let Some(t) = target {
                    cmd.arg(t);
                }
            }
            TargetMode::Flag(flag_name) => {
                if let Some(t) = target {
                    cmd.arg(flag_name).arg(t);
                }
            }
        }

        if !profile.env_vars.is_empty() {
            cmd.envs(&profile.env_vars);
        }

        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        // Ask the kernel to SIGTERM the child when the parent dies.
        // prctl(PR_SET_PDEATHSIG) is Linux-only; no macOS equivalent.
        #[cfg(target_os = "linux")]
        unsafe {
            cmd.pre_exec(|| {
                libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
                Ok(())
            });
        }

        let mut child = cmd.spawn().map_err(|e| PipeError::SpawnFailed {
            binary: profile.binary.clone(),
            source: e,
        })?;

        let stdin = child.stdin.take().ok_or(PipeError::StdinUnavailable)?;
        let stdout = child.stdout.take().ok_or(PipeError::StdoutUnavailable)?;
        let stderr = child.stderr.take().ok_or(PipeError::StderrUnavailable)?;

        // Drain stderr in background to prevent blocking
        let stderr_thread = thread::spawn(move || {
            let reader = BufReader::new(stderr);
            let mut collected = String::new();
            for line in reader.lines() {
                if let Ok(line) = line {
                    log::debug!("coprocess stderr: {}", line);
                    collected.push_str(&line);
                    collected.push('\n');
                }
            }
            collected
        });

        let mut coprocess = Coprocess {
            child,
            stdin: BufWriter::new(stdin),
            stdout: BufReader::new(stdout),
            stderr_thread: Some(stderr_thread),
        };

        // Send setup commands
        for setup_cmd in &profile.setup_commands {
            writeln!(coprocess.stdin, "{}", setup_cmd)?;
        }
        coprocess.stdin.flush()?;

        // If there are setup commands, drain their output with a sentinel
        if !profile.setup_commands.is_empty() {
            let setup_id = uuid::Uuid::new_v4();
            let sentinel = format!("__DQL_SETUP_{}", setup_id);
            writeln!(
                coprocess.stdin,
                "SELECT '{}' AS __dql_marker;",
                sentinel
            )?;
            coprocess.stdin.flush()?;

            // Read until we see the setup sentinel
            let mut line = String::new();
            loop {
                line.clear();
                let n = coprocess.stdout.read_line(&mut line)?;
                if n == 0 {
                    return Err(PipeError::ProcessExited {
                        stderr: coprocess.drain_stderr(),
                    });
                }
                if line.contains(&sentinel) {
                    break;
                }
            }
        }

        Ok(coprocess)
    }

    /// Execute a SQL query through the coprocess and return the raw output text.
    ///
    /// Uses UUID-based framing to delimit query output.
    pub fn execute_framed(&mut self, sql: &str, profile: &PipeProfile) -> Result<String> {
        let frame_id = uuid::Uuid::new_v4();
        let start_sentinel = format!("__DQL_FRAME_START_{}", frame_id);
        let end_sentinel = format!("__DQL_FRAME_END_{}", frame_id);

        // Write framed query
        writeln!(
            self.stdin,
            "SELECT '{}' AS __dql_marker;",
            start_sentinel
        )?;
        // Ensure SQL ends with semicolon
        let sql_trimmed = sql.trim().trim_end_matches(';');
        writeln!(self.stdin, "{};", sql_trimmed)?;
        writeln!(
            self.stdin,
            "SELECT '{}' AS __dql_marker;",
            end_sentinel
        )?;
        self.stdin.flush()?;

        // Read output between sentinels.
        //
        // With headers=true, each SELECT produces a header line + data line(s).
        // The start sentinel SELECT produces:
        //     __dql_marker          <- header (before we enter the frame)
        //     __DQL_FRAME_START_xxx <- data (triggers inside_frame)
        // The actual query produces:
        //     col1,col2,...         <- header (captured)
        //     val1,val2,...         <- data rows (captured)
        // The end sentinel SELECT produces:
        //     __dql_marker          <- header (must NOT be captured)
        //     __DQL_FRAME_END_xxx   <- data (triggers break)
        //
        // Strategy: enter the frame when we see the start sentinel line.
        // Capture lines while inside_frame. Check for end sentinel BEFORE
        // appending, and also filter out the end sentinel's header line.
        let mut output = String::new();
        let mut inside_frame = false;
        let mut line = String::new();

        loop {
            line.clear();
            let n = self.stdout.read_line(&mut line)?;
            if n == 0 {
                return Err(PipeError::ProcessExited {
                    stderr: self.drain_stderr(),
                });
            }

            // Check for end sentinel first (both header and data lines)
            if line.contains(&end_sentinel) {
                break;
            }

            // Check for start sentinel
            if line.contains(&start_sentinel) {
                inside_frame = true;
                continue; // Don't capture the start sentinel line itself
            }

            if inside_frame {
                // Skip the end sentinel's header line.
                // When headers=true, the end sentinel SELECT produces a header
                // "__dql_marker" before its data line. We detect this by checking
                // if the trimmed line equals "__dql_marker".
                if profile.headers && line.trim().eq_ignore_ascii_case("__dql_marker") {
                    continue;
                }
                output.push_str(&line);
            }
        }

        Ok(output)
    }

    /// Check if the coprocess is still running.
    pub fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// Collect any stderr output from the background thread.
    fn drain_stderr(&mut self) -> String {
        self.stderr_thread
            .take()
            .and_then(|h| h.join().ok())
            .unwrap_or_default()
            .trim()
            .to_string()
    }
}

impl Drop for Coprocess {
    fn drop(&mut self) {
        // 1. Ask politely: send .quit and close stdin (EOF)
        let _ = writeln!(self.stdin, ".quit");
        let _ = self.stdin.flush();

        // 2. Wait up to 500ms for graceful exit
        let deadline = Instant::now() + Duration::from_millis(500);
        loop {
            if matches!(self.child.try_wait(), Ok(Some(_))) {
                return;
            }
            if Instant::now() >= deadline {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }

        // 3. Force kill and reap
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A shared coprocess handle. All consumers (schema, introspector,
/// connection, query executor) lock the same mutex.
pub struct SharedCoprocess {
    coprocess: Mutex<Coprocess>,
    profile: PipeProfile,
}

impl SharedCoprocess {
    pub fn new(coprocess: Coprocess, profile: PipeProfile) -> Self {
        Self {
            coprocess: Mutex::new(coprocess),
            profile,
        }
    }

    pub fn execute_query_raw(
        &self,
        sql: &str,
    ) -> std::result::Result<(Vec<String>, Vec<Vec<String>>), PipeError> {
        let mut cp = self.coprocess.lock().unwrap();
        let output = cp.execute_framed(sql, &self.profile)?;
        if output.trim().is_empty() {
            return Ok((vec![], vec![]));
        }
        match self.profile.output_format {
            OutputFormat::Csv => parse::parse_csv(&output, self.profile.headers),
            OutputFormat::Tsv => parse::parse_tsv(&output, self.profile.headers),
        }
    }

    pub fn profile(&self) -> &PipeProfile {
        &self.profile
    }
}

unsafe impl Send for SharedCoprocess {}
unsafe impl Sync for SharedCoprocess {}
