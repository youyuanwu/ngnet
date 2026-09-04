//! Serial process supervisor for native HTTP/3 S9 reliability runs.
//!
//! Usage:
//! `s9_supervisor <probe> <arm> <mode> <body-bytes> <exchanges> <runs> <outer-seconds> [start-run] [manifest]`
//! `s9_supervisor fixture <test-name> <runs> <outer-seconds> [start-run] [manifest]`
//!
//! The optional start number makes a resumed invocation explicit without storing hidden state.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MAX_INPUT_RECORD_BYTES: usize = 64 * 1024;
const MAX_FAILURE_EVIDENCE_BYTES: usize = 96 * 1024 * 1024;
const MAX_FALLBACK_TAIL_BYTES: usize = 1024 * 1024;
const CUMULATIVE_EVIDENCE_BYTES: u64 = 12 * 1024 * 1024 * 1024;
const STORAGE_MARGIN_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_ATTEMPT_MILLIS: u64 = 24 * 60 * 60 * 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Completed,
    ClassifiedFailure,
    OuterKilled,
    UnclassifiedFailure,
    CleanupFailure,
}

#[derive(Default)]
struct StreamCapture {
    classified_failure: bool,
    success_marker: bool,
    last_checkpoint: Option<String>,
    last_metadata: Option<String>,
    failure_detail: Option<String>,
    evidence: VecDeque<String>,
    evidence_bytes: usize,
    fallback_tail: VecDeque<String>,
    fallback_tail_bytes: usize,
    failure_seen: bool,
    evidence_truncated: bool,
    invalid_records: usize,
    diagnostic_counts: BTreeMap<usize, usize>,
    liveness_counts: BTreeMap<usize, usize>,
    max_dropped_attempts: u64,
    max_dropped_liveness: u64,
}

impl StreamCapture {
    fn observe(&mut self, line: String, invalid: bool, expected_completion: Option<&str>) {
        if invalid {
            self.invalid_records = self.invalid_records.saturating_add(1);
        }
        if line.starts_with("PROBE-CHECKPOINT") || line.starts_with("S9-FIXTURE-CHECKPOINT") {
            self.last_checkpoint = Some(line.clone());
        }
        if line.starts_with("PROBE-METADATA") {
            self.last_metadata = Some(line.clone());
        }
        if line.starts_with("PROBE-FAIL")
            || line.starts_with("S9-FIXTURE-FAIL")
            || line.contains("stalled; last completed exchange")
            || line.contains("failed; last completed exchange")
        {
            self.classified_failure = true;
            self.failure_seen = true;
            self.failure_detail.get_or_insert_with(|| line.clone());
        }
        if line.contains("test result: ok. 1 passed")
            || expected_completion
                .is_some_and(|expected| line == format!("PROBE-DONE completed={expected}"))
        {
            self.success_marker = true;
        }
        if let Some(exchange) = field(&line, "exchange").and_then(|value| value.parse().ok()) {
            if line.starts_with("PROBE-DIAGNOSTIC") {
                *self.diagnostic_counts.entry(exchange).or_default() += 1;
            } else if line.starts_with("PROBE-LIVENESS") {
                *self.liveness_counts.entry(exchange).or_default() += 1;
            } else if line.starts_with("PROBE-SNAPSHOT") {
                self.max_dropped_attempts = self.max_dropped_attempts.max(
                    field(&line, "dropped_attempt_records")
                        .and_then(|value| value.parse().ok())
                        .unwrap_or(0),
                );
                self.max_dropped_liveness = self.max_dropped_liveness.max(
                    field(&line, "dropped_liveness_records")
                        .and_then(|value| value.parse().ok())
                        .unwrap_or(0),
                );
            }
        }
        push_bounded(
            &mut self.fallback_tail,
            &mut self.fallback_tail_bytes,
            line.clone(),
            MAX_FALLBACK_TAIL_BYTES,
            None,
        );
        if self.failure_seen && is_failure_evidence(&line) {
            let truncated = push_bounded(
                &mut self.evidence,
                &mut self.evidence_bytes,
                line,
                MAX_FAILURE_EVIDENCE_BYTES,
                Some(&["PROBE-FAIL", "S9-FIXTURE-FAIL"]),
            );
            self.evidence_truncated |= truncated;
        }
    }

    fn max_diagnostics(&self) -> usize {
        self.diagnostic_counts.values().copied().max().unwrap_or(0)
    }

    fn max_liveness(&self) -> usize {
        self.liveness_counts.values().copied().max().unwrap_or(0)
    }
}

fn field<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    let prefix = format!("{name}=");
    line.split_whitespace()
        .find_map(|value| value.strip_prefix(&prefix))
}

fn is_failure_evidence(line: &str) -> bool {
    [
        "PROBE-FAIL",
        "S9-FIXTURE-FAIL",
        "PROBE-DIAGNOSTIC",
        "PROBE-LIVENESS",
        "PROBE-SNAPSHOT",
        "PROBE-RSS",
        "PROBE-SYMMETRIC",
        "PROBE-CHECKPOINT",
        "S9-FIXTURE-CHECKPOINT",
        "PROBE-METADATA",
    ]
    .iter()
    .any(|prefix| line.starts_with(prefix))
}

fn is_live_line(line: &str) -> bool {
    [
        "PROBE-READY",
        "PROBE-CHECKPOINT",
        "S9-FIXTURE-CHECKPOINT",
        "PROBE-FAIL",
        "S9-FIXTURE-FAIL",
        "PROBE-DONE",
    ]
    .iter()
    .any(|prefix| line.starts_with(prefix))
}

fn push_bounded(
    lines: &mut VecDeque<String>,
    bytes: &mut usize,
    line: String,
    limit: usize,
    preserve_prefixes: Option<&[&str]>,
) -> bool {
    if let Some(prefixes) = preserve_prefixes
        && let Some(prefix) = prefixes.iter().find(|prefix| line.starts_with(**prefix))
    {
        while let Some(index) = lines
            .iter()
            .position(|candidate| candidate.starts_with(*prefix))
        {
            let removed = lines.remove(index).expect("preserved line index exists");
            *bytes = bytes.saturating_sub(removed.len().saturating_add(1));
        }
    }
    let size = line.len().saturating_add(1);
    lines.push_back(line);
    *bytes = bytes.saturating_add(size);
    let mut truncated = false;
    while *bytes > limit {
        let removable = lines.iter().position(|candidate| {
            !preserve_prefixes
                .is_some_and(|prefixes| prefixes.iter().any(|prefix| candidate.starts_with(prefix)))
        });
        let index = removable.unwrap_or(0);
        let removed = lines.remove(index).expect("bounded line index exists");
        *bytes = bytes.saturating_sub(removed.len().saturating_add(1));
        truncated = true;
    }
    truncated
}

fn read_bounded_line<R: BufRead>(reader: &mut R) -> io::Result<Option<(String, bool)>> {
    let mut bytes = Vec::new();
    let mut invalid = false;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            if bytes.is_empty() {
                return Ok(None);
            }
            invalid = true;
            break;
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |index| index + 1);
        let payload = newline.map_or(&available[..consumed], |index| &available[..index]);
        let remaining = MAX_INPUT_RECORD_BYTES.saturating_sub(bytes.len());
        bytes.extend_from_slice(&payload[..payload.len().min(remaining)]);
        if payload.len() > remaining {
            invalid = true;
        }
        reader.consume(consumed);
        if newline.is_some() {
            break;
        }
    }
    let line = String::from_utf8(bytes).unwrap_or_else(|error| {
        invalid = true;
        String::from_utf8_lossy(error.as_bytes()).into_owned()
    });
    Ok(Some((line, invalid)))
}

fn capture_stream<R: Read + Send + 'static>(
    stream: R,
    expected_completion: Option<String>,
    stderr: bool,
) -> thread::JoinHandle<io::Result<StreamCapture>> {
    thread::spawn(move || {
        let mut reader = BufReader::new(stream);
        let mut capture = StreamCapture::default();
        while let Some((line, invalid)) = read_bounded_line(&mut reader)? {
            if is_live_line(&line) {
                if stderr {
                    eprintln!("{line}");
                } else {
                    println!("{line}");
                }
            }
            capture.observe(line, invalid, expected_completion.as_deref());
        }
        Ok(capture)
    })
}

fn classify(
    code: Option<i32>,
    stderr: &str,
    cleanup_failed: bool,
    success_marker: bool,
) -> Outcome {
    if cleanup_failed {
        Outcome::CleanupFailure
    } else if code == Some(0) && success_marker {
        Outcome::Completed
    } else if code == Some(124) || code == Some(137) {
        Outcome::OuterKilled
    } else if stderr.contains("PROBE-FAIL")
        || stderr.contains("S9-FIXTURE-FAIL")
        || stderr.contains("stalled; last completed exchange")
        || stderr.contains("failed; last completed exchange")
    {
        Outcome::ClassifiedFailure
    } else {
        Outcome::UnclassifiedFailure
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ProcessIdentity {
    pid: u32,
    start_time: u64,
}

fn process_identity(pid: u32) -> Result<ProcessIdentity, String> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .map_err(|error| format!("reading identity for pid {pid}: {error}"))?;
    let end = stat
        .rfind(')')
        .ok_or_else(|| format!("malformed stat for pid {pid}"))?;
    let fields = stat[end + 1..].split_whitespace().collect::<Vec<_>>();
    let start_time = fields
        .get(19)
        .ok_or_else(|| format!("missing start time for pid {pid}"))?
        .parse::<u64>()
        .map_err(|error| format!("invalid start time for pid {pid}: {error}"))?;
    Ok(ProcessIdentity { pid, start_time })
}

fn process_group_pids(group: u32) -> Result<Vec<ProcessIdentity>, String> {
    let output = Command::new("ps")
        .args(["-eo", "pid=,pgid="])
        .output()
        .map_err(|error| format!("running ps: {error}"))?;
    if !output.status.success() {
        return Err(format!("ps failed with {}", output.status));
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| {
            let mut fields = line.split_whitespace();
            let pid = fields
                .next()
                .ok_or_else(|| format!("missing pid in ps row {line:?}"))?
                .parse::<u32>()
                .map_err(|error| format!("invalid pid in ps row {line:?}: {error}"))?;
            let pgid = fields
                .next()
                .ok_or_else(|| format!("missing pgid in ps row {line:?}"))?
                .parse::<u32>()
                .map_err(|error| format!("invalid pgid in ps row {line:?}: {error}"))?;
            Ok((pid, pgid))
        })
        .collect::<Result<Vec<_>, String>>()?
        .into_iter()
        .filter(|(pid, pgid)| *pgid == group && *pid != std::process::id())
        .map(|(pid, _)| process_identity(pid))
        .collect()
}

fn child_pids(parent: u32) -> Result<Vec<u32>, String> {
    let path = format!("/proc/{parent}/task/{parent}/children");
    let contents =
        std::fs::read_to_string(&path).map_err(|error| format!("reading {path}: {error}"))?;
    contents
        .split_whitespace()
        .map(|pid| {
            pid.parse::<u32>()
                .map_err(|error| format!("invalid child pid {pid:?}: {error}"))
        })
        .collect()
}

fn descendant_identities(root: u32) -> Result<Vec<ProcessIdentity>, String> {
    let mut pending = vec![root];
    let mut found = BTreeMap::new();
    while let Some(parent) = pending.pop() {
        let children = match child_pids(parent) {
            Ok(children) => children,
            Err(_) if !std::path::Path::new(&format!("/proc/{parent}")).exists() => continue,
            Err(error) => return Err(error),
        };
        for child in children {
            pending.push(child);
            match process_identity(child) {
                Ok(identity) => {
                    found.insert(child, identity);
                }
                Err(_) if !std::path::Path::new(&format!("/proc/{child}")).exists() => {}
                Err(error) => return Err(error),
            }
        }
    }
    Ok(found.into_values().collect())
}

fn still_same_process(identity: ProcessIdentity) -> Result<bool, String> {
    match process_identity(identity.pid) {
        Ok(current) => Ok(current == identity),
        Err(_) if !std::path::Path::new(&format!("/proc/{}", identity.pid)).exists() => Ok(false),
        Err(error) => Err(error),
    }
}

fn terminate_pids(pids: &[ProcessIdentity], signal: &str) {
    for identity in pids {
        let _ = Command::new("kill")
            .args([signal, "--", &identity.pid.to_string()])
            .status();
    }
}

#[cfg(test)]
fn last_checkpoint(stderr: &str) -> &str {
    stderr
        .lines()
        .rev()
        .find(|line| {
            line.starts_with("PROBE-CHECKPOINT") || line.starts_with("S9-FIXTURE-CHECKPOINT")
        })
        .unwrap_or("unavailable")
}

#[cfg(test)]
fn manifest_has_completed_run(contents: &str, run: usize) -> bool {
    let target = format!("run={run}");
    contents
        .lines()
        .filter(|line| line.starts_with("S9-SUPERVISOR-RESULT"))
        .any(|line| line.split_whitespace().any(|field| field == target))
}

#[derive(Clone, Debug)]
struct DanglingAttempt {
    run: usize,
    attempt: String,
    identity: Option<ProcessIdentity>,
    started_unix_ms: u128,
}

#[derive(Default)]
struct ManifestState {
    completed_runs: BTreeSet<usize>,
    dangling: BTreeMap<(usize, String), DanglingAttempt>,
    revision: Option<String>,
    supervisor_sha256: Option<String>,
    workload_sha256: Option<String>,
}

fn scan_manifest(path: &str) -> Result<ManifestState, String> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(ManifestState::default());
        }
        Err(error) => return Err(format!("opening S9 manifest {path}: {error}")),
    };
    scan_manifest_reader(BufReader::new(file))
        .map_err(|error| format!("reading S9 manifest {path}: {error}"))
}

fn scan_manifest_reader<R: BufRead>(reader: R) -> Result<ManifestState, String> {
    let mut state = ManifestState::default();
    for line in reader.lines() {
        let line = line.map_err(|error| error.to_string())?;
        if line.starts_with("S9-SUPERVISOR-METADATA") {
            state.revision = field(&line, "revision").map(ToString::to_string);
            state.supervisor_sha256 = field(&line, "supervisor_sha256").map(ToString::to_string);
            state.workload_sha256 = field(&line, "workload_sha256").map(ToString::to_string);
        } else if line.starts_with("S9-SUPERVISOR-START") {
            if let (Some(run), Some(attempt)) = (
                field(&line, "run").and_then(|value| value.parse().ok()),
                field(&line, "attempt"),
            ) {
                let identity = field(&line, "timeout_pid")
                    .and_then(|value| value.parse().ok())
                    .zip(field(&line, "start_time").and_then(|value| value.parse().ok()))
                    .map(|(pid, start_time)| ProcessIdentity { pid, start_time });
                let started_unix_ms = field(&line, "started_unix_ms")
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(0);
                state.dangling.insert(
                    (run, attempt.to_string()),
                    DanglingAttempt {
                        run,
                        attempt: attempt.to_string(),
                        identity,
                        started_unix_ms,
                    },
                );
            }
        } else if line.starts_with("S9-SUPERVISOR-RESULT") {
            if let Some(run) = field(&line, "run").and_then(|value| value.parse().ok()) {
                state.completed_runs.insert(run);
                if let Some(attempt) = field(&line, "attempt") {
                    state.dangling.remove(&(run, attempt.to_string()));
                } else {
                    state.dangling.retain(|(candidate, _), _| *candidate != run);
                }
            }
        } else if line.starts_with("S9-SUPERVISOR-INTERRUPTED")
            && let (Some(run), Some(attempt)) = (
                field(&line, "run").and_then(|value| value.parse().ok()),
                field(&line, "attempt"),
            )
        {
            state.dangling.remove(&(run, attempt.to_string()));
        }
    }
    Ok(state)
}

#[cfg(test)]
fn probe_completed_exactly(output: &str, expected: &str) -> bool {
    let marker = format!("PROBE-DONE completed={expected}");
    output.lines().any(|line| line == marker)
}

fn append_manifest(path: Option<&str>, record: &str) -> Result<(), String> {
    let Some(path) = path else {
        return Ok(());
    };
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| format!("opening S9 manifest {path}: {error}"))?;
    writeln!(file, "{record}").map_err(|error| format!("writing S9 manifest {path}: {error}"))?;
    file.flush()
        .map_err(|error| format!("flushing S9 manifest {path}: {error}"))
}

fn append_evidence(
    path: Option<&str>,
    run: usize,
    attempt: &str,
    revision: &str,
    lines: impl IntoIterator<Item = String>,
) -> Result<bool, String> {
    let Some(path) = path else {
        return Ok(false);
    };
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| format!("opening S9 manifest {path}: {error}"))?;
    let mut written = 0usize;
    let mut truncated = false;
    for line in lines {
        let escaped = escape_manifest(&line);
        let record = format!(
            "S9-SUPERVISOR-EVIDENCE run={run} attempt={attempt} revision={revision} line={escaped}"
        );
        let bytes = record.len().saturating_add(1);
        if written.saturating_add(bytes) > MAX_FAILURE_EVIDENCE_BYTES {
            truncated = true;
            continue;
        }
        writeln!(file, "{record}")
            .map_err(|error| format!("writing S9 manifest {path}: {error}"))?;
        written = written.saturating_add(bytes);
    }
    file.flush()
        .map_err(|error| format!("flushing S9 manifest {path}: {error}"))?;
    Ok(truncated)
}

fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn sha256(path: &Path) -> Result<String, String> {
    let output = Command::new("sha256sum")
        .arg(path)
        .output()
        .map_err(|error| format!("hashing {}: {error}", path.display()))?;
    if !output.status.success() {
        return Err(format!(
            "hashing {} failed with {}",
            path.display(),
            output.status
        ));
    }
    String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .next()
        .map(ToString::to_string)
        .ok_or_else(|| format!("sha256sum returned no digest for {}", path.display()))
}

fn escape_manifest(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            _ => escaped.push(character),
        }
    }
    escaped
}

fn directory_bytes(path: &Path) -> Result<u64, String> {
    let mut total = 0u64;
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => {
            return Err(format!(
                "reading evidence directory {}: {error}",
                path.display()
            ));
        }
    };
    for entry in entries {
        let entry = entry.map_err(|error| format!("reading evidence entry: {error}"))?;
        let metadata = entry
            .metadata()
            .map_err(|error| format!("reading {} metadata: {error}", entry.path().display()))?;
        if metadata.is_dir() {
            total = total.saturating_add(directory_bytes(&entry.path())?);
        } else {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

fn available_bytes(path: &Path) -> Result<u64, String> {
    let output = Command::new("df")
        .args(["-Pk", &path.display().to_string()])
        .output()
        .map_err(|error| format!("checking free space for {}: {error}", path.display()))?;
    if !output.status.success() {
        return Err(format!("df failed with {}", output.status));
    }
    let line = String::from_utf8_lossy(&output.stdout)
        .lines()
        .last()
        .ok_or_else(|| "df returned no filesystem row".to_string())?
        .to_string();
    let blocks = line
        .split_whitespace()
        .nth(3)
        .ok_or_else(|| format!("df row missing available blocks: {line}"))?
        .parse::<u64>()
        .map_err(|error| format!("invalid df available blocks: {error}"))?;
    Ok(blocks.saturating_mul(1024))
}

fn evidence_root(manifest: Option<&str>) -> Option<PathBuf> {
    let revision = Path::new(manifest?).parent()?;
    revision.parent().map(Path::to_path_buf)
}

fn elapsed_attempt_millis(path: &Path) -> Result<u64, String> {
    let ledger = path.join(".s9-evidence-reservations");
    let file = match File::open(&ledger) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(format!("opening campaign time ledger: {error}")),
    };
    BufReader::new(file).lines().try_fold(0u64, |total, line| {
        let line = line.map_err(|error| format!("reading campaign time ledger: {error}"))?;
        Ok(if line.starts_with("S9-EVIDENCE-TIME") {
            total.saturating_add(
                field(&line, "elapsed_ms")
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(0),
            )
        } else {
            total
        })
    })
}

fn record_attempt_elapsed(
    manifest: Option<&str>,
    attempt: &str,
    elapsed_ms: u128,
) -> Result<(), String> {
    let Some(root) = evidence_root(manifest) else {
        return Ok(());
    };
    let ledger = root.join(".s9-evidence-reservations");
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(&ledger)
        .map_err(|error| format!("opening campaign time ledger: {error}"))?;
    file.lock()
        .map_err(|error| format!("locking campaign time ledger: {error}"))?;
    let mut contents = String::new();
    file.seek(SeekFrom::Start(0))
        .and_then(|_| file.read_to_string(&mut contents))
        .map_err(|error| format!("reading campaign time ledger: {error}"))?;
    if contents.lines().any(|line| {
        line.starts_with("S9-EVIDENCE-TIME")
            && field(line, "attempt").is_some_and(|value| value == attempt)
    }) {
        file.unlock()
            .map_err(|error| format!("unlocking campaign time ledger: {error}"))?;
        return Ok(());
    }
    file.seek(SeekFrom::End(0))
        .map_err(|error| format!("seeking campaign time ledger: {error}"))?;
    writeln!(
        file,
        "S9-EVIDENCE-TIME attempt={attempt} elapsed_ms={elapsed_ms}"
    )
    .and_then(|_| file.flush())
    .map_err(|error| format!("writing campaign time ledger: {error}"))?;
    file.unlock()
        .map_err(|error| format!("unlocking campaign time ledger: {error}"))
}

struct ReservationGuard {
    ledger: PathBuf,
    id: String,
    bytes: u64,
    manifest: Option<PathBuf>,
    initial_manifest_bytes: u64,
}

impl Drop for ReservationGuard {
    fn drop(&mut self) {
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&self.ledger)
            && file.lock().is_ok()
        {
            let actual_bytes = self
                .manifest
                .as_ref()
                .and_then(|path| std::fs::metadata(path).ok())
                .map_or(0, |metadata| {
                    metadata.len().saturating_sub(self.initial_manifest_bytes)
                });
            let _ = writeln!(
                file,
                "S9-EVIDENCE-RELEASE id={} bytes={} actual_bytes={actual_bytes}",
                self.id, self.bytes,
            );
            let _ = file.flush();
            let _ = file.unlock();
        }
    }
}

fn reserve_storage(
    manifest: Option<&str>,
    runs: usize,
) -> Result<Option<ReservationGuard>, String> {
    let Some(root) = evidence_root(manifest) else {
        return Ok(None);
    };
    std::fs::create_dir_all(&root)
        .map_err(|error| format!("creating evidence root {}: {error}", root.display()))?;
    let retained = directory_bytes(&root)?;
    let reservation = (runs as u64).saturating_mul(MAX_FAILURE_EVIDENCE_BYTES as u64);
    let ledger = root.join(".s9-evidence-reservations");
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(&ledger)
        .map_err(|error| format!("opening evidence reservation ledger: {error}"))?;
    file.lock()
        .map_err(|error| format!("locking evidence reservation ledger: {error}"))?;
    let mut contents = String::new();
    file.seek(SeekFrom::Start(0))
        .and_then(|_| file.read_to_string(&mut contents))
        .map_err(|error| format!("reading evidence reservation ledger: {error}"))?;
    let mut active = BTreeMap::new();
    for line in contents.lines() {
        if line.starts_with("S9-EVIDENCE-RESERVE")
            && let (Some(id), Some(bytes), pid, start_time) = (
                field(line, "id"),
                field(line, "bytes").and_then(|value| value.parse::<u64>().ok()),
                field(line, "pid").and_then(|value| value.parse::<u32>().ok()),
                field(line, "start_time").and_then(|value| value.parse::<u64>().ok()),
            )
        {
            active.insert(
                id.to_string(),
                (
                    bytes,
                    pid.zip(start_time)
                        .map(|(pid, start_time)| ProcessIdentity { pid, start_time }),
                ),
            );
        } else if line.starts_with("S9-EVIDENCE-RELEASE")
            && let Some(id) = field(line, "id")
        {
            active.remove(id);
        }
    }
    active.retain(|id, (_, identity)| {
        identity.map_or_else(
            || {
                id.split('-')
                    .next()
                    .and_then(|pid| pid.parse::<u32>().ok())
                    .is_some_and(|pid| Path::new(&format!("/proc/{pid}")).exists())
            },
            |identity| still_same_process(identity).unwrap_or(true),
        )
    });
    let active_bytes = active.values().map(|(bytes, _)| *bytes).sum::<u64>();
    if retained
        .saturating_add(active_bytes)
        .saturating_add(reservation)
        > CUMULATIVE_EVIDENCE_BYTES
    {
        let _ = file.unlock();
        return Err(format!(
            "evidence reservation would exceed {} bytes: retained={retained} active={active_bytes} requested={reservation}",
            CUMULATIVE_EVIDENCE_BYTES,
        ));
    }
    let available = available_bytes(&root)?;
    if available
        < active_bytes
            .saturating_add(reservation)
            .saturating_add(STORAGE_MARGIN_BYTES)
    {
        let _ = file.unlock();
        return Err(format!(
            "insufficient evidence space: available={available} active={active_bytes} reservation={reservation} margin={STORAGE_MARGIN_BYTES}"
        ));
    }
    let elapsed = elapsed_attempt_millis(&root)?;
    if elapsed >= MAX_ATTEMPT_MILLIS {
        let _ = file.unlock();
        return Err(format!(
            "campaign attempt-time cap reached: elapsed_ms={elapsed} cap_ms={MAX_ATTEMPT_MILLIS}"
        ));
    }
    let id = format!("{}-{}", std::process::id(), unix_millis());
    let identity = process_identity(std::process::id()).ok();
    file.seek(SeekFrom::End(0))
        .and_then(|_| {
            writeln!(
                file,
                "S9-EVIDENCE-RESERVE id={id} bytes={reservation} retained={retained} \
                 active_before={active_bytes} pid={} start_time={}",
                identity.map_or(0, |identity| identity.pid),
                identity.map_or(0, |identity| identity.start_time),
            )
        })
        .and_then(|_| file.flush())
        .map_err(|error| format!("writing evidence reservation ledger: {error}"))?;
    file.unlock()
        .map_err(|error| format!("unlocking evidence reservation ledger: {error}"))?;
    let manifest_path = manifest.map(PathBuf::from);
    let initial_manifest_bytes = manifest_path
        .as_ref()
        .and_then(|path| std::fs::metadata(path).ok())
        .map_or(0, |metadata| metadata.len());
    Ok(Some(ReservationGuard {
        ledger,
        id,
        bytes: reservation,
        manifest: manifest_path,
        initial_manifest_bytes,
    }))
}

fn resolve_fixture_binary() -> Result<PathBuf, String> {
    let output = Command::new("cargo")
        .args([
            "test",
            "-p",
            "ngnet-bench",
            "--test",
            "ngtcp2_fixture",
            "--release",
            "--no-run",
            "--message-format=json",
        ])
        .output()
        .map_err(|error| format!("building fixture binary: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "building fixture binary failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    fixture_executable_from_json(&String::from_utf8_lossy(&output.stdout))
        .ok_or_else(|| "cargo did not report the ngtcp2_fixture executable".to_string())
}

fn fixture_executable_from_json(output: &str) -> Option<PathBuf> {
    output.lines().rev().find_map(|line| {
        let marker = "\"executable\":\"";
        let start = line.find(marker)? + marker.len();
        let remainder = &line[start..];
        let end = remainder.find('"')?;
        let path = &remainder[..end];
        path.contains("ngtcp2_fixture").then(|| PathBuf::from(path))
    })
}

fn validate_outer(fixture: bool, mode: Option<&str>, outer: u64) -> Result<(), String> {
    let minimum = if fixture {
        10
    } else {
        match mode {
            Some("reliability") => 180,
            Some("diagnostic") => 685,
            _ => 10,
        }
    };
    if outer < minimum {
        return Err(format!(
            "outer-seconds {outer} is smaller than the {minimum}-second mode minimum"
        ));
    }
    Ok(())
}

fn run_range(start: usize, runs: usize) -> Result<std::ops::Range<usize>, String> {
    if start == 0 || runs == 0 {
        return Err("start-run and runs must be non-zero".to_string());
    }
    let end = start
        .checked_add(runs)
        .ok_or_else(|| "run range overflowed usize".to_string())?;
    Ok(start..end)
}

fn main() {
    let invocation_started_unix_ms = unix_millis();
    let invocation_started = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let fixture = args.get(1).is_some_and(|value| value == "fixture");
    let (runs_index, outer_index, start_index, manifest_index) =
        if fixture { (3, 4, 5, 6) } else { (6, 7, 8, 9) };
    let runs = args[runs_index].parse::<usize>().expect("runs is a number");
    let outer = args[outer_index]
        .parse::<u64>()
        .expect("outer seconds is a number");
    let start = args.get(start_index).map_or(1, |value| {
        value.parse::<usize>().expect("start is a number")
    });
    let manifest = args.get(manifest_index).map(String::as_str);
    let selected_runs = run_range(start, runs).unwrap_or_else(|error| panic!("{error}"));
    validate_outer(fixture, (!fixture).then(|| args[3].as_str()), outer)
        .unwrap_or_else(|error| panic!("{error}"));
    let revision = std::env::var("S9_REVISION").unwrap_or_else(|_| {
        assert!(
            manifest.is_none(),
            "S9_REVISION is required when a manifest is provided"
        );
        "unrecorded".to_string()
    });
    let workload = if fixture {
        resolve_fixture_binary().unwrap_or_else(|error| panic!("{error}"))
    } else {
        PathBuf::from(&args[1])
    };
    let supervisor = std::env::current_exe().expect("locate supervisor executable");
    let supervisor_sha256 = sha256(&supervisor).unwrap_or_else(|error| panic!("{error}"));
    let workload_sha256 = sha256(&workload).unwrap_or_else(|error| panic!("{error}"));
    let supervisor_bytes = std::fs::metadata(&supervisor)
        .expect("inspect supervisor executable")
        .len();
    let workload_bytes = std::fs::metadata(&workload)
        .expect("inspect workload executable")
        .len();
    let reservation_guard =
        reserve_storage(manifest, runs).unwrap_or_else(|error| panic!("campaign guard: {error}"));
    let mut manifest_state = manifest
        .map(scan_manifest)
        .transpose()
        .unwrap_or_else(|error| panic!("{error}"))
        .unwrap_or_default();

    if let Some(recorded) = manifest_state.revision.as_deref() {
        assert_eq!(
            recorded, revision,
            "manifest revision changed across resume"
        );
    }
    if let Some(recorded) = manifest_state.supervisor_sha256.as_deref() {
        assert_eq!(
            recorded, supervisor_sha256,
            "supervisor binary changed across resume"
        );
    }
    if let Some(recorded) = manifest_state.workload_sha256.as_deref() {
        assert_eq!(
            recorded, workload_sha256,
            "workload binary changed across resume"
        );
    }

    let mut interrupted = 0usize;
    for dangling in manifest_state
        .dangling
        .values()
        .cloned()
        .collect::<Vec<_>>()
    {
        let mut cleanup = "not-running".to_string();
        if let Some(identity) = dangling.identity {
            match still_same_process(identity) {
                Ok(true) => {
                    let mut identities = process_group_pids(identity.pid).unwrap_or_default();
                    if !identities.contains(&identity) {
                        identities.push(identity);
                    }
                    terminate_pids(&identities, "-TERM");
                    thread::sleep(Duration::from_millis(100));
                    let survivors = identities
                        .into_iter()
                        .filter(|candidate| still_same_process(*candidate).unwrap_or(true))
                        .collect::<Vec<_>>();
                    terminate_pids(&survivors, "-KILL");
                    thread::sleep(Duration::from_millis(100));
                    let remaining = survivors
                        .into_iter()
                        .filter(|candidate| still_same_process(*candidate).unwrap_or(true))
                        .count();
                    cleanup = if remaining == 0 {
                        "terminated".to_string()
                    } else {
                        format!("failed:{remaining}-survivors")
                    };
                }
                Ok(false) => {}
                Err(error) => cleanup = format!("inspection-error:{error}"),
            }
        }
        let ended_unix_ms = unix_millis();
        let elapsed_ms = ended_unix_ms
            .saturating_sub(dangling.started_unix_ms)
            .min(u128::from(u64::MAX));
        let record = format!(
            "S9-SUPERVISOR-INTERRUPTED run={} attempt={} revision={} \
             started_unix_ms={} ended_unix_ms={ended_unix_ms} elapsed_ms={elapsed_ms} \
             cleanup={}",
            dangling.run, dangling.attempt, revision, dangling.started_unix_ms, cleanup
        );
        append_manifest(manifest, &record).unwrap_or_else(|error| panic!("{error}"));
        record_attempt_elapsed(manifest, &dangling.attempt, elapsed_ms)
            .unwrap_or_else(|error| panic!("{error}"));
        interrupted += 1;
        manifest_state
            .dangling
            .remove(&(dangling.run, dangling.attempt));
    }
    for run in selected_runs.clone() {
        assert!(
            !manifest_state.completed_runs.contains(&run),
            "manifest already contains completed run {run}"
        );
    }
    let metadata_record = format!(
        "S9-SUPERVISOR-METADATA revision={revision} supervisor_sha256={supervisor_sha256} \
         supervisor_bytes={supervisor_bytes} workload_sha256={workload_sha256} \
         workload_bytes={workload_bytes} fixture={fixture} feature_profile={}",
        if fixture {
            "release-default"
        } else {
            "release-diagnostics"
        }
    );
    append_manifest(manifest, &metadata_record).unwrap_or_else(|error| panic!("{error}"));

    let mut completed = 0usize;
    let mut classified = 0usize;
    let mut outer_killed = 0usize;
    let mut unclassified = 0usize;
    let mut cleanup_failed = 0usize;

    for run in selected_runs {
        if let Some(root) = evidence_root(manifest) {
            let elapsed = elapsed_attempt_millis(&root).unwrap_or_else(|error| panic!("{error}"));
            if elapsed >= MAX_ATTEMPT_MILLIS {
                eprintln!(
                    "S9-SUPERVISOR-GUARD reason=attempt-time elapsed_ms={elapsed} cap_ms={MAX_ATTEMPT_MILLIS}"
                );
                break;
            }
        }

        let mut command = Command::new("setsid");
        command
            .arg("timeout")
            .args(["--signal=TERM", "--kill-after=5s", &format!("{outer}s")]);
        if fixture {
            command
                .arg(&workload)
                .args([&args[2], "--ignored", "--exact", "--nocapture"]);
        } else {
            command
                .arg(&workload)
                .args([&args[2], "body", &args[4], &args[5], &args[3]]);
        }
        let started_unix_ms = unix_millis();
        let started = Instant::now();
        let mut child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn supervised S9 cell");
        let supervisor_pid = child.id();
        let process = process_identity(supervisor_pid).ok();
        let attempt = format!("{revision}-{run}-{supervisor_pid}-{started_unix_ms}");
        let stdout_reader = capture_stream(
            child.stdout.take().expect("capture supervised stdout"),
            (!fixture).then(|| args[5].clone()),
            false,
        );
        let stderr_reader = capture_stream(
            child.stderr.take().expect("capture supervised stderr"),
            (!fixture).then(|| args[5].clone()),
            true,
        );
        let start_record = if fixture {
            format!(
                "S9-SUPERVISOR-START run={run} attempt={attempt} revision={revision} \
                 timeout_pid={supervisor_pid} start_time={} started_unix_ms={started_unix_ms} \
                 fixture={} outer_seconds={outer}",
                process.map_or(0, |identity| identity.start_time),
                args[2]
            )
        } else {
            format!(
                "S9-SUPERVISOR-START run={run} attempt={attempt} revision={revision} \
                 timeout_pid={supervisor_pid} start_time={} started_unix_ms={started_unix_ms} \
                 arm={} mode={} body={} exchanges={} outer_seconds={outer}",
                process.map_or(0, |identity| identity.start_time),
                args[2],
                args[3],
                args[4],
                args[5]
            )
        };
        eprintln!("{start_record}");
        if let Err(error) = append_manifest(manifest, &start_record) {
            let identities = process_group_pids(supervisor_pid).unwrap_or_default();
            terminate_pids(&identities, "-KILL");
            let _ = child.kill();
            let _ = child.wait();
            let _ = stdout_reader.join();
            let _ = stderr_reader.join();
            eprintln!("S9-SUPERVISOR-GUARD reason=manifest-start-write error={error:?}");
            cleanup_failed += 1;
            break;
        }
        std::io::stderr()
            .flush()
            .expect("flush supervisor start record");
        thread::sleep(Duration::from_millis(100));
        let (mut captured, mut inspection_error) = match child.try_wait() {
            Ok(Some(_)) => (Vec::new(), None),
            Ok(None) => match descendant_identities(supervisor_pid) {
                Ok(captured) => (captured, None),
                Err(error) => (Vec::new(), Some(error)),
            },
            Err(error) => (
                Vec::new(),
                Some(format!("checking supervised process: {error}")),
            ),
        };
        let wait_deadline = Instant::now() + Duration::from_secs(outer.saturating_add(10));
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < wait_deadline => {
                    match descendant_identities(supervisor_pid) {
                        Ok(descendants) => {
                            for identity in descendants {
                                if !captured.contains(&identity) {
                                    captured.push(identity);
                                }
                            }
                        }
                        Err(error) => inspection_error = Some(error),
                    }
                    thread::sleep(Duration::from_millis(50));
                }
                Ok(None) => {
                    inspection_error =
                        Some("supervisor wait exceeded outer bound plus grace".to_string());
                    let identities =
                        process_group_pids(supervisor_pid).unwrap_or_else(|_| Vec::new());
                    terminate_pids(&identities, "-KILL");
                    let _ = child.kill();
                    break child.wait().expect("reap over-bound supervisor");
                }
                Err(error) => {
                    inspection_error = Some(format!("waiting for supervised process: {error}"));
                    let _ = child.kill();
                    break child.wait().expect("reap failed supervisor wait");
                }
            }
        };
        match descendant_identities(supervisor_pid) {
            Ok(descendants) => {
                for identity in descendants {
                    if !captured.contains(&identity) {
                        captured.push(identity);
                    }
                }
            }
            Err(_error) if !std::path::Path::new(&format!("/proc/{supervisor_pid}")).exists() => {}
            Err(error) => inspection_error = Some(error),
        }
        let captured_for_record = captured.clone();
        let mut remaining = Vec::new();
        for identity in captured {
            match still_same_process(identity) {
                Ok(true) => remaining.push(identity),
                Ok(false) => {}
                Err(error) => inspection_error = Some(error),
            }
        }
        terminate_pids(&remaining, "-TERM");
        if !remaining.is_empty() {
            thread::sleep(Duration::from_millis(100));
            let mut survivors = Vec::new();
            for identity in remaining {
                match still_same_process(identity) {
                    Ok(true) => survivors.push(identity),
                    Ok(false) => {}
                    Err(error) => inspection_error = Some(error),
                }
            }
            remaining = survivors;
            terminate_pids(&remaining, "-KILL");
            thread::sleep(Duration::from_millis(100));
            let mut survivors = Vec::new();
            for identity in remaining {
                match still_same_process(identity) {
                    Ok(true) => survivors.push(identity),
                    Ok(false) => {}
                    Err(error) => inspection_error = Some(error),
                }
            }
            remaining = survivors;
        }
        let stdout = stdout_reader
            .join()
            .map_err(|_| "stdout reader panicked".to_string())
            .and_then(|capture| capture.map_err(|error| error.to_string()))
            .unwrap_or_else(|error| {
                inspection_error = Some(error);
                StreamCapture::default()
            });
        let stderr = stderr_reader
            .join()
            .map_err(|_| "stderr reader panicked".to_string())
            .and_then(|capture| capture.map_err(|error| error.to_string()))
            .unwrap_or_else(|error| {
                inspection_error = Some(error);
                StreamCapture::default()
            });
        let success_marker = stdout.success_marker || stderr.success_marker;
        let classified_marker = stdout.classified_failure || stderr.classified_failure;
        let mut outcome = classify(
            status.code(),
            if classified_marker { "PROBE-FAIL" } else { "" },
            inspection_error.is_some() || !remaining.is_empty(),
            success_marker,
        );
        let failure_seen = stdout.failure_seen || stderr.failure_seen;
        let mut evidence = Vec::new();
        if outcome != Outcome::Completed {
            if let Some(metadata) = stderr
                .last_metadata
                .as_ref()
                .or(stdout.last_metadata.as_ref())
            {
                evidence.push(metadata.clone());
            }
            if let Some(checkpoint) = stderr
                .last_checkpoint
                .as_ref()
                .or(stdout.last_checkpoint.as_ref())
            {
                evidence.push(checkpoint.clone());
            }
            if failure_seen {
                evidence.extend(stdout.evidence.iter().cloned());
                evidence.extend(stderr.evidence.iter().cloned());
            } else {
                evidence.push("S9-EVIDENCE failure_marker=missing".to_string());
                evidence.extend(stdout.fallback_tail.iter().cloned());
                evidence.extend(stderr.fallback_tail.iter().cloned());
            }
        }
        for line in &evidence {
            eprintln!("{line}");
        }
        let persisted_truncated = append_evidence(manifest, run, &attempt, &revision, evidence)
            .unwrap_or_else(|error| {
                inspection_error = Some(error);
                outcome = Outcome::CleanupFailure;
                true
            });
        let evidence_truncated =
            stdout.evidence_truncated || stderr.evidence_truncated || persisted_truncated;
        let invalid_records = stdout
            .invalid_records
            .saturating_add(stderr.invalid_records);
        let max_diagnostic_records = stdout.max_diagnostics().max(stderr.max_diagnostics());
        let max_liveness_records = stdout.max_liveness().max(stderr.max_liveness());
        let max_dropped_attempts = stdout.max_dropped_attempts.max(stderr.max_dropped_attempts);
        let max_dropped_liveness = stdout.max_dropped_liveness.max(stderr.max_dropped_liveness);
        let diagnostic_continue =
            max_dropped_attempts == 0 && max_dropped_liveness == 0 && invalid_records == 0;
        let ended_unix_ms = unix_millis();
        let elapsed_ms = started.elapsed().as_millis();
        if let Err(error) = record_attempt_elapsed(manifest, &attempt, elapsed_ms) {
            inspection_error = Some(error);
            outcome = Outcome::CleanupFailure;
        }
        let result_record = format!(
            "S9-SUPERVISOR-RESULT run={run} attempt={attempt} revision={revision} \
             started_unix_ms={started_unix_ms} ended_unix_ms={ended_unix_ms} \
             elapsed_ms={elapsed_ms} timeout_pid={supervisor_pid} \
             exit_code={} outcome={outcome:?} remaining_pids={:?} inspection_error={inspection_error:?} \
             success_marker={success_marker} captured={captured_for_record:?} \
             classifier_detail={:?} last_checkpoint={:?} failure_marker={} \
             evidence_truncated={evidence_truncated} invalid_records={invalid_records} \
             max_diagnostic_records={max_diagnostic_records} \
             max_liveness_records={max_liveness_records} \
             max_dropped_attempts={max_dropped_attempts} \
             max_dropped_liveness={max_dropped_liveness} \
             diagnostic_continue={diagnostic_continue}",
            status
                .code()
                .map_or_else(|| "signal".to_string(), |code| code.to_string()),
            remaining
                .iter()
                .map(|identity| identity.pid)
                .collect::<Vec<_>>(),
            stderr
                .failure_detail
                .as_deref()
                .or(stdout.failure_detail.as_deref())
                .unwrap_or("unavailable"),
            stderr
                .last_checkpoint
                .as_deref()
                .or(stdout.last_checkpoint.as_deref())
                .unwrap_or("unavailable"),
            if failure_seen { "present" } else { "missing" },
        );
        eprintln!("{result_record}");
        if let Err(error) = append_manifest(manifest, &result_record) {
            eprintln!(
                "S9-SUPERVISOR-GUARD reason=manifest-result-write run={run} attempt={attempt} error={error:?}"
            );
            outcome = Outcome::CleanupFailure;
        }
        match outcome {
            Outcome::Completed => completed += 1,
            Outcome::ClassifiedFailure => classified += 1,
            Outcome::OuterKilled => outer_killed += 1,
            Outcome::UnclassifiedFailure => unclassified += 1,
            Outcome::CleanupFailure => cleanup_failed += 1,
        }
        if outcome == Outcome::CleanupFailure {
            break;
        }
    }

    let summary = format!(
        "S9-SUPERVISOR-SUMMARY revision={revision} start={start} requested={runs} \
         completed={completed} interrupted={interrupted} \
         started_unix_ms={invocation_started_unix_ms} ended_unix_ms={} elapsed_ms={} \
         classified_failures={classified} outer_killed={outer_killed} \
         unclassified_failures={unclassified} cleanup_failures={cleanup_failed}",
        unix_millis(),
        invocation_started.elapsed().as_millis(),
    );
    eprintln!("{summary}");
    if let Err(error) = append_manifest(manifest, &summary) {
        eprintln!("S9-SUPERVISOR-GUARD reason=manifest-summary-write error={error:?}");
        drop(reservation_guard);
        std::process::exit(1);
    }
    let failed = classified + outer_killed + unclassified + cleanup_failed > 0;
    drop(reservation_guard);
    if failed {
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_supervisor_outcomes() {
        assert_eq!(classify(Some(0), "", false, true), Outcome::Completed);
        assert_eq!(
            classify(Some(0), "", false, false),
            Outcome::UnclassifiedFailure
        );
        assert_eq!(
            classify(
                Some(101),
                "PROBE-FAIL classifier=s9-body-drain-timeout",
                false,
                false,
            ),
            Outcome::ClassifiedFailure
        );
        assert_eq!(classify(Some(124), "", false, false), Outcome::OuterKilled);
        assert_eq!(classify(Some(137), "", false, false), Outcome::OuterKilled);
        assert_eq!(
            classify(
                Some(101),
                "16384-byte exchange 2 stalled; last completed exchange was 1",
                false,
                false,
            ),
            Outcome::ClassifiedFailure
        );
        assert_eq!(
            classify(Some(2), "plain failure", false, false),
            Outcome::UnclassifiedFailure
        );
        assert_eq!(classify(Some(0), "", true, true), Outcome::CleanupFailure);
    }

    #[test]
    fn start_run_range_does_not_double_count_prior_runs() {
        assert_eq!(
            run_range(21, 3).unwrap().collect::<Vec<_>>(),
            vec![21, 22, 23]
        );
        assert!(run_range(0, 3).is_err());
        assert!(run_range(1, 0).is_err());
        assert!(run_range(usize::MAX, 1).is_err());
    }

    #[test]
    fn extracts_the_last_durable_checkpoint() {
        let output = "PROBE-CHECKPOINT exchange=1 phase=response-head received_bytes=0\n\
                      noise\n\
                      PROBE-CHECKPOINT exchange=1 phase=body-drain received_bytes=65536\n";
        assert_eq!(
            last_checkpoint(output),
            "PROBE-CHECKPOINT exchange=1 phase=body-drain received_bytes=65536"
        );
        assert_eq!(
            last_checkpoint(
                "S9-FIXTURE-CHECKPOINT size=16384 exchange=4 phase=body-drain received_bytes=8\n"
            ),
            "S9-FIXTURE-CHECKPOINT size=16384 exchange=4 phase=body-drain received_bytes=8"
        );
    }

    #[test]
    fn terminates_every_member_of_a_finite_process_group() {
        let mut child = Command::new("setsid")
            .args(["sh", "-c", "sleep 30 & wait"])
            .spawn()
            .expect("spawn finite process group");
        let group = child.id();
        thread::sleep(Duration::from_millis(100));
        let members = process_group_pids(group).expect("inspect finite process group");
        assert!(
            members.len() >= 2,
            "the test must observe both the shell and its child"
        );
        terminate_pids(&members, "-TERM");
        let _ = child.wait();
        thread::sleep(Duration::from_millis(100));
        let survivors = process_group_pids(group).expect("reinspect finite process group");
        terminate_pids(&survivors, "-KILL");
        thread::sleep(Duration::from_millis(100));
        assert!(
            process_group_pids(group)
                .expect("final process-group inspection")
                .is_empty(),
            "the exact process group retained a child after cleanup"
        );
    }

    #[test]
    fn captures_a_descendant_that_leaves_the_process_group() {
        let mut child = Command::new("setsid")
            .args(["sh", "-c", "setsid sleep 30 & wait"])
            .spawn()
            .expect("spawn process with escaped descendant");
        let root = child.id();
        thread::sleep(Duration::from_millis(100));
        let descendants = descendant_identities(root).expect("capture descendant tree");
        assert!(
            !descendants.is_empty(),
            "the escaped child must still be captured by ancestry"
        );
        terminate_pids(&descendants, "-TERM");
        let root_identity = process_identity(root).expect("root identity");
        terminate_pids(&[root_identity], "-TERM");
        let _ = child.wait();
        thread::sleep(Duration::from_millis(100));
        let survivors = descendants
            .into_iter()
            .filter(|identity| still_same_process(*identity).unwrap_or(true))
            .collect::<Vec<_>>();
        terminate_pids(&survivors, "-KILL");
        thread::sleep(Duration::from_millis(100));
        assert!(
            survivors
                .into_iter()
                .all(|identity| !still_same_process(identity).unwrap_or(true)),
            "an escaped captured descendant survived TERM"
        );
    }

    #[test]
    fn manifest_run_detection_matches_whole_fields() {
        let manifest = "S9-SUPERVISOR-RESULT run=2 outcome=Completed\n";
        assert!(manifest_has_completed_run(manifest, 2));
        assert!(!manifest_has_completed_run(manifest, 1));
        assert!(!manifest_has_completed_run(manifest, 20));
        assert!(!manifest_has_completed_run("S9-SUPERVISOR-START run=2", 2));
    }

    #[test]
    fn probe_completion_marker_matches_the_exact_count() {
        assert!(probe_completed_exactly("PROBE-DONE completed=1\n", "1"));
        assert!(!probe_completed_exactly("PROBE-DONE completed=10\n", "1"));
    }

    #[test]
    fn rejects_disabled_or_undersized_outer_bounds() {
        assert!(validate_outer(false, Some("reliability"), 0).is_err());
        assert!(validate_outer(false, Some("reliability"), 179).is_err());
        assert!(validate_outer(false, Some("reliability"), 180).is_ok());
        assert!(validate_outer(false, Some("diagnostic"), 684).is_err());
        assert!(validate_outer(true, None, 9).is_err());
    }

    #[test]
    fn bounded_line_reader_caps_oversized_and_unterminated_records() {
        let input = vec![b'x'; MAX_INPUT_RECORD_BYTES + 4096];
        let mut reader = BufReader::new(io::Cursor::new(input));
        let (line, invalid) = read_bounded_line(&mut reader)
            .expect("read oversized record")
            .expect("record exists");
        assert_eq!(line.len(), MAX_INPUT_RECORD_BYTES);
        assert!(invalid);
        assert!(read_bounded_line(&mut reader).expect("reach eof").is_none());
    }

    #[test]
    fn successful_diagnostics_are_summarized_without_retaining_full_lines() {
        let mut capture = StreamCapture::default();
        capture.observe(
            "PROBE-DIAGNOSTIC exchange=1 attempt=0 sequence=1".to_string(),
            false,
            Some("1"),
        );
        capture.observe(
            "PROBE-LIVENESS exchange=1 sequence=2".to_string(),
            false,
            Some("1"),
        );
        capture.observe(
            "PROBE-SNAPSHOT exchange=1 role=client dropped_attempt_records=0 \
             dropped_liveness_records=0"
                .to_string(),
            false,
            Some("1"),
        );
        capture.observe("PROBE-DONE completed=1".to_string(), false, Some("1"));
        assert!(capture.success_marker);
        assert_eq!(capture.max_diagnostics(), 1);
        assert_eq!(capture.max_liveness(), 1);
        assert!(capture.evidence.is_empty());
        assert_eq!(capture.max_dropped_attempts, 0);
        assert_eq!(capture.max_dropped_liveness, 0);
    }

    #[test]
    fn many_successful_diagnostics_keep_only_a_bounded_fallback_tail() {
        let mut capture = StreamCapture::default();
        for sequence in 0..100_000 {
            capture.observe(
                format!("PROBE-DIAGNOSTIC exchange=1 sequence={sequence} payload=xxxxxxxxxxxxxxxx"),
                false,
                None,
            );
        }
        assert!(capture.evidence.is_empty());
        assert!(capture.fallback_tail_bytes <= MAX_FALLBACK_TAIL_BYTES);
        assert_eq!(capture.max_diagnostics(), 100_000);
    }

    #[test]
    fn no_marker_failures_retain_a_bounded_fallback_tail() {
        let mut capture = StreamCapture::default();
        capture.observe("ordinary child output".to_string(), false, None);
        assert!(!capture.failure_seen);
        assert!(capture.evidence.is_empty());
        assert_eq!(
            capture.fallback_tail.back().map(String::as_str),
            Some("ordinary child output")
        );
    }

    #[test]
    fn live_filter_excludes_bulk_diagnostics() {
        assert!(is_live_line("PROBE-READY arm=ngnet-quic-h3"));
        assert!(is_live_line("PROBE-CHECKPOINT exchange=1"));
        assert!(is_live_line("PROBE-FAIL exchange=1"));
        assert!(!is_live_line("PROBE-DIAGNOSTIC exchange=1"));
        assert!(!is_live_line("PROBE-LIVENESS exchange=1"));
        assert!(!is_live_line("PROBE-SNAPSHOT exchange=1"));
    }

    #[test]
    fn failure_capture_starts_at_the_failure_occurrence() {
        let mut capture = StreamCapture::default();
        capture.observe(
            "PROBE-DIAGNOSTIC exchange=1 sequence=1".to_string(),
            false,
            None,
        );
        capture.observe(
            "PROBE-FAIL exchange=2 classifier=s9-body-drain-timeout".to_string(),
            false,
            None,
        );
        capture.observe(
            "PROBE-DIAGNOSTIC exchange=2 sequence=2".to_string(),
            false,
            None,
        );
        assert_eq!(capture.evidence.len(), 2);
        assert!(
            capture
                .evidence
                .front()
                .expect("failure retained")
                .starts_with("PROBE-FAIL")
        );
        assert!(
            capture
                .evidence
                .back()
                .expect("diagnostic retained")
                .contains("exchange=2")
        );
    }

    #[test]
    fn evidence_escaping_is_single_line_and_reversible_in_shape() {
        assert_eq!(escape_manifest("a\\b\nc\rd\te"), "a\\\\b\\nc\\rd\\te");
    }

    #[test]
    fn manifest_write_errors_are_returned() {
        assert!(append_manifest(Some("."), "record").is_err());
    }

    #[test]
    fn binary_hash_is_stable_and_full_length() {
        let executable = std::env::current_exe().expect("current test executable");
        let first = sha256(&executable).expect("first hash");
        let second = sha256(&executable).expect("second hash");
        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
    }

    #[test]
    fn fixture_binary_is_selected_from_cargo_json() {
        let output = "{\"reason\":\"compiler-artifact\",\"executable\":\"/repo/target/release/deps/ngtcp2_fixture-deadbeef\"}\n\
                      {\"reason\":\"build-finished\",\"success\":true}\n";
        assert_eq!(
            fixture_executable_from_json(output),
            Some(PathBuf::from(
                "/repo/target/release/deps/ngtcp2_fixture-deadbeef"
            ))
        );
    }

    #[test]
    fn concurrent_reservations_are_serialized_and_released() {
        use std::sync::{Arc, Barrier};

        let root = std::env::current_dir()
            .expect("current directory")
            .join("target")
            .join(format!(
                "s9-supervisor-reservation-test-{}-{}",
                std::process::id(),
                unix_millis()
            ));
        let revision = root.join("evidence").join("revision");
        std::fs::create_dir_all(&revision).expect("create reservation test root");
        let barrier = Arc::new(Barrier::new(3));
        let mut threads = Vec::new();
        for name in ["a.manifest", "b.manifest"] {
            let path = revision.join(name);
            let barrier = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                let path = path.to_string_lossy().into_owned();
                let guard = reserve_storage(Some(&path), 1)
                    .expect("reserve storage")
                    .expect("manifest creates reservation");
                barrier.wait();
                barrier.wait();
                drop(guard);
            }));
        }
        barrier.wait();
        let ledger = std::fs::read_to_string(root.join("evidence/.s9-evidence-reservations"))
            .expect("read active reservations");
        assert_eq!(ledger.matches("S9-EVIDENCE-RESERVE").count(), 2);
        assert_eq!(ledger.matches("S9-EVIDENCE-RELEASE").count(), 0);
        barrier.wait();
        for thread in threads {
            thread.join().expect("join reservation thread");
        }
        let ledger = std::fs::read_to_string(root.join("evidence/.s9-evidence-reservations"))
            .expect("read released reservations");
        assert_eq!(ledger.matches("S9-EVIDENCE-RELEASE").count(), 2);
        std::fs::remove_dir_all(root).expect("remove reservation test root");
    }

    #[test]
    fn interrupted_and_evidence_records_do_not_complete_a_run() {
        let manifest = "S9-SUPERVISOR-START run=2 attempt=a\n\
                        S9-SUPERVISOR-EVIDENCE run=2 attempt=a line=x\n\
                        S9-SUPERVISOR-INTERRUPTED run=2 attempt=a cleanup=gone\n";
        assert!(!manifest_has_completed_run(manifest, 2));
        assert!(manifest_has_completed_run(
            "S9-SUPERVISOR-RESULT run=2 attempt=b outcome=Completed\n",
            2
        ));
    }

    #[test]
    fn manifest_scan_reconciles_attempt_identity_without_completing_the_run() {
        let manifest = "S9-SUPERVISOR-METADATA revision=r supervisor_sha256=s workload_sha256=w\n\
                        S9-SUPERVISOR-START run=2 attempt=a timeout_pid=9 start_time=10 \
                        started_unix_ms=11\n\
                        S9-SUPERVISOR-INTERRUPTED run=2 attempt=a cleanup=gone elapsed_ms=1\n\
                        S9-SUPERVISOR-START run=2 attempt=b timeout_pid=12 start_time=13 \
                        started_unix_ms=14\n";
        let state =
            scan_manifest_reader(BufReader::new(io::Cursor::new(manifest))).expect("scan manifest");
        assert_eq!(state.revision.as_deref(), Some("r"));
        assert!(!state.completed_runs.contains(&2));
        assert_eq!(state.dangling.len(), 1);
        assert!(state.dangling.contains_key(&(2, "b".to_string())));
    }

    #[test]
    fn bounded_queue_preserves_failure_marker_while_rolling_the_tail() {
        let mut lines = VecDeque::new();
        let mut bytes = 0;
        assert!(!push_bounded(
            &mut lines,
            &mut bytes,
            "PROBE-FAIL classifier=x".to_string(),
            48,
            Some(&["PROBE-FAIL"])
        ));
        assert!(push_bounded(
            &mut lines,
            &mut bytes,
            "PROBE-DIAGNOSTIC sequence=1 payload=xxxxxxxx".to_string(),
            48,
            Some(&["PROBE-FAIL"])
        ));
        assert!(
            lines
                .front()
                .expect("failure marker retained")
                .starts_with("PROBE-FAIL")
        );
    }
}
