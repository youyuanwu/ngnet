//! Serial process supervisor for native HTTP/3 S9 reliability runs.
//!
//! Usage:
//! `s9_supervisor <probe> <arm> <mode> <body-bytes> <exchanges> <runs> <outer-seconds> [start-run] [manifest]`
//! `s9_supervisor fixture <test-name> <runs> <outer-seconds> [start-run] [manifest]`
//!
//! The optional start number makes a resumed invocation explicit without storing hidden state.

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Completed,
    ClassifiedFailure,
    OuterKilled,
    UnclassifiedFailure,
    CleanupFailure,
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
        for child in child_pids(parent)? {
            pending.push(child);
            found.insert(child, process_identity(child)?);
        }
    }
    Ok(found.into_values().collect())
}

fn still_same_process(identity: ProcessIdentity) -> bool {
    process_identity(identity.pid).is_ok_and(|current| current == identity)
}

fn terminate_pids(pids: &[ProcessIdentity], signal: &str) {
    for identity in pids {
        let _ = Command::new("kill")
            .args([signal, "--", &identity.pid.to_string()])
            .status();
    }
}

fn last_checkpoint(stderr: &str) -> &str {
    stderr
        .lines()
        .rev()
        .find(|line| {
            line.starts_with("PROBE-CHECKPOINT") || line.starts_with("S9-FIXTURE-CHECKPOINT")
        })
        .unwrap_or("unavailable")
}

fn manifest_has_run(contents: &str, run: usize) -> bool {
    contents.lines().any(|line| {
        line.split_whitespace()
            .any(|field| field == format!("run={run}"))
    })
}

fn append_manifest(path: Option<&str>, record: &str) {
    let Some(path) = path else {
        return;
    };
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap_or_else(|error| panic!("opening S9 manifest {path}: {error}"));
    writeln!(file, "{record}")
        .unwrap_or_else(|error| panic!("writing S9 manifest {path}: {error}"));
    file.flush()
        .unwrap_or_else(|error| panic!("flushing S9 manifest {path}: {error}"));
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

fn main() {
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
    assert!(runs > 0, "runs must be non-zero");
    assert!(start > 0, "start-run must be non-zero");
    validate_outer(fixture, (!fixture).then(|| args[3].as_str()), outer)
        .unwrap_or_else(|error| panic!("{error}"));
    if let Some(path) = manifest
        && let Ok(contents) = std::fs::read_to_string(path)
    {
        for run in start..start + runs {
            assert!(
                !manifest_has_run(&contents, run),
                "manifest {path} already contains run {run}"
            );
        }
    }

    let mut completed = 0usize;
    let mut classified = 0usize;
    let mut outer_killed = 0usize;
    let mut unclassified = 0usize;
    let mut cleanup_failed = 0usize;

    for run in start..start + runs {
        let start_record = if fixture {
            format!(
                "S9-SUPERVISOR-START run={run} fixture={} outer_seconds={outer}",
                args[2]
            )
        } else {
            format!(
                "S9-SUPERVISOR-START run={run} arm={} mode={} body={} \
                 exchanges={} outer_seconds={outer}",
                args[2], args[3], args[4], args[5]
            )
        };
        eprintln!("{start_record}");
        append_manifest(manifest, &start_record);
        std::io::stderr()
            .flush()
            .expect("flush supervisor start record");

        let mut command = Command::new("setsid");
        command
            .arg("timeout")
            .args(["--signal=TERM", "--kill-after=5s", &format!("{outer}s")]);
        if fixture {
            command.args([
                "cargo",
                "test",
                "-p",
                "ngnet-bench",
                "--test",
                "ngtcp2_fixture",
                "--release",
                &args[2],
                "--",
                "--ignored",
                "--exact",
                "--nocapture",
            ]);
        } else {
            command.args([&args[1], &args[2], "body", &args[4], &args[5], &args[3]]);
        }
        let mut child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn supervised S9 cell");
        let supervisor_pid = child.id();
        thread::sleep(Duration::from_millis(100));
        let (captured, mut inspection_error) = match child.try_wait() {
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
        let output = child.wait_with_output().expect("wait for supervised probe");

        print!("{}", String::from_utf8_lossy(&output.stdout));
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
        std::io::stdout().flush().expect("flush probe stdout");
        std::io::stderr().flush().expect("flush probe stderr");

        let mut remaining = captured
            .into_iter()
            .filter(|identity| still_same_process(*identity))
            .collect::<Vec<_>>();
        match process_group_pids(supervisor_pid) {
            Ok(group) => {
                for identity in group {
                    if !remaining.contains(&identity) {
                        remaining.push(identity);
                    }
                }
            }
            Err(error) => {
                eprintln!("S9-SUPERVISOR-INSPECTION-FAIL run={run} error={error:?}");
                inspection_error = Some(error);
            }
        }
        terminate_pids(&remaining, "-TERM");
        if !remaining.is_empty() {
            thread::sleep(Duration::from_millis(100));
            remaining.retain(|identity| still_same_process(*identity));
            terminate_pids(&remaining, "-KILL");
            thread::sleep(Duration::from_millis(100));
            remaining.retain(|identity| still_same_process(*identity));
        }
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let success_marker = if fixture {
            combined.contains("test result: ok. 1 passed")
        } else {
            combined.contains(&format!("PROBE-DONE completed={}", args[5]))
        };
        let outcome = classify(
            output.status.code(),
            &String::from_utf8_lossy(&output.stderr),
            inspection_error.is_some() || !remaining.is_empty(),
            success_marker,
        );
        match outcome {
            Outcome::Completed => completed += 1,
            Outcome::ClassifiedFailure => classified += 1,
            Outcome::OuterKilled => outer_killed += 1,
            Outcome::UnclassifiedFailure => unclassified += 1,
            Outcome::CleanupFailure => cleanup_failed += 1,
        }
        let result_record = format!(
            "S9-SUPERVISOR-RESULT run={run} timeout_pid={supervisor_pid} \
             exit_code={} outcome={outcome:?} remaining_pids={:?} inspection_error={inspection_error:?} \
             success_marker={success_marker} last_checkpoint={:?}",
            output
                .status
                .code()
                .map_or_else(|| "signal".to_string(), |code| code.to_string()),
            remaining
                .iter()
                .map(|identity| identity.pid)
                .collect::<Vec<_>>(),
            last_checkpoint(&String::from_utf8_lossy(&output.stderr)),
        );
        eprintln!("{result_record}");
        append_manifest(manifest, &result_record);
        if outcome == Outcome::CleanupFailure {
            break;
        }
    }

    let summary = format!(
        "S9-SUPERVISOR-SUMMARY start={start} requested={runs} completed={completed} \
         classified_failures={classified} outer_killed={outer_killed} \
         unclassified_failures={unclassified} cleanup_failures={cleanup_failed}"
    );
    eprintln!("{summary}");
    append_manifest(manifest, &summary);
    if classified + outer_killed + unclassified + cleanup_failed > 0 {
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
        assert_eq!(
            classify(Some(2), "plain failure", false, false),
            Outcome::UnclassifiedFailure
        );
        assert_eq!(classify(Some(0), "", true, true), Outcome::CleanupFailure);
    }

    #[test]
    fn start_run_range_does_not_double_count_prior_runs() {
        let start = 21usize;
        let runs = 3usize;
        assert_eq!((start..start + runs).collect::<Vec<_>>(), vec![21, 22, 23]);
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
            .filter(|identity| still_same_process(*identity))
            .collect::<Vec<_>>();
        terminate_pids(&survivors, "-KILL");
        thread::sleep(Duration::from_millis(100));
        assert!(
            survivors
                .into_iter()
                .all(|identity| !still_same_process(identity)),
            "an escaped captured descendant survived TERM"
        );
    }

    #[test]
    fn manifest_run_detection_matches_whole_fields() {
        let manifest = "S9-SUPERVISOR-RESULT run=2 outcome=Completed\n";
        assert!(manifest_has_run(manifest, 2));
        assert!(!manifest_has_run(manifest, 1));
        assert!(!manifest_has_run(manifest, 20));
    }

    #[test]
    fn rejects_disabled_or_undersized_outer_bounds() {
        assert!(validate_outer(false, Some("reliability"), 0).is_err());
        assert!(validate_outer(false, Some("reliability"), 179).is_err());
        assert!(validate_outer(false, Some("reliability"), 180).is_ok());
        assert!(validate_outer(false, Some("diagnostic"), 684).is_err());
        assert!(validate_outer(true, None, 9).is_err());
    }
}
