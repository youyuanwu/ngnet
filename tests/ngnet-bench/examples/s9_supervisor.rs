//! Serial process supervisor for native HTTP/3 S9 reliability runs.
//!
//! Usage:
//! `s9_supervisor <probe> <arm> <mode> <body-bytes> <exchanges> <runs> <outer-seconds> [start-run]`
//! `s9_supervisor fixture <test-name> <runs> <outer-seconds> [start-run]`
//!
//! The optional start number makes a resumed invocation explicit without storing hidden state.

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

fn classify(code: Option<i32>, stderr: &str, cleanup_failed: bool) -> Outcome {
    if cleanup_failed {
        Outcome::CleanupFailure
    } else if code == Some(0) {
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

fn process_group_pids(group: u32) -> Vec<u32> {
    let Ok(output) = Command::new("ps").args(["-eo", "pid=,pgid="]).output() else {
        return Vec::new();
    };
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let pid = fields.next()?.parse::<u32>().ok()?;
            let pgid = fields.next()?.parse::<u32>().ok()?;
            (pgid == group && pid != std::process::id()).then_some(pid)
        })
        .collect()
}

fn terminate_pids(pids: &[u32], signal: &str) {
    for pid in pids {
        let _ = Command::new("kill")
            .args([signal, "--", &pid.to_string()])
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

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let fixture = args.get(1).is_some_and(|value| value == "fixture");
    let (runs_index, outer_index, start_index) = if fixture { (3, 4, 5) } else { (6, 7, 8) };
    let runs = args[runs_index].parse::<usize>().expect("runs is a number");
    let outer = args[outer_index]
        .parse::<u64>()
        .expect("outer seconds is a number");
    let start = args.get(start_index).map_or(1, |value| {
        value.parse::<usize>().expect("start is a number")
    });
    assert!(runs > 0, "runs must be non-zero");
    assert!(start > 0, "start-run must be non-zero");

    let mut completed = 0usize;
    let mut classified = 0usize;
    let mut outer_killed = 0usize;
    let mut unclassified = 0usize;
    let mut cleanup_failed = 0usize;

    for run in start..start + runs {
        if fixture {
            eprintln!(
                "S9-SUPERVISOR-START run={run} fixture={} outer_seconds={outer}",
                args[2]
            );
        } else {
            eprintln!(
                "S9-SUPERVISOR-START run={run} arm={} mode={} body={} \
                 exchanges={} outer_seconds={outer}",
                args[2], args[3], args[4], args[5]
            );
        }
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
        let child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn supervised S9 cell");
        let supervisor_pid = child.id();
        thread::sleep(Duration::from_millis(100));
        let output = child.wait_with_output().expect("wait for supervised probe");

        print!("{}", String::from_utf8_lossy(&output.stdout));
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
        std::io::stdout().flush().expect("flush probe stdout");
        std::io::stderr().flush().expect("flush probe stderr");

        let mut remaining = process_group_pids(supervisor_pid);
        terminate_pids(&remaining, "-TERM");
        if !remaining.is_empty() {
            thread::sleep(Duration::from_millis(100));
            remaining = process_group_pids(supervisor_pid);
            terminate_pids(&remaining, "-KILL");
            thread::sleep(Duration::from_millis(100));
            remaining = process_group_pids(supervisor_pid);
        }
        let outcome = classify(
            output.status.code(),
            &String::from_utf8_lossy(&output.stderr),
            !remaining.is_empty(),
        );
        match outcome {
            Outcome::Completed => completed += 1,
            Outcome::ClassifiedFailure => classified += 1,
            Outcome::OuterKilled => outer_killed += 1,
            Outcome::UnclassifiedFailure => unclassified += 1,
            Outcome::CleanupFailure => cleanup_failed += 1,
        }
        eprintln!(
            "S9-SUPERVISOR-RESULT run={run} timeout_pid={supervisor_pid} \
             exit_code={} outcome={outcome:?} remaining_pids={remaining:?} last_checkpoint={:?}",
            output
                .status
                .code()
                .map_or_else(|| "signal".to_string(), |code| code.to_string()),
            last_checkpoint(&String::from_utf8_lossy(&output.stderr)),
        );
        if outcome == Outcome::CleanupFailure {
            break;
        }
    }

    eprintln!(
        "S9-SUPERVISOR-SUMMARY start={start} requested={runs} completed={completed} \
         classified_failures={classified} outer_killed={outer_killed} \
         unclassified_failures={unclassified} cleanup_failures={cleanup_failed}"
    );
    if classified + outer_killed + unclassified + cleanup_failed > 0 {
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_supervisor_outcomes() {
        assert_eq!(classify(Some(0), "", false), Outcome::Completed);
        assert_eq!(
            classify(
                Some(101),
                "PROBE-FAIL classifier=s9-body-drain-timeout",
                false
            ),
            Outcome::ClassifiedFailure
        );
        assert_eq!(classify(Some(124), "", false), Outcome::OuterKilled);
        assert_eq!(
            classify(Some(2), "plain failure", false),
            Outcome::UnclassifiedFailure
        );
        assert_eq!(classify(Some(0), "", true), Outcome::CleanupFailure);
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
        let members = process_group_pids(group);
        assert!(
            members.len() >= 2,
            "the test must observe both the shell and its child"
        );
        terminate_pids(&members, "-TERM");
        let _ = child.wait();
        thread::sleep(Duration::from_millis(100));
        let survivors = process_group_pids(group);
        terminate_pids(&survivors, "-KILL");
        thread::sleep(Duration::from_millis(100));
        assert!(
            process_group_pids(group).is_empty(),
            "the exact process group retained a child after cleanup"
        );
    }
}
