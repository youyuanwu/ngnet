//! Serial process supervisor for native HTTP/3 S9 reliability runs.
//!
//! Usage:
//! `s9_supervisor <probe> <arm> <mode> <body-bytes> <exchanges> <runs> <outer-seconds> [start-run]`
//!
//! The optional start number makes a resumed invocation explicit without storing hidden state.

use std::io::Write;
use std::path::Path;
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
    } else if stderr.contains("PROBE-FAIL") {
        Outcome::ClassifiedFailure
    } else {
        Outcome::UnclassifiedFailure
    }
}

fn child_pids(parent: u32) -> Vec<u32> {
    let path = format!("/proc/{parent}/task/{parent}/children");
    std::fs::read_to_string(path)
        .ok()
        .into_iter()
        .flat_map(|children| {
            children
                .split_whitespace()
                .filter_map(|pid| pid.parse::<u32>().ok())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn process_exists(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

fn terminate_pid(pid: u32) {
    let _ = Command::new("kill").arg(pid.to_string()).status();
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let probe = args.get(1).expect("probe executable");
    let arm = args.get(2).expect("arm");
    let mode = args.get(3).expect("mode");
    let body = args.get(4).expect("body bytes");
    let exchanges = args.get(5).expect("exchanges");
    let runs = args
        .get(6)
        .expect("runs")
        .parse::<usize>()
        .expect("runs is a number");
    let outer = args
        .get(7)
        .expect("outer seconds")
        .parse::<u64>()
        .expect("outer seconds is a number");
    let start = args.get(8).map_or(1, |value| {
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
        eprintln!(
            "S9-SUPERVISOR-START run={run} arm={arm} mode={mode} body={body} \
             exchanges={exchanges} outer_seconds={outer}"
        );
        std::io::stderr()
            .flush()
            .expect("flush supervisor start record");

        let child = Command::new("timeout")
            .args([
                "--signal=TERM",
                "--kill-after=5s",
                &format!("{outer}s"),
                probe,
                arm,
                "body",
                body,
                exchanges,
                mode,
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn supervised probe");
        let supervisor_pid = child.id();
        thread::sleep(Duration::from_millis(100));
        let observed_children = child_pids(supervisor_pid);
        let output = child.wait_with_output().expect("wait for supervised probe");

        print!("{}", String::from_utf8_lossy(&output.stdout));
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
        std::io::stdout().flush().expect("flush probe stdout");
        std::io::stderr().flush().expect("flush probe stderr");

        let remaining = observed_children
            .into_iter()
            .filter(|pid| process_exists(*pid))
            .collect::<Vec<_>>();
        for pid in &remaining {
            terminate_pid(*pid);
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
             exit_code={} outcome={outcome:?} remaining_pids={remaining:?}",
            output
                .status
                .code()
                .map_or_else(|| "signal".to_string(), |code| code.to_string())
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
}
