//! Checks on the workspace itself, rather than on any crate in it.
//!
//! Everything here asserts one of two things, and the distinction matters enough that each
//! test says which one it makes:
//!
//! - **What the resolved dependency graph contains.** `cargo tree` answers this. It is what
//!   a downstream user's build actually pulls in, after cargo has resolved versions, unified
//!   features across the workspace and applied defaults.
//! - **What a linked binary pulls in.** `readelf` answers this, for the cases cargo cannot:
//!   a C library that arrives through link flags a build script emits is not a cargo
//!   dependency and appears in no graph.
//!
//! Neither is the same claim as the one made by the `invariants.rs` suite in each crate.
//! Those assert what a crate *declares* in its own `Cargo.toml` -- that `ngnet-h3` names one
//! dependency, say. This crate asserts what the resolution of that declaration actually
//! produces. The two agree today, which is exactly why it is worth keeping them apart: a
//! transitive dependency, a feature enabled by an unrelated workspace member, or an axum
//! feature that quietly wants `hyper-util` can move the second without touching the first.
//! In English the two sound like the same sentence. They are not, and a reader who conflates
//! them will believe the manifest test is covering something it never looks at.
//!
//! These checks lived in `.github/workflows/ci.yml` as inline shell until they were moved
//! here. The reasoning that was in those comments moved with them, into the doc comments of
//! the individual tests, and the guidance the `::error::` annotations printed moved into the
//! assertion messages -- the hint naming the command that finds the culprit is the difference
//! between a five-minute debugging session and an hour of one.

use std::path::PathBuf;
use std::process::Command;

/// Runs `cargo tree` with the given arguments and returns its standard output.
///
/// Two details are load-bearing and neither is obvious:
///
/// `env!("CARGO")` rather than a bare `"cargo"` runs the same cargo that is running this
/// test, which is the one `rust-toolchain.toml` pins. A bare `"cargo"` would be whatever
/// is first on `PATH`, which on a machine with several toolchains is a coin toss, and the
/// answer to "what does the graph resolve to" can depend on which one you ask.
///
/// `--locked` is appended to every invocation so that a *test* can never rewrite
/// `Cargo.lock`. Without it a resolver change would be silently written to disk as a side
/// effect of running the test suite, which is a mutation no one asked for and no one would
/// notice until it appeared in a diff.
///
/// The working directory is this crate's manifest directory. `-p` is resolved against the
/// workspace either way, but pinning it means the result does not depend on where the test
/// binary happened to be invoked from.
///
/// A cargo failure panics with the full stderr rather than returning an empty string,
/// because an empty tree would make every caller's "no forbidden crate appears" assertion
/// pass vacuously -- the check would go green precisely when it could no longer be made.
pub fn cargo_tree(args: &[&str]) -> String {
    let output = Command::new(env!("CARGO"))
        .arg("tree")
        .args(args)
        .arg("--locked")
        // Force plain output rather than inheriting the ambient setting. CI sets
        // `CARGO_TERM_COLOR: always`, which makes `cargo tree` wrap its box-drawing prefix in
        // ANSI escapes -- `\x1b[2m\u{2514}\u{2500}\u{2500}\x1b[0m` instead of `\u{2514}\u{2500}\u{2500}`. Since `2` and `m` are
        // alphanumeric, the escape survives the prefix trim in `dependency_name` and gets
        // read as the package name. This is not hypothetical: it went green locally and red
        // on CI, which is the whole reason to pin the format instead of parsing around it.
        .arg("--color=never")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap_or_else(|e| panic!("could not run `cargo tree {}`: {e}", args.join(" ")));

    assert!(
        output.status.success(),
        "`cargo tree {}` failed with {}.\n\
         An empty or failed tree would make the caller's assertion pass for the wrong \
         reason, so this is a hard error rather than an empty result.\n\
         stderr:\n{}",
        args.join(" "),
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );

    String::from_utf8(output.stdout).expect("`cargo tree` emitted invalid UTF-8")
}

/// Builds the test executables for a package and returns their paths, without running them.
///
/// This is how a claim about *linkage* is asked, since linkage is only observable once
/// something has actually been linked. `--no-run` builds and stops.
///
/// Running `cargo` from inside a running `cargo test` sounds like it should deadlock on the
/// target directory lock, and it was measured rather than assumed before this was written:
/// the outer `cargo test` releases the build lock before it runs any test executable, so a
/// nested build acquires it without contention. Three of these running concurrently on
/// separate test threads complete in about a second on a warm target directory. Pointing the
/// nested build at a separate `CARGO_TARGET_DIR` would avoid a problem that does not occur,
/// at the price of a second full ngtcp2 build from source.
///
/// An empty result panics. The original shell check asked "did the build layout change?"
/// and that question is the whole reason the guard exists: a caller that iterates over no
/// binaries at all and concludes "none of them links OpenSSL" is not checking anything.
pub fn test_executables(args: &[&str]) -> Vec<PathBuf> {
    let output = Command::new(env!("CARGO"))
        .arg("test")
        .args(args)
        .args(["--no-run", "--locked", "--message-format=json"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap_or_else(|e| panic!("could not run `cargo test {}`: {e}", args.join(" ")));

    assert!(
        output.status.success(),
        "`cargo test {} --no-run` failed with {}.\nstderr:\n{}",
        args.join(" "),
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );

    let stdout = String::from_utf8(output.stdout).expect("cargo emitted invalid UTF-8");
    let binaries: Vec<PathBuf> = stdout.lines().filter_map(executable_path).collect();

    assert!(
        !binaries.is_empty(),
        "found no test binaries to inspect for `cargo test {}`; did the build layout change?\n\
         Concluding anything from an empty list would be checking nothing at all.",
        args.join(" "),
    );

    binaries
}

/// Pulls the `"executable"` field out of one line of cargo's JSON message stream.
///
/// Hand-written rather than delegating to a JSON crate, because this crate deliberately has
/// no dependencies and this is the only JSON it ever reads. Compiler artifact messages carry
/// `"executable":null` for anything that is not a runnable binary, and those are skipped by
/// requiring the opening quote of a string value.
fn executable_path(line: &str) -> Option<PathBuf> {
    const KEY: &str = "\"executable\":\"";
    let start = line.find(KEY)? + KEY.len();
    let rest = &line[start..];
    let end = rest.find('"')?;
    Some(PathBuf::from(&rest[..end]))
}

/// Whether `haystack` contains `needle` at a word boundary, compared case-insensitively.
///
/// This exists to reproduce `grep -qiE '\bhyper'` exactly, and the exactness is the point.
/// The checks that use it were shell scripts before they were tests, and a relocation that
/// quietly changes what a check catches is not a relocation.
///
/// The tempting simplification -- split the line into whitespace-delimited tokens and compare
/// the crate name against `hyper` and `hyper-*` -- is *weaker* than the regex, not stricter.
/// A crate named `some-hyper-thing` matches `\bhyper`, because `-` is not a word character
/// and so a boundary exists before `hyper`; a name-prefix comparison misses it entirely.
///
/// Equally, this does not treat `_` as a boundary, so `some_hyper_thing` does not match --
/// because `_` *is* a word character and `\b` does not see a boundary there either. That is
/// the pre-existing behaviour. Tightening it here would be a redesign smuggled in as a
/// refactor, and it belongs in its own change with its own argument.
pub fn contains_at_word_boundary(haystack: &str, needle: &str) -> bool {
    let haystack = haystack.to_ascii_lowercase();
    let needle = needle.to_ascii_lowercase();

    haystack
        .match_indices(&needle)
        .any(|(index, _)| match haystack[..index].chars().next_back() {
            // Start of input is a boundary.
            None => true,
            // `\b` sits between a word character and a non-word one. Word characters are
            // `[A-Za-z0-9_]`, which is why `-` counts as a boundary and `_` does not.
            Some(previous) => !(previous.is_ascii_alphanumeric() || previous == '_'),
        })
}

/// Whether this platform's executables are ELF, and so whether `readelf` can answer the
/// linkage questions at all.
///
/// The two linkage checks are Linux-only, and the asymmetry in how that is handled is
/// deliberate. On a platform whose binaries are Mach-O or PE there is no ELF dynamic section
/// to read, the question is malformed rather than failing, and the tests report themselves
/// skipped so that a contributor on macOS can still run the suite.
///
/// On Linux they always run, and a missing `readelf` is a failure rather than a skip. That
/// half is what stops the checks evaporating quietly on the one platform where they mean
/// something: CI is Linux and already installs a C toolchain to build nghttp2 and ngtcp2, so
/// the failure path should never fire there -- and if a runner image ever changes underneath
/// it, it says so instead of going green.
pub fn platform_uses_elf() -> bool {
    cfg!(target_os = "linux")
}

/// The shared libraries named in an executable's `DT_NEEDED` entries.
///
/// Panics if `readelf` cannot be run. See [`platform_uses_elf`] for why that is a failure
/// rather than a skip: callers are expected to have checked the platform first, so reaching
/// here without binutils means the check could have been made and was not.
pub fn needed_libraries(binary: &std::path::Path) -> Vec<String> {
    let output = Command::new("readelf")
        .arg("-d")
        .arg(binary)
        .output()
        .unwrap_or_else(|e| {
            panic!(
                "could not run `readelf -d {}`: {e}\n\
                 These checks inspect the ELF dynamic section and need binutils installed. \
                 On a Debian-family system: apt-get install binutils.",
                binary.display(),
            )
        });

    assert!(
        output.status.success(),
        "`readelf -d {}` failed with {}",
        binary.display(),
        output.status,
    );

    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| line.contains("(NEEDED)"))
        .filter_map(|line| {
            let start = line.find('[')? + 1;
            let end = line[start..].find(']')? + start;
            Some(line[start..end].to_string())
        })
        .collect()
}

/// The package name on a `cargo tree` dependency line.
///
/// A line looks like `└── ngnet-h3-sys v0.1.0 (/path)`, so the name is the token after
/// the box-drawing prefix. Returning it lets a caller compare names for equality rather than
/// by substring: the shell this replaces asked `grep -q 'ngnet-h3-sys'`, which a sole
/// dependency called `foo-ngnet-h3-sys` would have satisfied. Nothing is named that today,
/// and the point of moving these checks into tests is that they can be made exact rather
/// than merely as good as the grep they came from.
pub fn dependency_name(line: &str) -> String {
    strip_ansi(line)
        .trim_start_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_string()
}

/// Removes ANSI escape sequences from a string.
///
/// `cargo_tree` already asks for `--color=never`, so this should never have anything to do.
/// It is here because the alternative failed in the worst way available: colour made
/// `dependency_name` read the escape as a package name, and it did so only under
/// `CARGO_TERM_COLOR=always` -- so it passed locally and failed on CI. Belt and braces is
/// cheap; a parser that quietly reads terminal formatting as data is not.
fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\u{1b}' {
            out.push(c);
            continue;
        }
        // A CSI sequence is ESC '[', then parameter and intermediate bytes, then a final
        // byte in 0x40..=0x7E. The '[' is itself in that range, so it has to be consumed
        // before the search for the terminator starts -- otherwise the sequence "ends"
        // immediately and its parameters leak into the output as text.
        if chars.peek() == Some(&'[') {
            chars.next();
        }
        for c in chars.by_ref() {
            if ('@'..='~').contains(&c) {
                break;
            }
        }
    }
    out
}

/// Whether a library name is OpenSSL -- either half of it.
///
/// `libssl` and `libcrypto` are checked together wherever the claim is "no TLS at all", and
/// `libssl` alone where the claim is "TLS is present", matching the shell checks these
/// replace. Matching on the `lib` prefix rather than the bare word keeps a crate or file
/// merely called `crypto-something` from being mistaken for the C library.
pub fn is_openssl(library: &str) -> bool {
    names_library(library, "libssl") || names_library(library, "libcrypto")
}

/// Whether a library name is specifically `libssl`.
pub fn is_libssl(library: &str) -> bool {
    names_library(library, "libssl")
}

/// Whether a `DT_NEEDED` entry names the given library.
///
/// The entry is usually a bare soname, but it is permitted to be a path, and a build using
/// a non-default OpenSSL is exactly the case that produces one -- `/opt/openssl/lib/libssl.so.3`
/// rather than `libssl.so.3`. The shell this replaces matched `NEEDED.*lib(ssl|crypto)`,
/// anywhere in the line, so it caught those; anchoring at the start of the string would
/// have quietly narrowed the check while moving it, which is the specific failure this
/// migration is trying not to commit. So the file name is taken first, and the prefix
/// matched against that -- still refusing to mistake `crypto-something` for the C library,
/// but no longer blind to a path.
fn names_library(entry: &str, library: &str) -> bool {
    let entry = entry.to_ascii_lowercase();
    let file_name = entry.rsplit('/').next().unwrap_or(&entry);
    file_name.starts_with(library)
}

#[cfg(test)]
mod tests {
    use super::{contains_at_word_boundary, dependency_name, is_openssl};

    /// Pins the two matchers that the implementation review found narrower than the shell
    /// they replaced.
    #[test]
    fn matchers_are_no_narrower_than_the_shell_they_replace() {
        // A `DT_NEEDED` entry may be a path, which `NEEDED.*lib(ssl|crypto)` matched and an
        // anchored prefix comparison did not.
        assert!(is_openssl("libssl.so.3"));
        assert!(is_openssl("/opt/openssl/lib/libssl.so.3"));
        assert!(is_openssl("/usr/lib/x86_64-linux-gnu/libcrypto.so.3"));
        // Not fooled by something merely named after the concept, as long as it does not
        // start with the library name. `libcryptoki.so` (PKCS#11) does start with it and so
        // matches -- which is what `NEEDED.*lib(ssl|crypto)` did too. Being broader than the
        // property here is safe in a way that being narrower is not: a false positive fails
        // loudly and has never fired, whereas a false negative is a check that has stopped
        // working without saying so.
        assert!(!is_openssl("crypto-helper.so"));
        assert!(!is_openssl("libssh.so.4"));
        assert!(is_openssl("libcryptoki.so"));

        // And the dependency line names a package exactly, rather than containing it.
        assert_eq!(
            dependency_name("\u{2514}\u{2500}\u{2500} ngnet-h3-sys v0.1.0 (/x)"),
            "ngnet-h3-sys"
        );
        // The colourised form, which is what CI produces and what broke once.
        assert_eq!(
            dependency_name("\u{1b}[2m\u{2514}\u{2500}\u{2500}\u{1b}[0m ngnet-h3-sys v0.1.0 (/x)"),
            "ngnet-h3-sys"
        );
        assert_eq!(dependency_name("    ngnet-h3-sys v0.1.0"), "ngnet-h3-sys");
        assert_ne!(
            dependency_name("\u{2514}\u{2500}\u{2500} foo-ngnet-h3-sys v0.1.0"),
            "ngnet-h3-sys"
        );
    }

    /// Pins the boundary rule against the shell it replaces.
    ///
    /// The middle two cases are the ones worth having. `some-hyper-thing` is what a
    /// crate-name-prefix comparison would have missed, and missing it is precisely how this
    /// check would have been quietly weakened in the move out of `ci.yml`. `some_hyper_thing`
    /// is the other edge: `grep -qiE '\bhyper'` does *not* match it, because `_` is a word
    /// character, so neither does this.
    #[test]
    fn word_boundary_matches_what_grep_b_matches() {
        // Real `cargo tree` lines, prefixed with box-drawing characters.
        assert!(contains_at_word_boundary("├── hyper v1.8.1", "hyper"));
        assert!(contains_at_word_boundary(
            "│   ├── hyper-util v0.1.20",
            "hyper"
        ));
        // A hyphen is not a word character, so `\b` sees a boundary here.
        assert!(contains_at_word_boundary(
            "├── some-hyper-thing v0.1.0",
            "hyper"
        ));
        // An underscore is a word character, so `\b` does not.
        assert!(!contains_at_word_boundary(
            "├── some_hyper_thing v0.1.0",
            "hyper"
        ));
        // Nor does a bare alphanumeric run.
        assert!(!contains_at_word_boundary("├── superhyper v0.1.0", "hyper"));
        // Case-insensitive, as `grep -i` was.
        assert!(contains_at_word_boundary("├── Hyper v1.8.1", "hyper"));
        // And the ordinary negative: a graph with nothing of the sort in it.
        assert!(!contains_at_word_boundary("├── ngnet-h2 v0.1.0", "hyper"));
    }
}
