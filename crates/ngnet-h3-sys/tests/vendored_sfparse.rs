//! Guards a hazard created by vendoring two libraries that both embed sfparse.
//!
//! `nghttp2` and `nghttp3` each compile their own copy of the same
//! structured-field parser into their own archive, with external linkage and
//! identical symbol names (`sfparse_parser_init` and friends). Linking both
//! archives into one binary does *not* produce a duplicate-symbol error: the
//! linker simply satisfies libnghttp3's undefined references from whichever
//! archive it reaches first and never pulls libnghttp3's own `sfparse.o`.
//!
//! That is harmless only for as long as the two copies agree. If either
//! submodule is bumped to a revision whose `sfparse_parser` or `sfparse_value`
//! layout has changed, one library ends up calling the other's parser through a
//! mismatched struct — memory corruption, with no link error and no warning to
//! say it happened.
//!
//! There is no linker flag available here that would turn that into an error,
//! so this test makes the *precondition* explicit instead: as long as the two
//! copies are byte-identical, which archive wins cannot matter. A submodule
//! bump that breaks the assumption fails here, loudly, rather than silently
//! months later.
//!
//! If this test ever fails, the fix is not to delete it. Either pin both
//! submodules to revisions carrying the same sfparse, or build one of the
//! libraries with renamed symbols.

use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("crate is two levels below the repository root")
}

#[test]
fn the_two_vendored_copies_of_sfparse_are_identical() {
    let root = repo_root();

    for (nghttp2_relative, nghttp3_relative) in [
        (
            "deps/nghttp2/lib/sfparse.c",
            "deps/nghttp3/lib/sfparse/sfparse.c",
        ),
        (
            "deps/nghttp2/lib/sfparse.h",
            "deps/nghttp3/lib/sfparse/sfparse.h",
        ),
    ] {
        let in_nghttp2 = root.join(nghttp2_relative);
        let in_nghttp3 = root.join(nghttp3_relative);

        // A missing file means a checkout without the submodules, which every
        // other test in this crate would also fail on. Skip rather than add a
        // second confusing failure mode.
        if !in_nghttp2.is_file() || !in_nghttp3.is_file() {
            eprintln!("skipping: {nghttp2_relative} or {nghttp3_relative} is not checked out");
            continue;
        }

        let from_nghttp2 = std::fs::read(&in_nghttp2).unwrap();
        let from_nghttp3 = std::fs::read(&in_nghttp3).unwrap();

        assert_eq!(
            from_nghttp2, from_nghttp3,
            "{nghttp2_relative} and {nghttp3_relative} have diverged.\n\
             Both are compiled with external linkage into their own archive, so a binary \
             linking both silently uses whichever the linker reaches first. While the two \
             agree that is harmless; now that they do not, one library may be calling the \
             other's parser through a different struct layout.\n\
             See this file's module documentation for the two ways out."
        );
    }
}
