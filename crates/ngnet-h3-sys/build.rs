use std::env;
use std::path::{Path, PathBuf};

/// Relative location of the vendored nghttp3 checkout from this crate's root.
const VENDOR_RELATIVE: &str = "../../deps/nghttp3";

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());

    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-changed=wrapper.h");
    println!("cargo::rerun-if-env-changed=NGHTTP3_SOURCE_DIR");

    let source_dir = nghttp3_source_dir(&manifest_dir);
    println!(
        "cargo::rerun-if-changed={}",
        source_dir.join("lib").display()
    );
    println!(
        "cargo::rerun-if-changed={}",
        source_dir.join("CMakeLists.txt").display()
    );

    let install_dir = build_nghttp3(&source_dir);
    let lib_dir = find_lib_dir(&install_dir);
    let include_dir = install_dir.join("include");

    println!("cargo::rustc-link-search=native={}", lib_dir.display());
    println!("cargo::rustc-link-lib=static=nghttp3");

    // Consumers of this crate (via the `links = "nghttp3"` key) get these as
    // DEP_NGHTTP3_ROOT / DEP_NGHTTP3_INCLUDE / DEP_NGHTTP3_LIB.
    println!("cargo::metadata=root={}", install_dir.display());
    println!("cargo::metadata=include={}", include_dir.display());
    println!("cargo::metadata=lib={}", lib_dir.display());

    generate_bindings(&manifest_dir, &include_dir);
}

/// Resolve the nghttp3 sources, preferring an explicit `NGHTTP3_SOURCE_DIR`
/// override and falling back to the git submodule vendored in this repo.
fn nghttp3_source_dir(manifest_dir: &Path) -> PathBuf {
    if let Some(dir) = env::var_os("NGHTTP3_SOURCE_DIR") {
        let dir = PathBuf::from(dir);
        assert!(
            dir.join("CMakeLists.txt").is_file(),
            "NGHTTP3_SOURCE_DIR={} does not look like an nghttp3 checkout",
            dir.display()
        );
        check_sfparse(&dir);
        return dir;
    }

    let dir = manifest_dir.join(VENDOR_RELATIVE);
    if !dir.join("CMakeLists.txt").is_file() {
        panic!(
            "nghttp3 sources not found at {}.\n\
             The submodule has not been checked out. Run:\n\n    \
             just submodules\n\n\
             (do not use `git submodule update --init --recursive`; nghttp3's test\n\
             submodules are not needed, but `lib/sfparse` is, and `just submodules`\n\
             checks out exactly that set)",
            dir.display()
        );
    }
    check_sfparse(&dir);
    dir.canonicalize().unwrap_or(dir)
}

/// `lib/sfparse` is a nested submodule, but unlike nghttp2's nested submodules
/// it is compiled *into* libnghttp3 rather than into its tests. A checkout that
/// skipped it fails deep inside CMake with a missing source file, so check here
/// and say which command fixes it.
fn check_sfparse(source_dir: &Path) {
    if !source_dir.join("lib/sfparse/sfparse.c").is_file() {
        panic!(
            "nghttp3's `lib/sfparse` submodule is missing from {}.\n\
             It is compiled into libnghttp3 itself, not just its tests, so the\n\
             library cannot be built without it. Run:\n\n    \
             just submodules\n",
            source_dir.display()
        );
    }
}

/// Configure and build libnghttp3 as a static library, installing into OUT_DIR.
fn build_nghttp3(source_dir: &Path) -> PathBuf {
    let mut config = cmake::Config::new(source_dir);

    config
        // Build libnghttp3 only: no examples, and no C++ toolchain probe.
        .define("ENABLE_LIB_ONLY", "ON")
        // We want a static archive to link into rlibs, not a shared object.
        .define("ENABLE_STATIC_LIB", "ON")
        .define("ENABLE_SHARED_LIB", "OFF")
        // BUILD_TESTING is a dependent option that defaults to ON whenever
        // ENABLE_STATIC_LIB is on, and the test suite needs the tests/munit
        // submodule this repo deliberately does not check out.
        .define("BUILD_TESTING", "OFF")
        .define("ENABLE_WERROR", "OFF")
        // Required so the archive can be linked into cdylibs and proc macros.
        .define("CMAKE_POSITION_INDEPENDENT_CODE", "ON");

    // Deliberately absent, unlike the nghttp2 build script: nghttp3 has no
    // ENABLE_DOC, ENABLE_HTTP3, WITH_LIBXML2 or WITH_JEMALLOC options, and no
    // system dependencies at all — no TLS, no QUIC, no libev, no zlib.

    if env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") {
        let static_crt = env::var("CARGO_ENCODED_RUSTFLAGS")
            .unwrap_or_default()
            .contains("crt-static");
        config.define("ENABLE_STATIC_CRT", if static_crt { "ON" } else { "OFF" });
    }

    config.build()
}

/// nghttp3 installs via GNUInstallDirs, so the archive lands in `lib` or
/// `lib64` depending on the platform.
///
/// Only the unsuffixed name is accepted. nghttp3 appends `_static` only when
/// shared and static are built together, which this configuration rules out by
/// forcing `ENABLE_SHARED_LIB=OFF` — and the link line above asks for
/// `nghttp3`, so accepting a suffixed archive here would turn a clear panic
/// into an obscure linker error.
fn find_lib_dir(install_dir: &Path) -> PathBuf {
    for candidate in ["lib", "lib64"] {
        let dir = install_dir.join(candidate);
        if dir.join("libnghttp3.a").is_file() || dir.join("nghttp3.lib").is_file() {
            return dir;
        }
    }
    panic!(
        "could not locate the built nghttp3 static library under {}",
        install_dir.display()
    );
}

fn generate_bindings(manifest_dir: &Path, include_dir: &Path) {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());

    let bindings = bindgen::Builder::default()
        .header(manifest_dir.join("wrapper.h").to_str().unwrap())
        // The *install* include dir, not the source tree: `nghttp3/version.h`
        // is generated by CMake and exists only here.
        .clang_arg(format!("-I{}", include_dir.display()))
        // Matches the PUBLIC compile definition on the nghttp3_static target,
        // so NGHTTP3_EXTERN does not expand to a dllimport declspec.
        .clang_arg("-DNGHTTP3_STATICLIB")
        .allowlist_function("nghttp3_.*")
        .allowlist_type("nghttp3_.*")
        .allowlist_var("NGHTTP3_.*")
        // Plain constants rather than Rust enums: nghttp3 returns error codes
        // and frame types as ints, and values outside the enumerated set would
        // be undefined behaviour in a real Rust enum.
        .default_enum_style(bindgen::EnumVariation::Consts)
        .derive_default(true)
        .derive_debug(true)
        .prepend_enum_name(false)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("failed to generate nghttp3 bindings");

    bindings
        .write_to_file(out_dir.join("bindings.rs"))
        .expect("failed to write bindings.rs");
}
