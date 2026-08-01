use std::env;
use std::path::{Path, PathBuf};

/// Relative location of the vendored nghttp2 checkout from this crate's root.
const VENDOR_RELATIVE: &str = "../../deps/nghttp2";

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());

    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-changed=wrapper.h");
    println!("cargo::rerun-if-env-changed=NGHTTP2_SOURCE_DIR");

    let source_dir = nghttp2_source_dir(&manifest_dir);
    println!("cargo::rerun-if-changed={}", source_dir.join("lib").display());
    println!(
        "cargo::rerun-if-changed={}",
        source_dir.join("CMakeLists.txt").display()
    );

    let install_dir = build_nghttp2(&source_dir);
    let lib_dir = find_lib_dir(&install_dir);
    let include_dir = install_dir.join("include");

    println!("cargo::rustc-link-search=native={}", lib_dir.display());
    println!("cargo::rustc-link-lib=static=nghttp2");

    // Consumers of this crate (via the `links = "nghttp2"` key) get these as
    // DEP_NGHTTP2_ROOT / DEP_NGHTTP2_INCLUDE / DEP_NGHTTP2_LIB.
    println!("cargo::metadata=root={}", install_dir.display());
    println!("cargo::metadata=include={}", include_dir.display());
    println!("cargo::metadata=lib={}", lib_dir.display());

    generate_bindings(&manifest_dir, &include_dir);
}

/// Resolve the nghttp2 sources, preferring an explicit `NGHTTP2_SOURCE_DIR`
/// override and falling back to the git submodule vendored in this repo.
fn nghttp2_source_dir(manifest_dir: &Path) -> PathBuf {
    if let Some(dir) = env::var_os("NGHTTP2_SOURCE_DIR") {
        let dir = PathBuf::from(dir);
        assert!(
            dir.join("CMakeLists.txt").is_file(),
            "NGHTTP2_SOURCE_DIR={} does not look like an nghttp2 checkout",
            dir.display()
        );
        return dir;
    }

    let dir = manifest_dir.join(VENDOR_RELATIVE);
    if !dir.join("CMakeLists.txt").is_file() {
        panic!(
            "nghttp2 sources not found at {}.\n\
             The submodule has not been checked out. Run:\n\n    \
             git submodule update --init deps/nghttp2\n\n\
             (do not use --recursive; nghttp2's nested submodules are not needed)",
            dir.display()
        );
    }
    dir.canonicalize().unwrap_or(dir)
}

/// Configure and build libnghttp2 as a static library, installing into OUT_DIR.
fn build_nghttp2(source_dir: &Path) -> PathBuf {
    let mut config = cmake::Config::new(source_dir);

    config
        // Build libnghttp2 only: no nghttpx/nghttp/h2load, no examples, no
        // HPACK tools. Those are the targets that would pull in nghttp2's
        // nested submodules (mruby, neverbleed, urlparse) and extra system
        // dependencies such as libev, libevent and OpenSSL.
        .define("ENABLE_LIB_ONLY", "ON")
        // We want a static archive to link into rlibs, not a shared object.
        .define("BUILD_SHARED_LIBS", "OFF")
        .define("BUILD_STATIC_LIBS", "ON")
        // BUILD_TESTING defaults to ON whenever BUILD_STATIC_LIBS is ON, and
        // the test suite needs the tests/munit submodule. Turn it off.
        .define("BUILD_TESTING", "OFF")
        .define("ENABLE_DOC", "OFF")
        .define("ENABLE_WERROR", "OFF")
        // HTTP/3 needs ngtcp2/nghttp3; this crate targets HTTP/2 (incl. h2c).
        .define("ENABLE_HTTP3", "OFF")
        .define("WITH_LIBXML2", "OFF")
        .define("WITH_JEMALLOC", "OFF")
        // Required so the archive can be linked into cdylibs and proc macros.
        .define("CMAKE_POSITION_INDEPENDENT_CODE", "ON");

    if env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") {
        let static_crt = env::var("CARGO_ENCODED_RUSTFLAGS")
            .unwrap_or_default()
            .contains("crt-static");
        config.define("ENABLE_STATIC_CRT", if static_crt { "ON" } else { "OFF" });
    }

    config.build()
}

/// nghttp2 installs via GNUInstallDirs, so the archive lands in `lib` or
/// `lib64` depending on the platform.
fn find_lib_dir(install_dir: &Path) -> PathBuf {
    for candidate in ["lib", "lib64"] {
        let dir = install_dir.join(candidate);
        if dir.join("libnghttp2.a").is_file() || dir.join("nghttp2.lib").is_file() {
            return dir;
        }
    }
    panic!(
        "could not locate the built nghttp2 static library under {}",
        install_dir.display()
    );
}

fn generate_bindings(manifest_dir: &Path, include_dir: &Path) {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());

    let bindings = bindgen::Builder::default()
        .header(manifest_dir.join("wrapper.h").to_str().unwrap())
        .clang_arg(format!("-I{}", include_dir.display()))
        // Matches the PUBLIC compile definition on the nghttp2_static target,
        // so NGHTTP2_EXTERN does not expand to a dllimport declspec.
        .clang_arg("-DNGHTTP2_STATICLIB")
        .allowlist_function("nghttp2_.*")
        .allowlist_type("nghttp2_.*")
        .allowlist_var("NGHTTP2_.*")
        // Plain constants rather than Rust enums: nghttp2 returns error codes
        // and frame types as ints, and values outside the enumerated set would
        // be undefined behaviour in a real Rust enum.
        .default_enum_style(bindgen::EnumVariation::Consts)
        .derive_default(true)
        .derive_debug(true)
        .prepend_enum_name(false)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("failed to generate nghttp2 bindings");

    bindings
        .write_to_file(out_dir.join("bindings.rs"))
        .expect("failed to write bindings.rs");
}
