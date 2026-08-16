//! Builds the vendored dwnx sources and generates the raw bindings.
//!
//! This is the one `-sys` crate in the workspace that does not drive CMake, because dwnx does
//! not ship a CMakeLists.txt. What it ships is autotools, and the job autotools would do here
//! is small enough to do directly: probe a handful of headers, substitute two values into a
//! version header, and compile 25 C files with no external dependencies. Doing that with `cc`
//! keeps autoconf, automake and libtool off the list of things a contributor needs installed.
//!
//! The probing is the part worth reading carefully. `configure.ac` checks far more than the
//! sources actually consult, and the checks it performs are consumed in two different ways
//! that require two different treatments -- see [`configure_probes`].

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

/// Relative location of the vendored dwnx checkout from this crate's root.
const VENDOR_RELATIVE: &str = "../../deps/dwnx";

/// The C sources that make up libdwnx, from `lib/Makefile.am`'s `OBJECTS`.
///
/// Listed explicitly rather than globbed: a glob would silently pick up any new file dropped
/// into the directory, including ones upstream excludes from the library on purpose.
const SOURCES: &[&str] = &[
    "dwnx_balloc.c",
    "dwnx_buf.c",
    "dwnx_conn.c",
    "dwnx_conv.c",
    "dwnx_err.c",
    "dwnx_fmt.c",
    "dwnx_frame.c",
    "dwnx_gaptr.c",
    "dwnx_idtr.c",
    "dwnx_ksl.c",
    "dwnx_log.c",
    "dwnx_map.c",
    "dwnx_mem.c",
    "dwnx_objalloc.c",
    "dwnx_opl.c",
    "dwnx_pq.c",
    "dwnx_qre.c",
    "dwnx_range.c",
    "dwnx_record_reader.c",
    "dwnx_settings.c",
    "dwnx_str.c",
    "dwnx_strm.c",
    "dwnx_transport_params.c",
    "dwnx_unreachable.c",
    "dwnx_vec.c",
];

/// Package version from the vendored `configure.ac`'s `AC_INIT`.
const PACKAGE_VERSION: &str = "0.0.0-DEV";

/// `PACKAGE_VERSION` packed as `0xMMmmpp`, the transformation `configure.ac` performs with
/// `sed` and `printf`. `0.0.0-DEV` yields zero in all three bytes.
const PACKAGE_VERSION_NUM: &str = "0x000000";

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());

    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-changed=wrapper.h");
    println!("cargo::rerun-if-env-changed=DWNX_SOURCE_DIR");

    let source_dir = dwnx_source_dir(&manifest_dir);
    let lib_dir = source_dir.join("lib");
    println!("cargo::rerun-if-changed={}", lib_dir.display());

    // dwnx's public headers live in the source tree, but `version.h` is generated. Both
    // directories go on the include path, for the C compiler and for bindgen alike.
    let vendored_include = lib_dir.join("includes");
    let generated_include = generate_version_header(&source_dir, &out_dir);

    build_dwnx(&lib_dir, &vendored_include, &generated_include);

    println!("cargo::rustc-link-search=native={}", out_dir.display());
    println!("cargo::rustc-link-lib=static=dwnx");

    // Consumers of this crate (via the `links = "dwnx"` key) get these as
    // DEP_DWNX_ROOT / DEP_DWNX_INCLUDE / DEP_DWNX_LIB.
    println!("cargo::metadata=root={}", source_dir.display());
    println!("cargo::metadata=include={}", vendored_include.display());
    println!("cargo::metadata=lib={}", out_dir.display());

    generate_bindings(
        &manifest_dir,
        &out_dir,
        &vendored_include,
        &generated_include,
    );
}

/// Resolve the dwnx sources, preferring an explicit `DWNX_SOURCE_DIR` override and falling
/// back to the git submodule vendored in this repo.
fn dwnx_source_dir(manifest_dir: &Path) -> PathBuf {
    if let Some(dir) = env::var_os("DWNX_SOURCE_DIR") {
        let dir = PathBuf::from(dir);
        assert!(
            dir.join("lib/includes/dwnx/dwnx.h").is_file(),
            "DWNX_SOURCE_DIR={} does not look like a dwnx checkout",
            dir.display()
        );
        return dir;
    }

    let dir = manifest_dir.join(VENDOR_RELATIVE);
    if !dir.join("lib/includes/dwnx/dwnx.h").is_file() {
        panic!(
            "dwnx sources not found at {}.\n\
             The submodule has not been checked out. Run:\n\n    \
             git submodule update --init deps/dwnx\n\n\
             or `just submodules` to check out every vendored dependency.",
            dir.display()
        );
    }
    dir.canonicalize().unwrap_or(dir)
}

/// Produce `dwnx/version.h` from the vendored `version.h.in`.
///
/// Autotools does this at configure time via `AC_CONFIG_FILES`. The template has exactly two
/// substitutions, so a pair of string replacements is the whole of it. Returns the directory
/// to put on the include path.
fn generate_version_header(source_dir: &Path, out_dir: &Path) -> PathBuf {
    let template = source_dir.join("lib/includes/dwnx/version.h.in");
    println!("cargo::rerun-if-changed={}", template.display());

    let contents = fs::read_to_string(&template)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", template.display()));
    let contents = contents
        .replace("@PACKAGE_VERSION@", PACKAGE_VERSION)
        .replace("@PACKAGE_VERSION_NUM@", PACKAGE_VERSION_NUM);

    let include_dir = out_dir.join("include");
    let header_dir = include_dir.join("dwnx");
    fs::create_dir_all(&header_dir).expect("failed to create generated include directory");
    fs::write(header_dir.join("version.h"), contents).expect("failed to write version.h");

    include_dir
}

/// Apply the `configure.ac` probes that the sources actually consult.
///
/// `configure.ac` checks around thirty things; grepping the library for `HAVE_` shows that all
/// but a handful guard nothing. The ones that remain divide into two kinds, and conflating
/// them is the easy way to get this wrong:
///
/// * **Header probes** are tested with `#ifdef`, so defining one to `0` still includes the
///   header. They must be defined when the header exists and left entirely undefined when it
///   does not.
/// * **Declaration probes** are tested with `#if`, so leaving one undefined evaluates to `0`
///   silently -- which happens to be the right answer, but only by accident, and `-Wundef`
///   would flag it. They are always defined, to `1` or `0`.
///
/// Getting the header probes wrong is not a warning but a build failure or, worse, a silent
/// change of behaviour: with `HAVE_ARPA_INET_H` unset on a unix target, `dwnx_net.h` never
/// includes `<arpa/inet.h>`, and its fallback `dwnx_bswap64` expands to calls to `ntohl` that
/// were never declared.
fn configure_probes(build: &mut cc::Build) {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let target_family = env::var("CARGO_CFG_TARGET_FAMILY").unwrap_or_default();
    let target_env = env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();
    let big_endian = env::var("CARGO_CFG_TARGET_ENDIAN").as_deref() == Ok("big");

    let unix = target_family.split(',').any(|f| f == "unix");
    let windows = target_family.split(',').any(|f| f == "windows");
    let apple = matches!(target_os.as_str(), "macos" | "ios" | "tvos" | "watchos");
    let bsd = matches!(
        target_os.as_str(),
        "freebsd" | "openbsd" | "netbsd" | "dragonfly"
    );

    // Header probes: define only when present.
    if unix {
        build.define("HAVE_ARPA_INET_H", None);
        build.define("HAVE_NETINET_IN_H", None);
        build.define("HAVE_UNISTD_H", None);
    }
    // `endian.h` is glibc/musl and modern BSD; Apple has neither it nor `sys/endian.h`, and
    // reaches its 64-bit swaps through `OSByteOrder.h`, which `dwnx_net.h` includes on
    // `__APPLE__` without asking configure.
    if target_os == "linux" || target_os == "android" {
        build.define("HAVE_ENDIAN_H", None);
        build.define("HAVE_BYTESWAP_H", None);

        // What `AC_USE_SYSTEM_EXTENSIONS` does, and it is load-bearing rather than tidiness.
        // glibc's `endian.h` only *declares* `be64toh`/`htobe64` when a feature-test macro
        // asks for them. Without this the header is included, the declarations are absent, C
        // treats the calls as implicit, and the build gets all the way to a link error naming
        // two symbols that were never compiled -- which is a confusing way to learn that a
        // configure step was skipped.
        build.define("_GNU_SOURCE", None);
    } else if bsd {
        build.define("HAVE_SYS_ENDIAN_H", None);
    }

    // Declaration probes: always defined, to 1 or 0.
    //
    // `be64toh` comes with `endian.h`/`sys/endian.h`; `bswap_64` with glibc's `byteswap.h`.
    // Where neither is available `dwnx_net.h` still has a path -- Apple's `OSSwapInt64`,
    // Windows' `_byteswap_uint64`, or a portable pair of `ntohl` calls.
    let have_be64toh = target_os == "linux" || target_os == "android" || bsd;
    let have_bswap_64 = (target_os == "linux" || target_os == "android") && target_env != "musl";
    build.define("HAVE_DECL_BE64TOH", if have_be64toh { "1" } else { "0" });
    build.define("HAVE_DECL_BSWAP_64", if have_bswap_64 { "1" } else { "0" });

    // Only consulted when `HAVE_DECL_BE64TOH` is 0, but cheap to get right rather than rely on
    // that. Everything the workspace targets is little-endian; this is here so the one that is
    // not does not silently byte-swap.
    if big_endian {
        build.define("WORDS_BIGENDIAN", None);
    }

    // dwnx's byte-order header gates its Windows path on `WIN32`, not `_WIN32`. MSVC and
    // clang-cl predefine only the underscored spellings, so on a `*-pc-windows-msvc` target
    // the check fails, the `_byteswap_*` path is skipped, and `dwnx_bswap64` falls through to
    // a portable fallback calling `ntohl` -- which is neither declared nor linked, since
    // nothing includes `<winsock2.h>` or links `ws2_32`. The result is a link error naming
    // symbols no source appears to call. The MinGW target defines `WIN32` itself and is
    // unaffected; defining it unconditionally for the family is harmless there.
    if windows {
        build.define("WIN32", None);
    }

    // Deliberately *not* defined: `HAVE_CONFIG_H`. Every dwnx header guards its `config.h`
    // include with it, so leaving it unset means the generated config autotools would have
    // produced is simply not needed -- the defines above take its place.
    let _ = apple;
}

/// Compile libdwnx into a static archive in OUT_DIR.
fn build_dwnx(lib_dir: &Path, vendored_include: &Path, generated_include: &Path) {
    let mut build = cc::Build::new();

    build
        .include(vendored_include)
        .include(generated_include)
        // The sources include their private headers unqualified (`#include "dwnx_conn.h"`),
        // which resolves relative to each source file, but the compiler is invoked from
        // elsewhere -- so the library directory itself has to be on the path too.
        .include(lib_dir)
        // Matches `AM_CPPFLAGS` in lib/Makefile.am. On Windows this is what makes DWNX_EXTERN
        // an export rather than an import; on other platforms it sets default visibility.
        .define("BUILDING_DWNX", None)
        // Consumers link the archive directly, so the public header must not decorate its
        // declarations with `dllimport`.
        .define("DWNX_STATICLIB", None)
        .warnings(false)
        .std("c11");

    configure_probes(&mut build);

    for source in SOURCES {
        build.file(lib_dir.join(source));
    }

    build.compile("dwnx");
}

fn generate_bindings(
    manifest_dir: &Path,
    out_dir: &Path,
    vendored_include: &Path,
    generated_include: &Path,
) {
    let bindings = bindgen::Builder::default()
        .header(manifest_dir.join("wrapper.h").to_str().unwrap())
        .clang_arg(format!("-I{}", vendored_include.display()))
        .clang_arg(format!("-I{}", generated_include.display()))
        // Without this the header decorates every declaration with a dllimport attribute on
        // Windows, which bindgen would carry into the generated bindings.
        .clang_arg("-DDWNX_STATICLIB")
        .allowlist_function("dwnx_.*")
        .allowlist_type("dwnx_.*")
        .allowlist_var("DWNX_.*")
        .allowlist_var("NGNET_QMUX_.*")
        // Plain constants rather than Rust enums: dwnx returns error codes and frame types as
        // ints, and a value outside the enumerated set would be undefined behaviour in a real
        // Rust enum.
        .default_enum_style(bindgen::EnumVariation::Consts)
        .derive_default(true)
        .derive_debug(true)
        .prepend_enum_name(false)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("failed to generate dwnx bindings");

    bindings
        .write_to_file(out_dir.join("bindings.rs"))
        .expect("failed to write bindings.rs");
}
