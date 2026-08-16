#ifndef NGNET_QMUX_SYS_WRAPPER_H
#define NGNET_QMUX_SYS_WRAPPER_H

/* `version.h` is generated from `version.h.in` at configure time. Autotools would do it; here
   the build script does, into OUT_DIR, and puts that directory on bindgen's include path
   alongside the vendored one. */
#include <dwnx/version.h>
#include <dwnx/dwnx.h>

/* ---------------------------------------------------------------------------
   Constants bindgen cannot evaluate on its own.

   dwnx writes its time units with a cast -- `((dwnx_duration)1000ULL)`,
   `((dwnx_duration)(1000ULL * DWNX_MICROSECONDS))` -- and bindgen's macro evaluator gives up
   on those without saying so. The constant is simply absent from the generated bindings, and
   the absence is only noticed by whichever wrapper needed it. `ngnet-quic-sys` hit the same
   thing with ngtcp2 and answered it the same way; this is the smaller version of that file.

   Each is restated below without the cast so bindgen can evaluate it, and pinned to the
   header's own value by a _Static_assert. A value that diverges upstream becomes a compile
   error naming the constant, rather than a wrapper quietly computing timeouts in the wrong
   unit.

   The prefix keeps the restatements distinguishable from the real macros in the generated
   bindings; the safe crate re-exports them under their proper names.
   --------------------------------------------------------------------------- */

#define NGNET_QMUX_NANOSECONDS 1ULL
#define NGNET_QMUX_MICROSECONDS 1000ULL
#define NGNET_QMUX_MILLISECONDS 1000000ULL
#define NGNET_QMUX_SECONDS 1000000000ULL
#define NGNET_QMUX_MINUTES 60000000000ULL

_Static_assert(NGNET_QMUX_NANOSECONDS == DWNX_NANOSECONDS,
               "DWNX_NANOSECONDS changed upstream");
_Static_assert(NGNET_QMUX_MICROSECONDS == DWNX_MICROSECONDS,
               "DWNX_MICROSECONDS changed upstream");
_Static_assert(NGNET_QMUX_MILLISECONDS == DWNX_MILLISECONDS,
               "DWNX_MILLISECONDS changed upstream");
_Static_assert(NGNET_QMUX_SECONDS == DWNX_SECONDS, "DWNX_SECONDS changed upstream");
_Static_assert(NGNET_QMUX_MINUTES == DWNX_MINUTES, "DWNX_MINUTES changed upstream");

/* `DWNX_MAX_VARINT` is `((1ULL << 62) - 1)`, which bindgen does evaluate -- no cast is
   involved. It is restated anyway because the safe crate validates against it on every stream
   id and transport parameter, and a silent change to the variable-length integer bound would
   be a protocol-level bug rather than a compile error. */
#define NGNET_QMUX_MAX_VARINT ((1ULL << 62) - 1)
_Static_assert(NGNET_QMUX_MAX_VARINT == DWNX_MAX_VARINT, "DWNX_MAX_VARINT changed upstream");

#endif /* NGNET_QMUX_SYS_WRAPPER_H */
