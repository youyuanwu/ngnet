//! Per-session allocation accounting.
//!
//! libnghttp2 lets a session carry its own allocator. This crate installs one on every
//! session so that native allocations can be accounted exactly, which is what makes
//! deterministic-teardown testing possible without Valgrind, Miri or a sanitizer — none
//! of which are available here.
//!
//! This is deliberately not public API. The specification excludes allocator injection
//! from the public surface; only the resulting counters are observable, and only from
//! within the crate.

use core::ffi::c_void;
use core::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use nghttp2_sys as sys;

// Declared here rather than taken from the `libc` crate: a second runtime dependency
// would violate the single-dependency rule the crate is held to.
unsafe extern "C" {
    fn malloc(size: usize) -> *mut c_void;
    fn free(ptr: *mut c_void);
    fn calloc(nmemb: usize, size: usize) -> *mut c_void;
    fn realloc(ptr: *mut c_void, size: usize) -> *mut c_void;
}

/// Live-block accounting for one session's native allocations.
///
/// Counts live blocks rather than pairing allocation and free counts, because
/// libnghttp2 reallocates from a null pointer as an ordinary allocation and freeing a
/// null pointer is a no-op. A paired model would never balance.
///
/// Uses atomics rather than `Cell` so the state stays `Sync`; a session may be moved
/// between threads, and its allocator state travels with it.
#[derive(Debug, Default)]
pub(crate) struct AllocState {
    live: AtomicI64,
    total: AtomicU64,
}

impl AllocState {
    /// Blocks currently allocated and not yet freed.
    ///
    /// Zero once a session has been dropped, if the session leaked nothing.
    pub(crate) fn live_blocks(&self) -> i64 {
        self.live.load(Ordering::Relaxed)
    }

    /// Blocks allocated over the lifetime of the session, never decremented.
    ///
    /// Used by tests to confirm the allocator was actually exercised, so that a
    /// balance assertion cannot pass vacuously.
    #[cfg(test)]
    pub(crate) fn total_allocations(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    fn record_alloc(&self) {
        self.live.fetch_add(1, Ordering::Relaxed);
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_free(&self) {
        self.live.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Recovers the accounting state a callback was handed.
///
/// # Safety
///
/// `user_data` must be a pointer to a live `AllocState`, as installed by [`mem_for`].
unsafe fn state<'a>(user_data: *mut c_void) -> &'a AllocState {
    debug_assert!(!user_data.is_null());
    // SAFETY: the caller guarantees this points at a live `AllocState`. The state is
    // held behind an `Arc` owned by the session, so it outlives every callback.
    unsafe { &*(user_data.cast::<AllocState>()) }
}

unsafe extern "C" fn malloc_cb(size: usize, user_data: *mut c_void) -> *mut c_void {
    // SAFETY: `malloc` has no preconditions beyond a valid size.
    let ptr = unsafe { malloc(size) };
    if !ptr.is_null() {
        // SAFETY: `user_data` is the pointer installed by `mem_for`.
        unsafe { state(user_data) }.record_alloc();
    }
    ptr
}

unsafe extern "C" fn free_cb(ptr: *mut c_void, user_data: *mut c_void) {
    // Freeing null is a no-op in C and must not be counted, or the balance drifts.
    if ptr.is_null() {
        return;
    }
    // SAFETY: `user_data` is the pointer installed by `mem_for`.
    unsafe { state(user_data) }.record_free();
    // SAFETY: `ptr` came from this allocator and is freed exactly once by libnghttp2.
    unsafe { free(ptr) };
}

unsafe extern "C" fn calloc_cb(nmemb: usize, size: usize, user_data: *mut c_void) -> *mut c_void {
    // SAFETY: `calloc` has no preconditions beyond valid counts.
    let ptr = unsafe { calloc(nmemb, size) };
    if !ptr.is_null() {
        // SAFETY: `user_data` is the pointer installed by `mem_for`.
        unsafe { state(user_data) }.record_alloc();
    }
    ptr
}

unsafe extern "C" fn realloc_cb(
    ptr: *mut c_void,
    size: usize,
    user_data: *mut c_void,
) -> *mut c_void {
    let was_null = ptr.is_null();
    // SAFETY: `ptr` is either null or a block obtained from this allocator.
    let out = unsafe { realloc(ptr, size) };

    // Reallocating from null is a fresh allocation. Reallocating an existing block frees
    // the old and returns the new, so the live count is unchanged; and if it fails, the
    // old block survives, which is also unchanged.
    if was_null && !out.is_null() {
        // SAFETY: `user_data` is the pointer installed by `mem_for`.
        unsafe { state(user_data) }.record_alloc();
    }
    out
}

/// Builds the `nghttp2_mem` describing this allocator.
///
/// # Safety
///
/// The returned struct borrows `state` as a raw pointer. libnghttp2 copies the struct
/// into the session but not the state behind it, so `state` must remain at a stable
/// address and stay alive for at least as long as the session. Callers satisfy this by
/// keeping the state behind an `Arc` that the session owns.
pub(crate) fn mem_for(state: &AllocState) -> sys::nghttp2_mem {
    sys::nghttp2_mem {
        mem_user_data: (state as *const AllocState).cast_mut().cast::<c_void>(),
        malloc: Some(malloc_cb),
        free: Some(free_cb),
        calloc: Some(calloc_cb),
        realloc: Some(realloc_cb),
    }
}
