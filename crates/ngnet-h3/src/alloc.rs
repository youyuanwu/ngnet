//! Per-connection allocation accounting.
//!
//! nghttp3 lets a connection carry its own allocator. This crate installs one on every
//! connection so native allocations can be accounted exactly, which is what makes
//! deterministic teardown testing possible without Valgrind, Miri or a sanitizer — none
//! of which are available here. It is what lets a test assert that dropping a connection
//! released every retained body buffer, rather than merely hoping so.
//!
//! **This differs from the HTTP/2 crate in a way that matters.** `nghttp2_session` stores
//! `nghttp2_mem` *by value*, so `ngnet-h2` can build one on the stack and hand it over.
//! `nghttp3_conn` stores `const nghttp3_mem *` — a pointer — and uses it for the whole
//! life of the connection, including inside `nghttp3_conn_del`. Handing nghttp3 a stack
//! local would leave it dereferencing a dead frame. The struct itself must therefore be
//! owned at a stable address, which is why [`Allocator`] owns both halves and is only
//! ever used from behind a `Box`.
//!
//! This is deliberately not public API: only the resulting counters are observable, and
//! only from within the crate.

use core::ffi::c_void;
use core::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use ngnet_h3_sys as sys;

// Declared here rather than taken from the `libc` crate: a second runtime dependency
// would violate the single-dependency rule this crate is held to.
unsafe extern "C" {
    fn malloc(size: usize) -> *mut c_void;
    fn free(ptr: *mut c_void);
    fn calloc(nmemb: usize, size: usize) -> *mut c_void;
    fn realloc(ptr: *mut c_void, size: usize) -> *mut c_void;
}

/// Live-block accounting for one connection's native allocations.
///
/// Counts live blocks rather than pairing allocation and free counts, because nghttp3
/// reallocates from a null pointer as an ordinary allocation and freeing a null pointer
/// is a no-op. A paired model would never balance.
#[derive(Debug, Default)]
pub(crate) struct AllocState {
    live: AtomicI64,
    total: AtomicU64,
}

impl AllocState {
    /// Blocks currently allocated and not yet freed.
    ///
    /// Zero once a connection has been dropped, if the connection leaked nothing.
    pub(crate) fn live_blocks(&self) -> i64 {
        self.live.load(Ordering::Relaxed)
    }

    /// Blocks allocated over the lifetime of the connection, never decremented.
    ///
    /// Used by tests to confirm the allocator was actually exercised, so a balance
    /// assertion cannot pass vacuously.
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

/// The allocator handed to a connection, owning both the accounting state and the
/// `nghttp3_mem` struct that points at it.
///
/// Both halves must stay at fixed addresses for as long as the connection lives, so this
/// is constructed into a `Box` and never moved out of one. Keeping them in one allocation
/// means there is a single thing to keep alive rather than two that must agree.
pub(crate) struct Allocator {
    state: AllocState,
    mem: sys::nghttp3_mem,
}

impl Allocator {
    /// Builds an allocator whose `nghttp3_mem` points at its own state.
    ///
    /// The self-reference is set up after boxing, because the address is not knowable
    /// before then.
    pub(crate) fn new() -> Box<Self> {
        let mut allocator = Box::new(Self {
            state: AllocState::default(),
            mem: sys::nghttp3_mem {
                user_data: core::ptr::null_mut(),
                malloc: Some(malloc_cb),
                free: Some(free_cb),
                calloc: Some(calloc_cb),
                realloc: Some(realloc_cb),
            },
        });

        let state: *const AllocState = &allocator.state;
        allocator.mem.user_data = state.cast_mut().cast::<c_void>();
        allocator
    }

    /// The accounting counters.
    pub(crate) fn state(&self) -> &AllocState {
        &self.state
    }

    /// The `nghttp3_mem` to hand to a constructor.
    ///
    /// # Safety
    ///
    /// nghttp3 stores this pointer, not a copy of what it points at, so the `Allocator`
    /// must outlive the connection it is given to.
    pub(crate) fn as_mem_ptr(&self) -> *const sys::nghttp3_mem {
        &self.mem
    }
}

// SAFETY: `Allocator` owns only atomics and a struct of function pointers, all of which
// are `Send`. The `user_data` pointer refers into the same allocation, so moving the
// `Box` between threads moves the whole thing consistently.
unsafe impl Send for Allocator {}

/// Recovers the accounting state a callback was handed.
///
/// # Safety
///
/// `user_data` must be a pointer to a live `AllocState`, as installed by
/// [`Allocator::new`].
unsafe fn state<'a>(user_data: *mut c_void) -> &'a AllocState {
    debug_assert!(!user_data.is_null());
    // SAFETY: the caller guarantees this points at a live `AllocState`, which is owned by
    // the boxed `Allocator` the connection keeps alive for longer than itself.
    unsafe { &*(user_data.cast::<AllocState>()) }
}

unsafe extern "C" fn malloc_cb(size: usize, user_data: *mut c_void) -> *mut c_void {
    // SAFETY: `malloc` has no preconditions beyond a valid size.
    let ptr = unsafe { malloc(size) };
    if !ptr.is_null() {
        // SAFETY: `user_data` is the pointer installed by `Allocator::new`.
        unsafe { state(user_data) }.record_alloc();
    }
    ptr
}

unsafe extern "C" fn free_cb(ptr: *mut c_void, user_data: *mut c_void) {
    // Freeing null is a no-op in C and must not be counted, or the balance drifts.
    if ptr.is_null() {
        return;
    }
    // SAFETY: `user_data` is the pointer installed by `Allocator::new`.
    unsafe { state(user_data) }.record_free();
    // SAFETY: `ptr` came from this allocator and is freed exactly once by nghttp3.
    unsafe { free(ptr) };
}

unsafe extern "C" fn calloc_cb(nmemb: usize, size: usize, user_data: *mut c_void) -> *mut c_void {
    // SAFETY: `calloc` has no preconditions beyond valid counts.
    let ptr = unsafe { calloc(nmemb, size) };
    if !ptr.is_null() {
        // SAFETY: `user_data` is the pointer installed by `Allocator::new`.
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
    //
    // The remaining case is `realloc(non_null, 0)`, which C permits to free the block and
    // return null. Current nghttp3 never reallocates to zero, but accounting for it costs
    // nothing and stops a future upgrade silently skewing the balance.
    if was_null {
        if !out.is_null() {
            // SAFETY: `user_data` is the pointer installed by `Allocator::new`.
            unsafe { state(user_data) }.record_alloc();
        }
    } else if out.is_null() && size == 0 {
        // SAFETY: `user_data` is the pointer installed by `Allocator::new`.
        unsafe { state(user_data) }.record_free();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_mem_struct_points_at_its_own_state() {
        let allocator = Allocator::new();
        let mem = allocator.as_mem_ptr();
        // SAFETY: `mem` points into the live boxed allocator.
        let user_data = unsafe { (*mem).user_data };
        assert_eq!(
            user_data.cast::<AllocState>().cast_const(),
            &allocator.state as *const AllocState,
            "nghttp3 would be handed a pointer to the wrong state"
        );
    }

    #[test]
    fn the_pointer_survives_moving_the_box() {
        let allocator = Allocator::new();
        let before = allocator.as_mem_ptr();
        // Moving the `Box` moves the pointer to the heap block, not the block itself.
        let moved = allocator;
        assert_eq!(before, moved.as_mem_ptr());
    }

    #[test]
    fn allocation_is_balanced() {
        let allocator = Allocator::new();
        let user_data = allocator.mem.user_data;

        // SAFETY: exercising the callbacks exactly as nghttp3 would.
        unsafe {
            let a = malloc_cb(64, user_data);
            let b = calloc_cb(4, 16, user_data);
            assert_eq!(allocator.state().live_blocks(), 2);

            let a = realloc_cb(a, 128, user_data);
            assert_eq!(
                allocator.state().live_blocks(),
                2,
                "realloc is not a new block"
            );

            free_cb(a, user_data);
            free_cb(b, user_data);
            free_cb(core::ptr::null_mut(), user_data);
        }

        assert_eq!(allocator.state().live_blocks(), 0);
        assert_eq!(allocator.state().total_allocations(), 2);
    }
}
