//! Minimal stable-surface contract between a cordis host and a dynamically
//! loaded plugin. Deliberately contains NO cordis and NO tokio: both sides
//! speak only raw C-ABI functions and caller-owned buffers.
//!
//! Why not pass Arc<dyn Plugin> across the boundary? A cdylib statically
//! links its own copies of every crate it uses. Two tokio instances have
//! separate thread-local runtime handles, so any framework call made from
//! library-side code panics with "no reactor running". Inverting the API -
//! the HOST implements the framework, the PLUGIN only computes - removes the
//! duplication entirely.

/// One loaded plugin, as seen by the host. All functions are infallible from
/// the ABI perspective; errors are reported through the returned status.
#[repr(C)]
pub struct PluginExports {
    /// ABI version, bumped on incompatible changes. Host rejects mismatches.
    pub abi_version: u32,

    /// Write the plugin display name into out_buf (capacity cap); set *out_len.
    pub name: unsafe extern "C" fn(out_buf: *mut u8, cap: usize, out_len: *mut usize),

    /// Allocate plugin-private state; returned handle is opaque to the host.
    /// May return null to signal init failure.
    pub setup: unsafe extern "C" fn() -> *mut usize,

    /// Produce the service value payload (here: UTF-8 bytes of the greeting).
    pub produce: unsafe extern "C" fn(handle: *mut usize, out_buf: *mut u8, cap: usize, out_len: *mut usize) -> i32,

    /// Release plugin-private state. Called exactly once per successful setup.
    pub teardown: unsafe extern "C" fn(handle: *mut usize),
}

pub const ABI_VERSION: u32 = 1;

/// Helper for plugins: copy a byte slice into an out-buffer protocol.
pub fn write_out(src: &[u8], out_buf: *mut u8, cap: usize, out_len: *mut usize) -> bool {
    if src.len() > cap {
        return false;
    }
    unsafe {
        std::ptr::copy_nonoverlapping(src.as_ptr(), out_buf, src.len());
        *out_len = src.len();
    }
    true
}
