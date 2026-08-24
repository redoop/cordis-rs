//! Greeter plugin as a pure C-ABI cdylib.
//!
//! Note the dependency list: ONLY plugin-contract. No cordis, no tokio - so
//! there is no duplicated runtime state and no way for this library to
//! interfere with the host reactor. All framework semantics (effects,
//! provide, notify) are supplied by the host wrapping these exports.

use plugin_contract::{write_out, PluginExports, ABI_VERSION};

const GREETING: &[u8] = b"hello from a runtime-loaded dylib";

struct State {
    invocations: usize,
}

unsafe extern "C" fn name(out_buf: *mut u8, cap: usize, out_len: *mut usize) {
    write_out(b"greeter(dylib)", out_buf, cap, out_len);
}

unsafe extern "C" fn setup() -> *mut usize {
    let state = Box::new(State { invocations: 0 });
    Box::into_raw(state) as *mut usize
}

unsafe extern "C" fn produce(
    handle: *mut usize,
    out_buf: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    let state = unsafe { &mut *(handle as *mut State) };
    state.invocations += 1;
    if write_out(GREETING, out_buf, cap, out_len) {
        0
    } else {
        -1
    }
}

unsafe extern "C" fn teardown(handle: *mut usize) {
    if handle.is_null() {
        return;
    }
    let state = unsafe { Box::from_raw(handle as *mut State) };
    eprintln!(
        "      [greeter(dylib)] teardown; produced {} greeting(s)",
        state.invocations
    );
}

#[no_mangle]
pub static cordis_plugin_exports: PluginExports = PluginExports {
    abi_version: ABI_VERSION,
    name,
    setup,
    produce,
    teardown,
};
