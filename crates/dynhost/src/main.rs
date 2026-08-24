//! Loads a compiled plugin (.dylib/.so) from disk at runtime and adapts it
//! to the cordis Plugin trait via the zero-dependency plugin-contract.
//
// The library exports raw C functions only; ALL framework semantics
//! (effects, provide, reactive notify) are applied on the host side, so the
//! plugin can never touch a duplicated tokio/cordis runtime state.

use serde_json::Value;
use std::sync::Arc;

use cordis::{plugin_with, Context, Injection, Plugin};
use libloading::{Library, Symbol};
use plugin_contract::PluginExports;

/// Host-side adapter: wraps raw exports into the object-safe trait.
struct ForeignPlugin {
    name: String,
    exports: Exports,
}

// The exports table points at immutable code in the mapped library; sharing
// it across threads is sound (calls are externally synchronized by fibers).
unsafe impl Send for Exports {}
unsafe impl Sync for Exports {}

/// Owns both the resolved function table and the mapped Library.
#[derive(Clone)]
struct Exports {
    table: *mut PluginExports,
    // Underscored: kept alive so the code stays mapped while in use.
    _lib: Arc<Library>,
}

impl Exports {
    /// Methods force closures to capture the WHOLE Send wrapper instead of
    /// edition-2021 disjoint raw-pointer fields.
    fn setup(&self) -> Handle {
        Handle(unsafe { ((*self.table).setup)() })
    }

    fn produce(&self, handle: &Handle, buf: &mut [u8]) -> Result<String, ()> {
        let mut len = 0usize;
        let rc =
            unsafe { ((*self.table).produce)(handle.0, buf.as_mut_ptr(), buf.len(), &mut len) };
        if rc != 0 {
            return Err(());
        }
        Ok(String::from_utf8_lossy(&buf[..len]).into_owned())
    }

    fn teardown(&self, handle: &Handle) {
        unsafe { ((*self.table).teardown)(handle.0) };
    }
}

/// Opaque plugin handle; the raw pointer crosses threads only inside
/// host-serialized effect code.
struct Handle(*mut usize);
unsafe impl Send for Handle {}

impl ForeignPlugin {
    unsafe fn load(_path: &std::path::Path, lib: Arc<Library>) -> Result<Self, Box<dyn std::error::Error>> {
        let table_sym: Symbol<*mut PluginExports> = unsafe { lib.get(b"cordis_plugin_exports")? };
        let table = *table_sym;
        assert!(!table.is_null(), "plugin exported null table");
        let abi = unsafe { (*table).abi_version };
        assert_eq!(abi, plugin_contract::ABI_VERSION, "ABI version mismatch");

        let mut buf = [0u8; 128];
        let mut len = 0usize;
        unsafe { ((*table).name)(buf.as_mut_ptr(), buf.len(), &mut len) };
        let name = String::from_utf8_lossy(&buf[..len]).into_owned();
        Ok(Self { name, exports: Exports { table, _lib: lib } })
    }
}

impl Plugin for ForeignPlugin {
    fn name(&self) -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Owned(self.name.clone())
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::BoxFuture<cordis::Result<()>> {
            let exports = self.exports.clone();
        Box::pin(async move {
            let handle = exports.setup();
            if handle.0.is_null() {
                return Err(cordis::Error::msg("plugin setup failed"));
            }

            let effect_ctx = ctx.clone();
            ctx.effect("greeter greeting", async move {
                let mut buf = [0u8; 512];
                let greeting = exports.produce(&handle, &mut buf).map_err(|_| cordis::Error::msg("plugin produce failed"))?;
                effect_ctx.provide("greeting", greeting).await?;

                // Disposer calls back INTO the library for pure cleanup;
                // no cordis/tokio types exist on that side.
                let disposer: cordis::fiber::Disposer = Box::new(move || {
                    Box::pin(async move {
                        exports.teardown(&handle);
                    })
                });
                Ok(Some(disposer))
            })
            .await?;
            Ok(())
        })
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = env!("CARGO_MANIFEST_DIR").to_string() + "/../../target/debug";
    let file = if cfg!(target_os = "macos") {
        "libgreeter_plugin.dylib"
    } else if cfg!(windows) {
        "greeter_plugin.dll"
    } else {
        "libgreeter_plugin.so"
    };
    let path = std::path::PathBuf::from(dir).join(file);
    eprintln!("-- loading {} --", path.display());

    let lib = Arc::new(unsafe { Library::new(&path)? });
    let greeter: Arc<dyn Plugin> = Arc::new(unsafe { ForeignPlugin::load(&path, lib.clone())? });
    eprintln!("-- loaded plugin: {} --", greeter.name());

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    rt.block_on(async move {
        let ctx = Context::new();

        // Static consumer depending on the dynamic plugin service; it waits
        // in Pending until the loaded fiber provides.
        let consumer = ctx.plugin(
            plugin_with(
                "consumer",
                vec![Injection::from("greeting")],
                |ctx: Context, _c: Value| async move {
                    let greeting = ctx.require::<String>("greeting")?;
                    ctx.logger().log(
                        cordis::logger::LogLevel::Info,
                        format!("consumer got: {greeting}"),
                    );
                    Ok(())
                },
            ),
            None,
        );

        let fiber = ctx.plugin(greeter, Some(serde_json::json!({})));
        fiber.join().await.expect("dylib plugin activates");
        consumer.join().await.expect("consumer activates on top of it");
        assert_eq!(fiber.state(), cordis::FiberState::Active);
        eprintln!("-- effect metas of the dynamic fiber: {:?} --", fiber.effect_metas());

        fiber.dispose().await;
        consumer.dispose().await;
        ctx.stop().await;
        eprintln!("-- stopped cleanly --");
    });

    // Library intentionally never dlclose-d: ForeignPlugin may outlive any
    // safe unmap point; process exit reclaims everything.
    std::mem::forget(lib);
    eprintln!("-- done --");
    Ok(())
}