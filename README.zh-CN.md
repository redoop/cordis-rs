# cordis-rust — 一切皆插件（Rust 版）

[![crates.io](https://img.shields.io/crates/v/cordis-rust)](https://crates.io/crates/cordis-rust)
[![docs.rs](https://img.shields.io/docsrs/cordis-rust)](https://docs.rs/cordis-rust)

DSH 插件系统所依赖的 cordis 内核的 Rust 移植版。保留相同的思维模型与
生命周期文法，按 Rust 的所有权模型做了适配：运行时是 tokio，插件是
`Send` 的 future，服务存放在以 `(服务名, 隔离作用域)` 为键的两层存储中。

crate 在 crates.io 上以 **`cordis-rust`** 发布；库目标名保持 **`cordis`**，
因此使用方照常写 `use cordis::…`：

```toml
[dependencies]
cordis-rust = "0.2"
```

```rust
use cordis::{Context, plugin, plugin_with, Injection};

// 插件就是「(ctx, config) -> future」，可选声明所需服务；
// 依赖图由内核自动收敛。
let api = ctx.plugin(plugin_with(
    "api",
    vec![Injection::new("database")],
    |ctx: Context, _config: ()| async move {
        let db = ctx.require::<Database>("database")?;
        // ... 提供服务 ...
        Ok(())
    },
), None);
```

## 核心思想

不需要任何手工连线。一个**插件**声明它 *需要什么*（inject）与
*提供什么*（`ctx.provide`）。内核自动计算依赖图，并把每个 fiber 收敛到
它期望的 epoch：

    consumer 先注册（PENDING）
          |  provider 出现
          v
    provider LOADING --> ACTIVE --notify--> consumer 重新检查 --> ACTIVE

卸载则反向进行：dispose provider 后，依赖者级联回到 Pending；重新提供
（或热更新）时，它们以全新的快照重新加载。

## 概念对照：cordis (JS) vs cordis-rust

| cordis (JS) | cordis-rust | 说明 |
| --- | --- | --- |
| Context | Context（Clone，廉价） | extend / isolate / intercept 作用域链 |
| Plugin (ctx, config) | trait Plugin + plugin() / plugin_with() 闭包 | apply 前经 serde 校验配置 |
| inject: string[] | Injection::from("name") 列表 | 严格解析 + 响应式通知键 |
| Fiber states | FiberState::{Pending, Loading, Active, Unloading, Disposed, Failed} | 与 JS 文法一致 |
| ctx.provide(name, value) | ctx.provide(name, value).await | 异步；注册撤销 effect |
| ctx.effect(name, cb) | ctx.effect(name, future).await -> Option\<Disposer\> | disposer 不可失败：BoxFnOnce -> BoxFuture\<()\> |
| ctx.get(name) | ctx.get::\<T\>(name)? | 所属 fiber 的类型化存储快照 |
| require without inject | Error: cannot get property ... | 同样的护栏报错 |
| app.scope(label) | ctx.isolate(label) | 按作用域做重复检测与可见性 |
| config layers | ctx.intercept(service, json) | 由外向内合并，内层冲突取胜 |
| events emit/parallel/bail/waterfall | 同样四种模式 | bail = 返回非 null / 非 false |
| Next veto handle | next.run(payload).await | 不调用即否决，覆盖内建行为 |
| internal events | internal/{status,plugin,service,config,update} | 服务/配置按作用域过滤 |
| accessor(name, {get}) | ctx.accessor::\<T\>(name, closure).await | 派生服务每次读取重算，绝不缓存 |
| fiber.getEffects() | fiber.effect_metas() | 活动 effect 标签，按注册序 |
| ctx.is(other) | ctx.is(&other) | 作用域同一性 |

刻意的偏差：provide / on 是异步的（无锁快照）；disposer 不允许失败；
epoch 是显式字符串（`:` 前缀 + 每个已满足依赖追加 `:uid`），而非隐藏指纹。
JS 的 mixin 与 trace/bind 代理在 Rust 中没有直接对应：上下文扩展以
Context 上的扩展 trait 表达（见 `TimerContextExt`），静态、类型化、零开销。

## v0.2 起引入的生产级加固（P1/P2）

在忠实移植之上，内核同一版本携带了一批生产级加固，全部由工作区测试
验证：

- **锁无关热路径。** `FiberState` 被镜像进原子缓存——`state()` 不再串联
  四把生命周期 Mutex；根级 ACTIVE 位图让 `fiber_is_active` 无需触碰 fiber
  注册表即可回答，因此每次类型化服务读取都保持轻锁。完整的锁序
  （`reflect < registry < fibers < fiber 内部锁`）已在各模块文档中说明。
- **有界等待。** `FiberHandle::join_with_timeout()` 让永不结束的 `apply()`
  从"卡死 join/dispose"变为"超时返回错误"；`ctx.parallel_timeout()` 对挂死
  的监听器同样生效，返回稳定的 `CordisCode::Timeout`。
- **零 spawn 事件热路径。** `ctx.on_sync` + `ctx.emit_sync` 在派发任务上
  内联等待同步槽监听器（事件不再每次 tokio spawn）——针对每秒 token 级的
  高频流（如逐 token 增量）；普通 `emit` 保持 fire-and-forget 语义。
- **结构化日志。** `Logger::log_event(level, event, code, message)` 配合
  稳定分类器 `Error::code()`（`CordisCode` / `validation` / `aggregate`），
  默认折叠进单一 sink；可被更丰富的实现覆盖。

## 目录结构

    crates/cordis/src/
      context.rs   Context、作用域、根 fiber、类型化 accessor
      fiber.rs     生命周期驱动：epoch、load/unload、effects、restart/update
      registry.rs  插件标识（Arc 指针键）、注册簿记
      events.rs    emit / parallel / serial(bail) / waterfall(+Next)、同步槽
      service.rs   provide/set/notify、严格解析、intercept 合并
      plugin.rs    trait Plugin + FnPlugin 适配 + 注入列表
      error.rs     CordisCode、ValidationError、聚合错误

    crates/cordis/examples/mini_dsh.rs   仅用插件组装的小型 harness
    crates/cordis/tests/
      reactive.rs 依赖图收敛场景
      events.rs   分发模式语义、同步槽、有界 parallel
      accessor.rs 派生服务、热路径稳定性
      isolate.rs  隔离与 intercept 分层

    crates/cordis-plugin-timer/  参考生态插件：
      timeout()/interval() 作为 fiber 拥有的 effect（卸载时取消）

    crates/plugin-contract/  动态插件的零依赖 C-ABI 面
    crates/greeter-plugin/   运行时由 dynhost 加载的 cdylib 插件
    crates/dynhost/          dlopen 宿主：把导出适配为 trait Plugin

## 试一试

    cd crates/cordis
    cargo run --example mini_dsh      # 组装 -> 收敛 -> 热更新 -> 停止
    cargo test --workspace            # 38 个测试，全部通过
    cargo run -p dynhost              # 运行时 dlopen 一个 .dylib/.so 插件
    cargo add cordis-rust             # 或直接通过 crates.io 使用

示例在数据库出现**之前**就注册 API 服务，观察依赖图逐步补齐、热切换
配置，最后按 LIFO 顺序关闭——全程无手工连线。

## 被使用方

- **dsh-rs**（`github.com/redoop/dsh-rs`）——接口隔离的 agent 宿主机；
  依赖 `cordis = { version = "0.2", package = "cordis-rust" }`。