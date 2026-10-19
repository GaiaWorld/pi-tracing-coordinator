// 设计：tracing-coordinator
//! 跨进程Trace上报通知协调库。
//!
//! 宿主负责关联、目标选择及共享通信环境；本库负责指定通知的接入。
//! protocol集中登记和业务校验，binding管理准入/记录器/关闭。
//! client复用宿主预绑定codec，service使用标准宏，coordinator统一装配与关闭。
//! 五模块37个手写实现入口均已完成，并通过各切片的独立测试及功能审查。
//! 当前按仅WSL、排除集成/压力/基准的范围收尾，不推定完整部署矩阵已验收。
//! 实施验收：[矩阵与证据](../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-matrix)。
//!
//! 正式入口：[设计索引](../design/tracing-coordinator/00-index.md)。

mod binding;
mod client;
mod coordinator;
mod protocol;
mod service;

pub use client::CoordinationSendError;
pub use coordinator::{CoordinationError, TracingCoordinator};
pub use protocol::register_fory_types;
pub use service::COORDINATION_SERVICE_LAYER;

/// 唯一静态send服务的逻辑端口，供收发及宿主装配共用。
///
/// 4091是服务路由编号，不是OS端口或Fory类型号。
/// 宿主须保证进程内全局唯一；layer/id/version不同不能豁免冲突。
/// 冲突不覆盖、跳过或自动换号；声明不表示已注册、初始化或可达。
///
/// 正式条目：
/// [服务设计](../design/tracing-coordinator/07-design/api.md#api-service-typed)。
pub const COORDINATION_SERVICE_PORT: u16 = 4091;
