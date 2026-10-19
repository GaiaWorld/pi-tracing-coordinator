// 设计：tracing-coordinator
//! 上报通知的标准宏类型化静态接收适配。
//!
//! 两个具体codec适配共用业务处理；宿主每进程只装配一种。
//! 宏负责前置解码，本模块负责业务校验后的交接及拒绝日志。
//! 不拥有client、终端、runtime、记录器或binding生命周期。
//!
//! 四个手写实现入口及两种宏适配均已验证并通过独立审查。
//! 实施验收：[接收/宏装配及证据](../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-service)。
//! 正式条目：
//! [接口](../design/tracing-coordinator/07-design/api.md#api-service-typed)、
//! [表示](../design/tracing-coordinator/07-design/data.md#data-service-typed)、
//! [图源](../design/tracing-coordinator/07-design/call-chain.md#chain-service-typed)、
//! [验证](../design/tracing-coordinator/07-design/errors.md#errors-service-typed)。

use crate::{
    binding::CoordinationHandle,
    protocol::SelectionMessage,
};
use pi_service::{DefaultCodec, MultiLayerCodec, Service};

#[path = "service_inner.rs"]
mod service_inner;

/// 本库静态服务的layer，供宿主inventory过滤及冷路径选层。
///
/// @public
/// 不是网络地址、Fory类型号或端口隔离域；两个适配仍共用4091。
/// 宿主只装配一种，使用期保持相关类型表/层配置稳定。
pub const COORDINATION_SERVICE_LAYER: &str = "tracing-coordinator";

/// 宏只读上下文；仅持有受限弱binding，全部字段私有。
///
/// 外层Arc保存并注入ServiceContexts，生成params强持此Arc和codec。
/// 本类型无Clone/Default/Debug、getter或关闭出口，不保存client、
/// 终端、runtime或记录器；上下文存活不永久保活binding。
///
/// 关闭后旧params可存活，接收经binding拒绝；关闭不注销静态服务、
/// 清params codec或依赖TLS。注入/重复安装及失败收尾归coordinator。
pub(crate) struct CoordinationServiceContext {
    binding: CoordinationHandle,
}

impl CoordinationServiceContext {
    /// 接管弱binding并组合上下文，不发布到宿主容器。
    ///
    /// @internal
    /// binding按值接管；不升级、取锁或检查开放状态。
    /// 返回调用方拥有值，目标O(1)，外层Arc分配另计。
    /// 无普通错误，重复构造不表示重复安装合法。
    /// 验证见tests/service/service_test.rs中的上下文/静态出口场景。
    pub(crate) fn new(binding: CoordinationHandle) -> Self {
        service_inner::new_context(binding)
    }
}

/// 取得DefaultCodec适配的同一静态Service借用。
///
/// @internal
/// 无输入；返回值有效至进程结束，不转移生命周期所有权。
/// 目标O(1)，无分配、锁、日志、注册或初始化，可重复取得。
/// 不暴露生成params或codec槽；宿主每进程只装配一种适配。
/// 验证见tests/service/service_test.rs中的上下文/静态出口场景。
pub(crate) fn default_service() -> &'static dyn Service<Codec = DefaultCodec> {
    service_inner::default_service()
}

/// 取得MultiLayerCodec适配的同一静态Service借用。
///
/// @internal
/// 输入、结果、成本及限制同default_service；不是新增逻辑服务。
/// 无分配、锁、注册或初始化；由MultiLayer静态出口场景保护。
pub(crate) fn multi_layer_service() -> &'static dyn Service<Codec = MultiLayerCodec> {
    service_inner::multi_layer_service()
}

/// DefaultCodec的私有typed业务入口；只委托共同处理芯。
///
/// context仅借本次，宏params强持对应Arc；message按值消费。
/// 宏在Future被框架poll时先default_decode，失败不调用本函数。
/// 本函数同步，无新Future/线程/任务，返回()不产生远端应答。
///
/// 共同处理按protocol校验→binding交接顺序；非法消息零记录器调用。
/// 校验失败尝试stage=validate警告，交接拒绝/未发布尝试stage=submit。
/// 日志target=pi_tracing_coordinator::service，字段service_port=4091、
/// stage和error=%实际错误；不记原消息/source/Trace/操作号，成功无日志。
///
/// binding返回后才日志，其锁和临时资源已释放；记录受宿主过滤控制，
/// 格式化/subscriber/依赖/析构可阻塞、重入或panic，不捕获为普通拒绝。
/// 成功仅本地事件发布，无重试/补偿/输入保存/record_peer调用。
///
/// 目标O(n)校验+O(1)准入状态工作；decode/TLS、锁等待、记录器、日志
/// 及析构另计。先准入可在close后收尾，不保证任意异常Fory输入安全。
/// 验证见两适配真实registry/poll、拒绝日志和记录器交接场景。
#[pi_service::pi_service(
    id = "/observability/tracing/coordinator",
    port = 4091,
    layer = "tracing-coordinator",
    mode = send,
    version = "0.1.0",
    codec = pi_service::DefaultCodec
)]
fn receive_selection_default(
    context: &CoordinationServiceContext,
    message: SelectionMessage,
) {
    service_inner::receive_selection(context, message)
}

/// MultiLayerCodec的私有typed入口，共同合同逐字适用上一入口。
///
/// 只替换具体codec，仍同一业务/端口/layer/version及共同处理芯；
/// 两个inventory条目不自动跨表互斥，宿主每进程只装配一种。
/// 所有权、失败日志、同步执行、成本和验证边界同上一入口。
#[pi_service::pi_service(
    id = "/observability/tracing/coordinator",
    port = 4091,
    layer = "tracing-coordinator",
    mode = send,
    version = "0.1.0",
    codec = pi_service::MultiLayerCodec
)]
fn receive_selection_multi_layer(
    context: &CoordinationServiceContext,
    message: SelectionMessage,
) {
    service_inner::receive_selection(context, message)
}
