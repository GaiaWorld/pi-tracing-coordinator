// 设计：tracing-coordinator
//! 宿主统一协调入口：组合发送、接收上下文及唯一binding所有者。
//!
//! 类型表登记发布、通信环境和logger初始化归宿主；本模块冷路径
//! 预绑定codec并提供两种静态安装接入，不创建runtime或后台任务。
//! 正式条目：
//! [接口](../design/tracing-coordinator/07-design/api.md#api-coordinator)、
//! [表示](../design/tracing-coordinator/07-design/data.md#data-coordinator)、
//! [图源](../design/tracing-coordinator/07-design/call-chain.md#chain-coordinator)、
//! [验证](../design/tracing-coordinator/07-design/errors.md#errors-coordinator)。
//!
//! 十个手写实现入口均已完成；构造/安装/发送/关闭已有分块行为证据及独立审查。
//! 实施验收：[公开入口矩阵](../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-coordinator)。

use crate::{
    binding::{BindingError, CoordinationBinding},
    client::{CoordinationClient, CoordinationSendError},
    service::CoordinationServiceContext,
};
use pi_logger::{FlightRecorder, TraceNodeId, TraceNotifier, TraceSelection};
use pi_service::{
    DefaultCodec, MultiLayerCodec, Service, ServiceCodec,
    ServiceRuntime, ServiceTerminal,
};
use std::{error::Error, fmt, ops::Deref, sync::Arc};

#[path = "coordinator_inner.rs"]
mod coordinator_inner;

/// 宿主唯一协调生命周期对象，字段私有，无Clone/Default/Debug/getter。
///
/// @public
/// binding唯一拥有准入/一次记录器绑定/关闭状态；client共享发送接入，
/// context供宏绑定，service保存对应具体codec的静态适配。
/// 固定字段顺序使自动析构先关闭binding，再释放client/context；
/// 不另写Drop，也不等待在途。存活notifier只持client，不保活binding。
///
/// 不保存runtime、终端强引用或FlightRecorderGuard，不接管共享设施。
/// 宿主须保证runtime/终端匹配，后续安装使用构造时同一runtime；
/// 相同泛型不证明配对。每进程只装配一种codec对应服务，端口全局唯一。
///
/// 宿主先完成类型表登记、发布和真实表核对，再构造、取得notifier
/// 注入logger、一次绑定Some/None、安装接收服务。构造成功不证明
/// 记录器就绪或服务可达；未绑定可发送，接收拒绝。
///
/// 无新增线程、任务、队列或全局安装锁；依赖执行可阻塞/重入/panic。
/// 所有方法及binding主动析构已实现；完整部署/性能结论限验收矩阵。
pub struct TracingCoordinator<T, C = DefaultCodec>
where
    C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>,
    T: Deref<Target = ServiceTerminal<C>> + Send + Sync + 'static,
{
    binding: CoordinationBinding,
    client: Arc<CoordinationClient<T, C>>,
    context: Arc<CoordinationServiceContext>,
    service: &'static dyn Service<Codec = C>,
}

/// 公开不透明的构造/绑定/安装错误，无公开构造、Kind/getter或Clone。
///
/// 不额外保存参数、runtime、context、codec或终端；依赖错误自身
/// 可能带原因文本。Debug为派生，Display/source保留真实分类及直接原因。
#[derive(Debug)]
pub struct CoordinationError {
    kind: CoordinationErrorKind,
}

/// 私有阶段原因，不开放内部状态或资源能力。
#[derive(Debug)]
enum CoordinationErrorKind {
    Binding(BindingError),
    Service(pi_service::ServiceError),
    CodecLayerMissing,
    ContextAlreadyInstalled,
}

impl<T> TracingCoordinator<T, DefaultCodec>
where
    T: Deref<Target = ServiceTerminal<DefaultCodec>> + Send + Sync + 'static,
{
    /// 从宿主默认codec构造独立协调器，不发布上下文或服务。
    ///
    /// terminal只借用于建立弱引用，runtime只借用于冷路径预绑定，
    /// local按值移入client，空值实际发送时校验。宿主使用期强持终端。
    /// 取得codec_for_layer及对应静态服务后组合binding/client/context；
    /// 不登记类型、绑定记录器、注册/init服务或创建任务。
    ///
    /// 宿主独占候选Fory先完成所有集中登记，成功后发布DefaultCodec；
    /// 以default_codec真实快照核对已登记Arc，使用期保持配置稳定。
    /// 返回成功由调用方拥有，不证明登记、通信或记录器已就绪。
    ///
    /// 普通失败保留ServiceError，不发布装配，局部codec持有释放。
    /// 本库固定对象组合O(1)，三处必要Arc分配；依赖克隆/选层、
    /// 同步等待及析构另计，不声称整个构造固定O(1)或零分配。
    /// 同步无await；不捕获依赖panic为Err。验证见默认懒构造场景。
    pub fn new_default(
        terminal: &Arc<T>,
        runtime: &ServiceRuntime<DefaultCodec>,
        local: TraceNodeId,
    ) -> Result<Self, CoordinationError> {
        coordinator_inner::new_default(terminal, runtime, local)
    }
}

impl<T> TracingCoordinator<T, MultiLayerCodec>
where
    T: Deref<Target = ServiceTerminal<MultiLayerCodec>> + Send + Sync + 'static,
{
    /// 从宿主明确目标层构造，缺层拒绝，不接受default隐式回退。
    ///
    /// 参数所有权、组合/失败/成本/线程合同同new_default。
    /// 先has_layer(COORDINATION_SERVICE_LAYER)，缺失CodecLayerMissing；
    /// 再codec_for_layer及对应静态服务，失败保留实际ServiceError。
    ///
    /// 宿主先登记候选Fory并安装明确目标层，以has_layer/layer_fory
    /// 核对真实已登记Arc；current_layer字符串或选层Ok单独不足。
    /// 构造和宏init都在稳定配置下完成，已绑定快照不自动跟随替换。
    /// 两端实例可不同但表示/编号/配置一致，不做热路径表检查。
    /// 验证见tests/coordinator_test.rs中对应构造、委托或生命周期场景。
    pub fn new_multi_layer(
        terminal: &Arc<T>,
        runtime: &ServiceRuntime<MultiLayerCodec>,
        local: TraceNodeId,
    ) -> Result<Self, CoordinationError> {
        coordinator_inner::new_multi_layer(terminal, runtime, local)
    }
}

impl<T, C> TracingCoordinator<T, C>
where
    C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>,
    T: Deref<Target = ServiceTerminal<C>> + Send + Sync + 'static,
{
    /// 返回共享client的既有TraceNotifier trait对象。
    ///
    /// self只借本次，返回拥有式Arc，可跨线程/调用共享；目标O(1)
    /// 复制引用计数，无新分配/任务/注册或准入检查。重复调用共享client。
    /// 关闭后仍可取得，实际notify经binding拒绝；不保活binding/终端，
    /// 可延长codec持有，不保证最后Arc释放即清依赖TLS。
    /// 验证见tests/coordinator_test.rs中对应构造、委托或生命周期场景。
    pub fn notifier(&self) -> Arc<dyn TraceNotifier> {
        coordinator_inner::notifier(self)
    }

    /// 一次绑定宿主可选记录器；None同样消耗机会。
    ///
    /// recorder按值交binding，成功移入、失败锁外释放，不返还输入。
    /// 不接管Guard或shutdown；重复/关闭/中毒沿binding合同，
    /// 保存实际BindingError。本库状态工作O(1)，锁等待/析构另计。
    /// 非幂等，不改旧值、不重开；同步无await，由Some/None及关闭场景保护。
    pub fn bind_recorder(
        &self,
        recorder: Option<FlightRecorder>,
    ) -> Result<(), CoordinationError> {
        coordinator_inner::bind_recorder(self, recorder)
    }

    /// 向构造时同一runtime发布本次接收context，供宿主inventory流程使用。
    ///
    /// runtime仅借本次，不保存；先检查binding开放，再查同类型context。
    /// 已有即ContextAlreadyInstalled，即使同Arc也不幂等；保持旧值/服务。
    /// 不存在才插入本次Arc，不自行register/init；关闭错误不发布，
    /// 中毒遵守binding终态清理，返回实际BindingError。
    ///
    /// 宿主必须串行化同逻辑服务的安装、容器变更和关闭；先查再插
    /// 不提供对抗任意并发替换的原子保证，不新增安装锁/状态机。
    /// 依赖容器可分配/同步等待，成本按实际表规模，非纯函数、无await。
    /// 已发布值不自动移除，失败收尾见install_service；验证见预占/关闭场景。
    pub fn bind_service_context(
        &self,
        runtime: &ServiceRuntime<C>,
    ) -> Result<(), CoordinationError> {
        coordinator_inner::bind_service_context(self, runtime)
    }

    /// 显式发布context、注册对应静态服务并初始化4091。
    ///
    /// runtime必须与构造时同一环境，只借本次；依次调用本次
    /// bind_service_context、registry.register、registry.init，首错返回，
    /// ServiceError原样保存。与宿主inventory方式择一，不重复注册。
    ///
    /// 非事务：context发布后注册冲突保留context及旧服务；
    /// bind_params失败保留框架旧状态，Service::init失败保留Failed。
    /// 成功只证明本runtime初始化，不证记录器就绪、远端可达或所有表唯一。
    /// 宿主立即处理装配失败并close/丢弃本候选，不盲重试/覆盖旧值；
    /// 无静态注销/按owner删context出口，重装使用新的合法环境。
    ///
    /// 串行安装/容器变更/关闭；依赖register/init可能持锁、复制表、
    /// 同步调用监听器，监听器不得重入同一注册/init过程。
    /// 不持binding锁进入registry；依赖成本/阻塞/析构另计，无await。
    /// 不自动回滚、换号或关闭共享设施；验证见显式/inventory及部分失败场景。
    pub fn install_service(
        &self,
        runtime: &ServiceRuntime<C>,
    ) -> Result<(), CoordinationError> {
        coordinator_inner::install_service(self, runtime)
    }

    /// 向指定remote发送一次本跳上报通知，原样委托client。
    ///
    /// remote/source只借本次，合法域/错误顺序及成本沿client合同：
    /// remote为PeerId Base58，source非空UTF-8、真实性归宿主，
    /// selection按值保真及100ns转换；不以保存local覆盖本次source。
    /// 前置失败零提交，进入send恰一次；Ok只表示本地受理，
    /// 不重试、保存待发或额外日志。关闭先行拒绝，已准入可收尾。
    /// 同步无await，锁外编码/提交/析构；外部执行成本另计。
    /// 验证见tests/coordinator_test.rs中对应构造、委托或生命周期场景。
    pub fn send_selection(
        &self,
        remote: &TraceNodeId,
        selection: TraceSelection,
        source: &TraceNodeId,
    ) -> Result<(), CoordinationSendError> {
        coordinator_inner::send_selection(self, remote, selection, source)
    }

    /// 停止本库新准入，委托binding幂等关闭，不等待在途。
    ///
    /// self只借本次，无普通返回错误，不重开/撤回消息/注销服务，
    /// 不清宿主context、params codec或TLS，不停终端/runtime/Guard。
    /// 状态工作O(1)，锁等待/析构另计；与安装按宿主规则串行。
    /// 不捕获依赖panic；验证见关闭、自动析构和共享环境保护场景。
    pub fn close(&self) {
        coordinator_inner::close(self)
    }
}

impl fmt::Display for CoordinationError {
    /// 写真实分类和直接原因，传播fmt错误，不日志或改变状态。
    /// 成本随输出及依赖格式化；验证见四分类及逐容量写入失败场景。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        coordinator_inner::fmt_coordination_error(self, f)
    }
}

impl Error for CoordinationError {
    /// 借用Binding/Service直接原因，其余None；借用受self约束。
    /// O(1)、无分配/格式化/状态变化；验证见实际直接原因同地址借用场景。
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        coordinator_inner::coordination_error_source(self)
    }
}
