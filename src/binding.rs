// 设计：tracing-coordinator
//! 协调操作的准入、一次记录器绑定与关闭边界。
//!
//! coordinator拥有唯一CoordinationBinding；client和service只持受限弱句柄。
//! 本模块只拥有私有状态和可选记录器，不准备或保存Fory/codec资源。
//! 原始状态、锁、记录器和弱引用升级权限不跨模块开放。
//!
//! 目标方法同步、无await；状态锁可能阻塞，无公平或固定等待保证。
//! 锁内仅状态裁决/移动和必要记录器克隆；外调、日志及析构均锁外。
//! 不引入unsafe、FFI、任务、线程或在途队列。
//!
//! 十个手写实现入口已完成；行为测试与独立源码审查共同保护内部边界。
//! 实施验收：[状态/释放及证据](../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-binding)。
//! 正式条目：
//! [接口](../design/tracing-coordinator/07-design/api.md#api-binding-typed)、
//! [表示](../design/tracing-coordinator/07-design/data.md#data-binding-typed)、
//! [时序](../design/tracing-coordinator/07-design/call-chain.md#chain-binding-typed)、
//! [验证](../design/tracing-coordinator/07-design/errors.md#errors-binding-typed)。

use crate::protocol::DecodedSelection;
use std::{
    error::Error,
    fmt,
    sync::{Arc, Mutex, Weak},
};

#[path = "binding_inner.rs"]
mod binding_inner;

/// 某个协调器的唯一生命周期所有者，库内可见，不实现Clone。
///
/// inner强持独立绑定状态；操作内部可以临时取得强引用，
/// 但所有者Drop必须主动关闭，不能等待最后一个临时Arc消失才关闭。
/// 由coordinator拥有，不直接作为宿主独立生命周期对象或notifier持有物。
/// 所有字段私有，不导出锁、记录器、状态或关闭后门。
/// 构造、操作及主动关闭已实现，行为入口为tests/binding/binding_test.rs。
pub(crate) struct CoordinationBinding {
    inner: Arc<BindingInner>,
}

/// 只提供发送准入及接收交接的库内弱句柄。
///
/// 仅Weak，不保存所有者、终端或记录器；克隆不延长所有者生命周期。
/// 不开放upgrade、内部状态、锁或关闭权限。
/// 弱引用失效，或升级后观察到Closed，均按关闭拒绝新操作。
pub(crate) struct CoordinationHandle {
    inner: Weak<BindingInner>,
}

/// binding唯一维护的私有权威状态容器。
///
/// 同一把锁裁决绑定、准入与关闭；外部不能自行拼接多步状态变化。
/// 不保存协议、codec、终端或宿主runtime。
struct BindingInner {
    state: Mutex<BindingState>,
}

/// 私有绑定状态；关闭为终态，中毒只允许终态清理。
enum BindingState {
    /// 尚未消耗绑定机会；发送可准入，接收拒绝。
    OpenUnbound,
    /// 绑定机会已消耗，None同样不能再替换为Some。
    OpenBound(Option<pi_logger::FlightRecorder>),
    /// 不再保存本库原记录器句柄；先准入调用可能仍有临时副本。
    Closed,
}

/// 库内绑定语义错误，不保存PoisonError、锁守卫或输入消息。
///
/// Debug为派生实现；Display表达五类语义，source不暴露内部守卫。
/// source全部为None；client保存直接原因，service按阶段尝试拒绝日志。
#[derive(Debug)]
pub(crate) enum BindingError {
    /// 所有者已消失，或状态已关闭。
    Closed,
    /// 一次绑定机会已被Some或None消耗。
    RecorderAlreadyBound,
    /// 尚未绑定，或已明确绑定None，当前无法接收上报。
    RecorderUnavailable,
    /// 记录器返回false，不能由此推断唯一内部原因。
    EventNotPublished,
    /// 观察到状态锁中毒，已按终态清理政策关闭。
    StatePoisoned,
}

impl CoordinationBinding {
    /// 创建独立OpenUnbound所有者，不接收协议资源。
    ///
    /// @internal
    /// 无输入；返回调用方拥有的绑定对象，目标O(1)状态构造及一次Arc分配。
    /// 不安装记录器、准备codec、注册服务或创建线程，无普通错误返回。
    /// 重复构造产生独立对象，不代表可重复安装同一逻辑服务。
    /// 分配终止等异常不保证返回；验证见独立对象及弱生命周期场景。
    pub(crate) fn new() -> Self {
        binding_inner::new()
    }

    /// 为client或service创建不透明弱接入。
    ///
    /// 只借用self，本次O(1)建立弱引用；不升级强引用、不取得状态锁。
    /// 可重复调用，不消耗绑定机会；不授予关闭或原始状态访问权。
    /// 不检查是否开放，后续每次语义操作自行准入。
    /// 验证见弱句柄、关闭及绑定机会场景。
    pub(crate) fn handle(&self) -> CoordinationHandle {
        binding_inner::handle(self)
    }

    /// 一次性接管宿主提供的可选记录器句柄。
    ///
    /// recorder为Some时保存共享FlightRecorder，不接管FlightRecorderGuard；
    /// None也消耗唯一绑定机会。输入不异步借用，成功移入状态。
    /// 健康锁下Closed优先于重复绑定；重复绑定保持原值并返回RecorderAlreadyBound。
    /// 失败的新输入在锁外释放，不返还、不替换已有记录器。
    ///
    /// 观察到锁中毒则强制Closed，移出旧值后锁外释放旧/新句柄，
    /// 返回StatePoisoned，不改报Closed、不clear_poison或恢复。
    /// 本库状态操作O(1)，锁可能阻塞；析构成本另计，不在锁内调用记录器或日志。
    /// 非幂等：第一次Some/None成功后不能再次绑定；close后不能重开。
    /// 验证见Some/None一次绑定场景；锁外释放另经独立源码审查。
    pub(crate) fn bind_recorder(
        &self,
        recorder: Option<pi_logger::FlightRecorder>,
    ) -> Result<(), BindingError> {
        binding_inner::bind_recorder(self, recorder)
    }

    /// 停止新准入并解除本库保存的记录器持有，幂等且不可重开。
    ///
    /// 同锁把状态转Closed，移出旧记录器，释放锁后再析构。
    /// 已中毒时只取守卫作终态清理，不清除中毒、不恢复开放。
    /// 不等待已准入调用：它们可在本方法返回后继续既已获准的提交或事件发布。
    /// 不撤回消息、不注销静态服务、不关闭宿主终端/运行时/线程/记录器守卫，
    /// client及宏params持有的codec按各自共享所有权释放，本方法不清理依赖TLS。
    ///
    /// 本库状态转换O(1)，锁等待及依赖析构成本另计；没有普通错误返回。
    /// 目标不主动制造panic，但不能把依赖析构panic或分配终止伪装成正常返回。
    /// 验证见关闭/有界竞争及Owner析构场景；不保证任意宿主析构无panic。
    pub(crate) fn close(&self) {
        binding_inner::close(self)
    }
}

impl CoordinationHandle {
    /// 为本次发送检查绑定仍开放，仅返回准入结果。
    ///
    /// @internal
    /// self只借用；临时升级Weak，失效返回Closed。
    /// 健康锁下OpenUnbound/OpenBound均Ok(())，无需记录器；Closed拒绝。
    /// 中毒仅终态清理，解锁后释放旧值并返回StatePoisoned。
    ///
    /// 返回前释放锁和临时强引用，不交资源、token或撤销凭据，不建在途计数。
    /// 准入先于close时，本次后续发送可在close返回后继续，不加第二次检查。
    /// 失败零发送；成功不证明编码、提交或远端完成，不承诺跨调用一次性。
    ///
    /// 状态工作O(1)，锁等待/引用计数/析构成本另计，无固定等待保证。
    /// 不编码、提交或调用记录器；可重复调用，每次分别裁决，无await。
    /// 验证见开放状态准入与关闭交错场景，内部释放由独立源码审查补足。
    pub(crate) fn admit_send(&self) -> Result<(), BindingError> {
        binding_inner::admit_send(self)
    }

    /// 接管protocol已校验的结果，并在准入后向记录器发布一次。
    ///
    /// message成功和失败均被消费，失败不返还输入。
    /// 弱失效/Closed返回Closed；未绑定或None返回RecorderUnavailable；
    /// 中毒按终态清理后返回StatePoisoned，拒绝时不调用记录器。
    /// 只有开放且Some才在短锁内克隆记录器，释放锁后消费into_parts，
    /// 再调用一次apply_remote_selection；true返回Ok，false返回EventNotPublished。
    ///
    /// Ok仅表示本地事件已发布，不保证选择、传播或导出完成；
    /// false不推断唯一原因，不重试、不调用record_peer或重复协议校验。
    /// 所有输入、临时引用及记录器副本均锁外释放。
    /// 先准入调用可在close返回后继续发布，不提供关闭后的副作用屏障。
    ///
    /// 本库准入状态操作O(1)，记录器处理、输入释放和同步等待成本另计；
    /// 不跨await持锁，不读取now或复制记录器时效/去重策略。
    /// 验证见真实记录器交接及关闭场景；精确调用/锁外消费另经独立源码审查。
    pub(crate) fn submit_received(
        &self,
        message: DecodedSelection,
    ) -> Result<(), BindingError> {
        binding_inner::submit_received(self, message)
    }
}

impl Clone for CoordinationHandle {
    /// 目标只克隆Weak，O(1)，不升级、不取状态锁或延长所有者生命期。
    /// 返回新的弱句柄；无绑定/关闭/记录器副作用，由弱生命周期场景保护。
    fn clone(&self) -> Self {
        binding_inner::clone_handle(self)
    }
}

impl Drop for CoordinationBinding {
    /// 目标在所有者析构时主动执行close政策，不等待最后临时Arc释放。
    /// 记录器移出后锁外析构；不关闭共享宿主资源，依赖析构异常不被捕获。
    fn drop(&mut self) {
        binding_inner::drop_binding(self)
    }
}

impl fmt::Display for BindingError {
    /// 目标写出真实语义分类，传播fmt::Error，不改变状态或保存输入。
    /// 输出长度固定，O(1)本库工作；验证见error_display两场景。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        binding_inner::fmt_binding_error(self, f)
    }
}

impl Error for BindingError {
    /// 目标全部返回None，不保存或暴露底层守卫；O(1)，无分配或状态变化。
    /// 验证见error_source场景，五分类重复查询均为None。
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        binding_inner::binding_error_source(self)
    }
}
