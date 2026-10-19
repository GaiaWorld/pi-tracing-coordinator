// 设计：tracing-coordinator
//! 指定目标的一次上报发送及既有TraceNotifier接入。
//!
//! 外层协调器共享客户端；本模块弱持宿主终端和binding，
//! 强持冷路径已预绑定的宿主codec，不拥有记录器或绑定权威状态。
//! 显式发送与notifier共用路径，不维护关联、去重或待发状态。
//!
//! 方法同步、无await，不新增线程、任务、队列或状态锁。
//! binding仅在准入时短暂持锁；此后转换、编码、终端、日志及析构锁外。
//! T::Deref、codec、宿主传输、subscriber及析构是实际外部执行，
//! 不据同步签名保证全路径无锁、零等待、无重入或无panic。
//!
//! 五个手写实现入口已完成，发送、错误及资源边界已有分块验证和独立审查。
//! 实施验收：[发送/通知及证据](../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-client)。
//! 正式条目：
//! [接口](../design/tracing-coordinator/07-design/api.md#api-client-typed)、
//! [表示](../design/tracing-coordinator/07-design/data.md#data-client-typed)、
//! [图源](../design/tracing-coordinator/07-design/call-chain.md#chain-client-typed)、
//! [验证](../design/tracing-coordinator/07-design/errors.md#errors-client-typed)。

use crate::{
    binding::{BindingError, CoordinationHandle},
    protocol::ProtocolError,
};
use pi_logger::{TraceNodeId, TraceNotifier, TraceSelection};
use pi_service::{ServiceCodec, ServiceTerminal};
use std::{
    error::Error,
    fmt,
    ops::Deref,
    sync::{Arc, Weak},
};

#[path = "client_inner.rs"]
mod client_inner;

/// 库内唯一发送接入，由外层以Arc共享，所有字段私有。
///
/// terminal弱持宿主现有Arc，binding提供受限弱准入；
/// codec强持已选好层的Arc<C>，local保存notify使用的本进程身份。
/// 无Clone、Drop、Debug或资源getter，不保存runtime、记录器或外层所有者。
///
/// notifier可延长client/codec，不永久保活terminal/binding。
/// 最后client引用释放自身codec持有；其它宿主、宏params及Fory TLS
/// 可能继续存在，binding.close不清理它们。内置codec的已核字段
/// 不形成终端/runtime反向强边，不推广为任意C或整个宿主无环。
///
/// 宿主先登记、发布、预绑定并保持使用期配置一致；DefaultCodec共享
/// 可替换槽，MultiLayerCodec已绑定快照不自动跟随层替换。
/// 每通知只借用codec，不克隆、切层或检查登记表；环境配对由宿主保证。
///
/// C的输入/输出均为Vec<u8>，本模块不要求C:Clone；
/// T须已有Deref到ServiceTerminal<C>的出口，直接终端作T不满足约束。
/// 允许共享线程调用，不额外串行化；外部执行成本与重入另计。
pub(crate) struct CoordinationClient<T, C>
where
    C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>,
    T: Deref<Target = ServiceTerminal<C>> + Send + Sync + 'static,
{
    terminal: Weak<T>,
    codec: Arc<C>,
    binding: CoordinationHandle,
    local: TraceNodeId,
}

/// 公开不透明的发送错误，不提供公开构造、Kind入口、可写字段或Clone。
///
/// kind保存真实阶段及依赖原因，不额外保存原始调用参数或资源句柄；
/// 原样保留的ServiceError自身仍可能携带host、port或原因字符串。
/// 不把本地提交错误扩写为远端处理结果，也不加入TraceId或操作编号。
///
/// Debug为派生实现；Display和source保留真实发送阶段及直接原因。
#[derive(Debug)]
pub struct CoordinationSendError {
    kind: CoordinationSendErrorKind,
}

/// 客户端私有错误表示，不作为公开匹配或状态访问出口。
#[derive(Debug)]
enum CoordinationSendErrorKind {
    /// 发送准入被拒绝，保留实际绑定原因。
    Binding(BindingError),
    /// 宿主终端弱引用无法升级，不推断失效的唯一原因。
    TerminalUnavailable,
    /// anyhow接管原始PeerId解析错误，不先字符串化原因。
    InvalidTarget(anyhow::Error),
    /// 业务转换失败，保留实际协议错误。
    Protocol(ProtocolError),
    /// 宿主codec编码失败，保留实际返回的ServiceError。
    Encode(pi_service::ServiceError),
    /// 唯一一次终端提交返回失败，保留实际服务错误。
    Submit(pi_service::ServiceError),
}

impl<T, C> CoordinationClient<T, C>
where
    C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>,
    T: Deref<Target = ServiceTerminal<C>> + Send + Sync + 'static,
{
    /// 组合本次协调器的发送接入，不检查通信、登记或身份就绪。
    ///
    /// @internal
    /// terminal仅借用于建立Weak，不保存借用或常驻强引用；
    /// codec按值接管已选好层的Arc，binding接管同一协调器弱接入，
    /// local按值保存原身份，不裁剪、规范化或验证空串。
    /// 空local在实际发送的source校验阶段拒绝。
    ///
    /// 仅组合对象，不升级终端、取锁、验证、登记、切层或探测服务。
    /// 返回调用方拥有的客户端，外层Arc共享/分配另计。
    /// 目标O(1)，构造不另建堆对象，Weak计数成本另计；
    /// 无普通错误返回，重复构造独立，不代表幂等安装。
    /// 依赖panic或分配终止不伪装普通结果；由懒构造及资源生命周期场景保护。
    pub(crate) fn new(
        terminal: &Arc<T>,
        codec: Arc<C>,
        binding: CoordinationHandle,
        local: TraceNodeId,
    ) -> Self {
        client_inner::new(terminal, codec, binding, local)
    }

    /// 向宿主指定的目标提交本次上报通知，至多一次终端调用。
    ///
    /// # 参数与合法域
    /// remote为现有PeerId::FromStr接受的Base58目标进程文本；
    /// source为本跳直接发送身份，由宿主保证真实性，协议检查非空。
    /// 二者只借本次，不保存、不异步使用、不trim或规范化；
    /// source不强制PeerId格式，本库不独立认证或选择目标。
    ///
    /// selection按值使用，Trace和期限由协议校验及100ns规则转换；
    /// 不生成新Trace、不读取now、不续期或复制记录器时效策略。
    ///
    /// # 顺序、结果与失败状态
    /// 严格依次执行绑定准入、终端Weak升级、目标解析、业务转换、
    /// 已保存codec的default_encode和唯一send。
    /// 错误依次Binding、TerminalUnavailable、InvalidTarget、Protocol、
    /// Encode、Submit；所有前置失败零次send，提交步骤恰一次。
    /// 协议内部TraceId、source、期限顺序不变，多项非法按先到阶段裁决。
    ///
    /// PeerId类型由ServiceHost::Remote参数推导，解析原因由anyhow接管。
    /// 地址端口使用crate::COORDINATION_SERVICE_PORT转换为ServicePort，
    /// 移交拥有字节并传from=None。目标等于终端本地身份时，
    /// 沿用ServiceTerminal本地路由，不新增自发送拒绝或local比较。
    ///
    /// Ok只表示终端本地受理，不证明送达、接收、选择、传播或导出。
    /// 失败不重试、重连、预探测、转存、补偿或回滚；宿主已有分片及
    /// IPC/QUIC回退属于一次服务提交内部，不等于第二次本库调用。
    /// 重复调用分别执行，不承诺跨调用exactly-once。
    ///
    /// 前置失败不调用记录器或提交；中毒可能按binding政策转Closed，
    /// 编码可能改变依赖TLS/缓冲。提交失败不能证明外部必然零效果。
    /// 不额外保存原调用参数，但实际ServiceError可带host/port/原因。
    /// 本方法不额外记失败日志，显式调用者处理。
    ///
    /// # 并发与资源
    /// 准入返回前已释放绑定锁，其后解析、编码、终端调用及析构均锁外。
    /// 先准入后close仍可继续提交，不增加第二次检查、在途计数或屏障。
    /// 临时Arc<T>覆盖解析、编码和提交，全部成功/失败/展开退出锁外释放。
    /// 若它成为最后强引用，T::Drop可能在本调用线程阻塞或panic，
    /// 已提交副作用不回滚；宿主负责使用期强持终端。
    /// 本库不主动关闭共享通信环境，不捕获依赖panic改普通Err。
    ///
    /// # 成本与副作用
    /// 非纯函数，依赖绑定、终端和codec并可能提交；同步、无await。
    /// 设目标长度m、身份及消息规模n，本库解析/转换最坏O(m²+n)时间、
    /// O(m+n)临时空间；编码、首次TLS解析器复制、缓冲扩容、
    /// T::Deref、终端、日志、回溯和析构成本另计。
    /// 本次借用已保存codec，不克隆、登记、重建Fory或切层。
    /// 内置codec已把Fory原因格式化为ServiceError::CodecError(String)，
    /// 本库保留真实ServiceError，不承诺原fory::Error类型链。
    /// 不新增任务、线程或积压，不承诺零分配、零等待或常量总内存。
    ///
    /// 验证：tests/client/client_test.rs覆盖前置零提交、来源保真、错误和关闭交错；
    /// 精确终端调用、from=None及全分支释放另经独立源码审查。
    pub(crate) fn send_selection(
        &self,
        remote: &TraceNodeId,
        selection: TraceSelection,
        source: &TraceNodeId,
    ) -> Result<(), CoordinationSendError> {
        client_inner::send_selection(self, remote, selection, source)
    }
}

impl<T, C> TraceNotifier for CoordinationClient<T, C>
where
    C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>,
    T: Deref<Target = ServiceTerminal<C>> + Send + Sync + 'static,
{
    /// 复用唯一发送路径，以保存的local作为本跳source。
    ///
    /// remote和selection继承send_selection的合法域和错误优先级，
    /// 只用于本次调用；不先行检查local，不保存参数或另派任务。
    /// 成功直接Ok；普通失败在无绑定锁、无本次临时终端强引用时，
    /// 尝试一条警告，再用anyhow::Error::new接管同一发送错误返回。
    /// 不字符串化替代原因，不克隆错误、重试或额外记成功日志。
    ///
    /// 警告target固定为pi_tracing_coordinator::client；
    /// 字段remote使用%remote.as_str()、service_port使用共用常量、
    /// error使用%error，不加TraceId或操作编号。
    /// 只保证尝试事件，实际记录受宿主过滤/subscriber控制；
    /// pi_logger另累计失败数，本库不初始化第二subscriber。
    ///
    /// 同步且无await；发送成本继承send_selection，日志及回溯另计。
    /// 错误包装可分配/采集回溯，日志可阻塞、重入或panic；
    /// 不捕获这些panic改普通Err，异常时不承诺仍返回包装错误。
    /// 不新增队列、线程或串行锁；上游内置通知线程串行调用的规则不变。
    /// 验证见notify及日志场景；共用路径单次委托由独立源码审查补足。
    fn notify(
        &self,
        remote: &TraceNodeId,
        selection: TraceSelection,
    ) -> anyhow::Result<()> {
        client_inner::notify(self, remote, selection)
    }
}

impl fmt::Display for CoordinationSendError {
    /// 写出实际发送阶段和保存原因，不加入Trace或操作信息。
    ///
    /// 传播fmt::Error，不改变错误或业务状态，不执行日志或提交；
    /// 成本随实际输出长度；验证见error_display及写入失败场景。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        client_inner::fmt_send_error(self, f)
    }
}

impl Error for CoordinationSendError {
    /// 借用实际原因；Binding/Protocol/Encode/Submit返回保存错误本身，不跳层。
    ///
    /// InvalidTarget返回anyhow.as_ref()指向原始解析错误；
    /// TerminalUnavailable返回None。借用受self约束，+'static只限定对象。
    /// O(1)，无分配、格式化或状态改变；验证见error_source真实原因场景。
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        client_inner::send_error_source(self)
    }
}
