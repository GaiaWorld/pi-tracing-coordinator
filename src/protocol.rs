// 设计：tracing-coordinator
//! 唯一上报消息、集中登记与业务合法性边界。
//!
//! client把业务值转换为消息后交宿主codec编码；service接收宏解码的
//! 类型化消息后经本模块校验，binding只消费已校验结果。
//! 本模块不拥有codec，不管理关联、目标、绑定或网络提交。
//!
//! 业务/期限/消费/错误trait及固定4300集中登记均已实现并通过独立审查。
//! 集中登记验收：[矩阵](../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-registration)。
//! 本块验证：[验收](../docs/SLICE_PROTOCOL_ACCEPTANCE.md#slice-protocol-matrix)、
//! [稳定出口](../docs/SLICE_PROTOCOL_API.md#slice-protocol-api)。
//! 正式条目：
//! [接口](../design/tracing-coordinator/07-design/api.md#api-protocol-typed)、
//! [表示](../design/tracing-coordinator/07-design/data.md#data-protocol-typed)、
//! [图源](../design/tracing-coordinator/07-design/call-chain.md#chain-protocol-typed)、
//! [后续验证](../design/tracing-coordinator/07-design/errors.md#errors-protocol-typed)。

use std::{error::Error, fmt, str::Utf8Error, time::SystemTime};

#[path = "protocol_inner.rs"]
mod protocol_inner;

/// 共同计数域下界：1601 年起点相对 Unix 纪元的 100ns 计数。
const MIN_DEADLINE_100NS: i64 = -116_444_736_000_000_000;

/// 共同计数域上界：Windows 有符号 FILETIME 上界扣除纪元偏移。
const MAX_DEADLINE_100NS: i64 = 9_106_927_300_854_775_807;

/// 协议私有期限错误，不携带输入、资源或内部状态。
///
/// 范围错误与平台无法还原必须区分；上层映射为既有 ProtocolError 分类。
#[derive(Debug)]
enum DeadlineError {
    /// 规范化后的发送计数或收到的整数计数超出共同范围。
    OutOfRange,
    /// 计数合法，但当前平台的检查式时间运算无法表示对应时刻。
    NotRepresentable,
}

/// 将绝对时刻转换为 Unix 纪元有符号 100ns 计数。
///
/// # 输入与返回
/// value 按值传入，不被保存；不读取当前时间，也不判断是否过期。
/// 先数学向下规范化，再检查共同范围，成功返回合法 i64 计数；
/// 越界返回 OutOfRange。共同范围由本模块两个边界常量定义。
///
/// 最多提前 99ns，绝不延后：+99ns→0、+100ns→1、
/// -1ns→-1、-100ns→-1、-101ns→-2。
/// Linux 上界外 1..=99ns 可规范化回上界，外 100ns 拒绝。
///
/// # 目标性质与失败状态
/// 目标转换是纯函数，O(1) 时间和空间，无分配、锁、I/O、回调或任务；
/// 不修改全局、输入或业务状态，可重复及并发调用，不含 await。
/// 使用宽整数中间值，不能依赖溢出或负数向零截断；普通越界返回错误。
/// 合法过期期限仍可转换，不续期、不裁剪本地历史窗口。
///
/// # 验证入口
/// tests/protocol/protocol_test.rs 中 deadline_to 系列及期限往返场景；
/// Linux纳秒余数与Windows实际100ns输入分别验证，见本块验收矩阵。
fn deadline_to_100ns(value: SystemTime) -> Result<i64, DeadlineError> {
    protocol_inner::deadline_to_100ns(value)
}

/// 将合法 Unix 纪元 100ns 整数计数还原为绝对时刻。
///
/// # 输入与返回
/// value 的单位为 100ns；先检查共同范围，不再次取整。
/// 成功返回调用方拥有的 SystemTime；越界返回 OutOfRange，
/// 平台检查式还原失败返回 NotRepresentable，不用饱和值代替。
///
/// # 目标性质与失败状态
/// 目标转换是纯函数，O(1) 时间和空间，无分配、锁、I/O、回调或任务；
/// 不保存输入、不读当前时间、不判断过期，不修改任何业务状态。
/// 使用宽整数拆分秒和余数及 checked_add/sub；
/// 不在 i64 内直接把完整计数乘 100，也不对 i64::MIN 直接取负。
/// 可重复及并发调用，不含 await；合法可表示计数往返保持不变。
///
/// # 验证入口
/// tests/protocol/protocol_test.rs 中 deadline_from 系列及期限往返场景；
/// 共同域在目标两平台可表示，NotRepresentable保留为检查式失败分类。
fn deadline_from_100ns(value: i64) -> Result<SystemTime, DeadlineError> {
    protocol_inner::deadline_from_100ns(value)
}

/// 唯一上报消息的Fory类型号，与服务端口属于不同编号域。
/// 4301–4310仅文档预留，本批不登记占位类型。
const SELECTION_TYPE_ID: u32 = 4300;

/// 库内待校验的上报消息，字段保持私有。
///
/// trace_id为原TraceId的16字节，顺序不变、全零非法；
/// expires_at_100ns为Unix纪元有符号100ns计数，遵守本模块共同范围；
/// source为本跳发送身份的原始UTF-8字节，接收后严格验证再构造业务身份。
///
/// 消息只拥有数据，不持有codec、绑定、锁、记录器或终端。
/// client可借它编码，service可接收并交给本模块校验。
/// Fory解码成功及派生默认值均不代表业务合法；不开放跨模块字段写入。
#[derive(fory::ForyObject)]
pub(crate) struct SelectionMessage {
    trace_id: [u8; 16],
    expires_at_100ns: i64,
    source: Vec<u8>,
}

/// 仅由protocol完整业务校验后构造的库内交接结果。
///
/// 拥有selection/source，不借用输入缓冲，不持有codec或绑定资源。
/// 字段私有，不派生序列化、Default或Clone，不提供字段getter或可写出口。
/// binding只能通过消费into_parts移动取得已有业务值。
pub(crate) struct DecodedSelection {
    selection: pi_logger::TraceSelection,
    source: pi_logger::TraceNodeId,
}

/// 库内业务转换错误，不包含codec错误或完整非法输入。
///
/// 身份field当前只使用静态字符串"source"；UTF-8分类保留实际Utf8Error。
/// Debug为派生实现；Display/source保留直接分类和实际UTF-8原因。
/// 验证：tests/protocol/protocol_test.rs 中 test_error 系列。
#[derive(Debug)]
pub(crate) enum ProtocolError {
    InvalidTraceId,
    EmptyIdentity {
        field: &'static str,
    },
    InvalidIdentityUtf8 {
        field: &'static str,
        source: Utf8Error,
    },
    DeadlineOutOfRange,
    DeadlineNotRepresentable,
}

/// 向宿主独占的未发布类型表登记本库唯一消息。
///
/// @public
/// fory可以已登记其它库类型，但尚未发布或用于编解码；不保存该借用。
/// 只以4300登记SelectionMessage，不改宿主codec槽或配置。
/// 成功仅表示本步登记完成，不证明选层、服务初始化或收发成功。
///
/// 先登记、后发布、再预绑定；同一实际表只调用一次，重复不是幂等成功。
/// 普通失败原样返回fory::Error，不保证候选原样，不回滚、重试或换号；
/// 宿主停止发布该候选。
///
/// 非纯冷路径，可分配；本库一次底层登记，保守计入既有表项N和索引
/// 扩容规模K，为O(N+K)最坏工作及扩容空间，不承诺固定O(1)。
/// 不取本库状态锁，无await，不触网、注册服务或调用记录器。
/// 普通Result错误要求依赖构建时未设置FORY_PANIC_ON_ERROR；
/// 该变量存在即启用，值0也启用。本库不捕获依赖panic伪装普通错误。
///
/// 验证：tests/protocol/registration_test.rs；共表、固定号、配置保持及两codec往返已验证。
pub fn register_fory_types(fory: &mut fory::Fory) -> Result<(), fory::Error> {
    protocol_inner::register_fory_types(fory)
}

/// 将本次上报业务值转换为待宿主codec编码的消息。
///
/// @internal
/// selection按值使用；source只借本次，不保存或异步使用。
/// 按TraceId非全零、source非空、期限的顺序校验，首错返回ProtocolError。
/// source是合法Rust字符串，不要求PeerId，不裁剪或规范化原始身份。
/// 期限按本模块100ns规则向下规范化；不读now或判断过期、自引用、历史窗口。
///
/// 成功交出拥有身份字节的SelectionMessage；失败无编码、提交或记录器副作用，
/// 不改变业务状态。目标为同步纯转换，O(n)时间/额外空间，n为身份字节数。
/// 不取状态锁，无await、线程、任务或全局业务状态，可对独立输入并发调用。
/// 分配失败等进程级异常不伪装普通业务错误。
///
/// 验证：tests/protocol/protocol_test.rs 中 test_send 系列；
/// 包含稀疏Trace、原始非ASCII来源、期限及多项非法时固定首错。
pub(crate) fn selection_to_message(
    selection: pi_logger::TraceSelection,
    source: &pi_logger::TraceNodeId,
) -> Result<SelectionMessage, ProtocolError> {
    protocol_inner::selection_to_message(selection, source)
}

/// 校验宏已经解码的拥有式消息，建立合法业务交接结果。
///
/// @internal
/// message成功失败均被消费；按TraceId、source严格UTF-8/非空、期限顺序校验。
/// 全部通过后才构造TraceNodeId、TraceSelection和DecodedSelection。
/// 成功交出拥有的selection/source；失败释放消息、不返还缓冲、不进入binding或记录器。
/// 不保存输入借用，不裁剪、规范化或有损替换身份，不读now或复制记录器时效策略。
///
/// 目标为同步纯转换，O(n)时间/额外空间，n为身份字节数。
/// 不取状态锁，无await、线程、任务或全局业务状态，可处理独立并发输入。
/// 分配等异常不伪装普通错误；本函数不额外保证宏前置解码或任意异常Fory输入安全。
///
/// 验证：tests/protocol/protocol_test.rs 中 test_receive 系列；
/// 校验拒绝、字段保真及合法消费已验证，真实记录器发布归binding阶段。
pub(crate) fn validate_selection_message(
    message: SelectionMessage,
) -> Result<DecodedSelection, ProtocolError> {
    protocol_inner::validate_selection_message(message)
}

impl DecodedSelection {
    /// 消费交接结果，移动交出已有上报决定和本跳来源。
    ///
    /// @internal
    /// 不重新校验、克隆、分配、取锁、执行I/O或外调；目标O(1)时间和额外空间。
    /// self消费后不能重用，不改变协议或业务状态。
    /// 由test_receive_preserves_decision_and_source及sent_message_roundtrip保护；
    /// 记录器准入和锁外调用由binding阶段验证。
    pub(crate) fn into_parts(self) -> (pi_logger::TraceSelection, pi_logger::TraceNodeId) {
        protocol_inner::into_parts(self)
    }
}

impl fmt::Display for ProtocolError {
    /// 写出真实分类、静态字段及已有原因；传播fmt::Error，不保存输入或改变状态。
    /// 成本随输出长度，格式化目标外部执行另计；不记录日志。
    /// 验证：test_error_display系列，含前缀成功后的字段/原因写失败。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        protocol_inner::fmt_protocol_error(self, f)
    }
}

impl Error for ProtocolError {
    /// 仅InvalidIdentityUtf8返回实际Utf8Error借用，其余None。
    /// 借用受self约束；O(1)、无分配/格式化/状态变化。
    /// 验证：test_error_source系列，核对实际原因同地址借用及其余None。
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        protocol_inner::protocol_error_source(self)
    }
}
