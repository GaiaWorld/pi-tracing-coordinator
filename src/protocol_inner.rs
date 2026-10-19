// 设计：tracing-coordinator
// 第1块7芯及第2块集中登记已实现。下列步骤注释保持冻结实现预期。
// 追溯：tests/protocol/protocol_test.rs；docs/SLICE_PROTOCOL_ACCEPTANCE.md#slice-protocol-matrix。

use super::{DeadlineError, DecodedSelection, ProtocolError, SelectionMessage};
use std::{error::Error, fmt, time::SystemTime};

// 绝对时刻转计数：
// 前提：输入是平台可表示的 SystemTime；没有当前时间或业务状态依赖。
// 1. 比较输入与 Unix 纪元，取得时差及其方向，不采样当前时间。
// 2. 纪元之后或相等：用宽整数表示完整纳秒时差，除以100取下整。
// 3. 纪元之前：用宽整数表示时差绝对值，分离完整刻度和余数。
//    余数非零时向更早方向多取一刻度，再对宽计数取负。
//    不把纪元前不足100ns的时差向零截断。
// 4. 得到规范化计数后，比较父模块定义的共同下界与上界。
//    越界返回 OutOfRange，不构造替代值或改变状态。
// 5. 仅在范围内转为 i64，交出计数。
// 后置：相对输入不延后、最多提前99ns；失败无业务副作用。
pub(super) fn deadline_to_100ns(value: SystemTime) -> Result<i64, DeadlineError> {
    // Duration完整纳秒最多约2^94，i128足够；纪元前必须向过去取整。
    // 对应protocol_test的deadline_to四场景，不读取当前时间或平台内部表示。
    let ticks = match value.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => (duration.as_nanos() / 100) as i128,
        Err(error) => -((error.duration().as_nanos().div_ceil(100)) as i128),
    };
    // 先规范化再判域，允许上界外不足100ns的时刻向下落回共同域。
    if ticks < i128::from(super::MIN_DEADLINE_100NS)
        || ticks > i128::from(super::MAX_DEADLINE_100NS)
    {
        return Err(DeadlineError::OutOfRange);
    }
    // 上述共同域严格包含于i64，转换不会丢失信息。
    Ok(ticks as i64)
}

// 计数还原绝对时刻：
// 前提：输入是整数100ns计数，可能越界；本步骤不再次取整。
// 1. 比较共同范围；超界立即返回 OutOfRange。
// 2. 用宽整数取得符号及绝对刻度，分离完整秒和不足一秒的余数。
// 3. 由秒和余数形成非负时间差，不在 i64 内把完整计数乘100。
// 4. 非负计数对Unix纪元检查式加时间差，负计数检查式减。
//    平台无法表示则返回 NotRepresentable，不panic或饱和替代。
// 5. 返回还原时刻。
// 后置：合法可表示计数往返不变；不判断过期、自引用或本地窗口。
pub(super) fn deadline_from_100ns(value: i64) -> Result<SystemTime, DeadlineError> {
    // 外部整数先判域；越界始终是范围错，不能被平台还原错误覆盖。
    if !(super::MIN_DEADLINE_100NS..=super::MAX_DEADLINE_100NS).contains(&value) {
        return Err(DeadlineError::OutOfRange);
    }
    let magnitude = i128::from(value).unsigned_abs();
    // 先拆秒和余数：共同域的秒可放u64，纳秒余数严格小于1秒。
    // 不在i64内将完整计数乘100；对应from_exact_counts的两极与负计数。
    let duration = std::time::Duration::new(
        (magnitude / 10_000_000) as u64,
        ((magnitude % 10_000_000) * 100) as u32,
    );
    let restored = if value < 0 {
        SystemTime::UNIX_EPOCH.checked_sub(duration)
    } else {
        SystemTime::UNIX_EPOCH.checked_add(duration)
    };
    restored.ok_or(DeadlineError::NotRepresentable)
}

// 集中登记：
// 前提：宿主独占未发布、未用于编解码的候选，可已有其它库类型。
// 1. 以父模块固定4300登记唯一SelectionMessage一次。
// 2. 原样返回底层结果，不重试、换号或回滚。
// 后置：失败候选由宿主停止发布；不访问codec槽、网络、服务或记录器。
pub(super) fn register_fory_types(fory: &mut fory::Fory) -> Result<(), fory::Error> {
    // 唯一底层调用直接交还结果；不捕获依赖panic，也不假定失败已回滚。
    // 验证：tests/protocol/registration_test.rs；设计：call-chain.md#chain-protocol-registration。
    fory.register::<SelectionMessage>(super::SELECTION_TYPE_ID)
}

// 发送业务转换：
// 1. 拒绝全零TraceId，返回InvalidTraceId。
// 2. 拒绝空source，返回EmptyIdentity，字段为source。
// 3. 按父模块冻结规则转换期限，准确映射范围/平台表示错误。
// 4. 保留Trace字节顺序和规范化计数，复制原source字节，组成消息。
// 5. 交出消息，不保存source借用。
// 后置：不读now、不裁决过期/自引用，不编码、提交或调用记录器。
pub(super) fn selection_to_message(
    selection: pi_logger::TraceSelection,
    source: &pi_logger::TraceNodeId,
) -> Result<SelectionMessage, ProtocolError> {
    // 首错顺序属于协议合同，身份失败不执行期限转换或复制来源。
    if selection.trace_id == pi_logger::TraceId::INVALID {
        return Err(ProtocolError::InvalidTraceId);
    }
    if source.as_str().is_empty() {
        return Err(ProtocolError::EmptyIdentity { field: "source" });
    }
    let expires_at_100ns = super::deadline_to_100ns(selection.expires_at).map_err(|error| {
        match error {
            DeadlineError::OutOfRange => ProtocolError::DeadlineOutOfRange,
            DeadlineError::NotRepresentable => ProtocolError::DeadlineNotRepresentable,
        }
    })?;
    // Trace按官方字节表示保真；消息拥有来源，不保存本次借用或进行身份规范化。
    Ok(SelectionMessage {
        trace_id: selection.trace_id.to_bytes(),
        expires_at_100ns,
        source: source.as_str().as_bytes().to_vec(),
    })
}

// 接收业务校验：
// 1. 接管消息，拒绝全零TraceId。
// 2. 严格验证source的UTF-8，再检查非空；失败只保留Utf8Error或静态字段。
//    不有损替换、裁剪或规范化，不先构造非法字符串。
// 3. 检查并还原期限，准确映射范围/平台表示错误。
// 4. 全部通过后构造业务身份、上报决定，再封装DecodedSelection。
// 5. 释放原消息，交出拥有式结果，不保存缓冲借用。
// 后置：成功失败均消费输入；失败不进binding或记录器，不复制记录器时效策略。
pub(super) fn validate_selection_message(
    message: SelectionMessage,
) -> Result<DecodedSelection, ProtocolError> {
    // 输入在所有退出路径均归本次调用所有；错误只保存静态字段或实际UTF-8原因。
    if message.trace_id == [0; 16] {
        return Err(ProtocolError::InvalidTraceId);
    }
    let source = std::str::from_utf8(&message.source).map_err(|source| {
        ProtocolError::InvalidIdentityUtf8 { field: "source", source }
    })?;
    if source.is_empty() {
        return Err(ProtocolError::EmptyIdentity { field: "source" });
    }
    let expires_at = super::deadline_from_100ns(message.expires_at_100ns).map_err(|error| {
        match error {
            DeadlineError::OutOfRange => ProtocolError::DeadlineOutOfRange,
            DeadlineError::NotRepresentable => ProtocolError::DeadlineNotRepresentable,
        }
    })?;
    // 完整校验通过才构造业务值；UTF-8借用只在当前栈内，结果不借用原缓冲。
    Ok(DecodedSelection {
        selection: pi_logger::TraceSelection {
            trace_id: pi_logger::TraceId::from_bytes(message.trace_id),
            expires_at,
        },
        source: pi_logger::TraceNodeId::new(source),
    })
}

// 消费交接：移动已有selection/source，不校验、克隆、分配或外调。
pub(super) fn into_parts(
    message: DecodedSelection,
) -> (pi_logger::TraceSelection, pi_logger::TraceNodeId) {
    // 合法结果仅移动；binding负责先准入和解锁，本出口不反查binding或重复校验。
    (message.selection, message.source)
}

// 错误显示：
// 1. 写出真实业务分类、静态字段及已有UTF-8原因，不输出完整非法输入。
// 2. 写出失败原样传播，不记录日志或改变状态。
pub(super) fn fmt_protocol_error(
    error: &ProtocolError,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    // 各分支直接返回写出结果；不吞fmt::Error，不持有或输出完整非法来源。
    match error {
        ProtocolError::InvalidTraceId => f.write_str("TraceId不能全零"),
        ProtocolError::EmptyIdentity { field } => write!(f, "身份字段{field}不能为空"),
        ProtocolError::InvalidIdentityUtf8 { field, source } => {
            write!(f, "身份字段{field}不是合法UTF-8：{source}")
        }
        ProtocolError::DeadlineOutOfRange => f.write_str("期限超出共同100ns计数范围"),
        ProtocolError::DeadlineNotRepresentable => f.write_str("期限无法在当前平台表示"),
    }
}

// 错误取源：非法UTF-8返回保存原因借用；其余None，不分配/格式化或改状态。
pub(super) fn protocol_error_source(
    error: &ProtocolError,
) -> Option<&(dyn Error + 'static)> {
    match error {
        ProtocolError::InvalidIdentityUtf8 { source, .. } => Some(source),
        _ => None,
    }
}

// @inline-test-exception：冻结私有协议边界的独立文件单元测试，不扩大可见性。
#[cfg(test)]
#[path = "../tests/protocol/protocol_test.rs"]
mod protocol_tests;
