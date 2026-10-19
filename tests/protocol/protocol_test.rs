//! slice-protocol 测试声明：只透传同名场景，断言位于 protocol_test_inner.rs。
//! @api:src/protocol.rs @impl:src/protocol_inner.rs
//! @test:tests/protocol/protocol_test.rs
//! @design-doc:design/tracing-coordinator/07-design/call-chain.md#slice-protocol
//! 测试作为 protocol 子模块挂载，访问已冻结私有边界，不扩大生产可见性。

#[path = "protocol_test_inner.rs"]
mod protocol_test_inner;

/// 契约：slice-protocol / deadline_to_100ns。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @critical
/// Scenario Outline: 对齐的纪元两侧时刻转换为精确有符号计数。
/// Given 纪元及正负100ns、999999900ns、1000000000ns、1000000100ns
/// When 经壳调用 deadline_to_100ns
/// Then 依次得到0、正负1、9999999、10000000、10000001，无当前时间依赖。
#[test]
fn test_deadline_to_aligned_epoch_sides() {
    protocol_test_inner::test_deadline_to_aligned_epoch_sides();
}

/// 契约：slice-protocol / deadline_to_100ns；仅Linux可构造这些纳秒输入。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @edgecase
/// Scenario Outline: 非对齐期限向过去取整，负余数不能向零截断。
/// Given 纪元两侧1、99、101、199、1000000001ns的固定时刻
/// When 经壳转换
/// Then 正侧为0、0、1、1、10000000，负侧为-1、-1、-2、-2、-10000001。
#[cfg(target_os = "linux")]
#[test]
fn test_deadline_to_floors_fractional_ticks() {
    protocol_test_inner::test_deadline_to_floors_fractional_ticks();
}

/// 契约：slice-protocol / 两个期限边界常量与deadline_to_100ns。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @edgecase
/// Scenario: 共同极值精确转换，各平台可构造的外侧拒绝。
/// Given 1601起点及Unix后910692730085秒477580700ns
/// When 转换这两个极值及平台可构造的极值外侧
/// Then 极值分别为-116444736000000000和9106927300854775807，外侧为OutOfRange。
#[test]
fn test_deadline_to_common_bounds_and_outside() {
    protocol_test_inner::test_deadline_to_common_bounds_and_outside();
}

/// 契约：slice-protocol / 先规范化再检查范围。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @edgecase
/// Scenario: Linux上界外不足一刻度可向下归入共同域。
/// Given 上界之后1至99ns及100ns、下界之前1ns
/// When 转换期限
/// Then 前99个输入仍为上界，后两类OutOfRange，绝不饱和裁剪。
#[cfg(target_os = "linux")]
#[test]
fn test_deadline_to_normalizes_before_range_check() {
    protocol_test_inner::test_deadline_to_normalizes_before_range_check();
}

/// 契约：slice-protocol / deadline_from_100ns。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @critical
/// Scenario Outline: 合法整数计数精确还原，不能因完整计数乘100溢出。
/// Given 共同两极、纪元及正负1、9999999、10000000、10000001
/// When 还原SystemTime并再次转换
/// Then 与独立秒/纳秒字面量时刻相同，往返计数不变。
#[test]
fn test_deadline_from_exact_counts() {
    protocol_test_inner::test_deadline_from_exact_counts();
}

/// 契约：slice-protocol / DeadlineError范围分类。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @negative
/// Scenario Outline: 越界整数先判域，不误报平台表示错误。
/// Given i64::MIN、下界减1、上界加1、i64::MAX
/// When 调用deadline_from_100ns
/// Then 全部返回OutOfRange，不panic、不饱和或绕回。
#[test]
fn test_deadline_from_rejects_out_of_range() {
    protocol_test_inner::test_deadline_from_rejects_out_of_range();
}

/// 契约：slice-protocol / 规范化往返。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @edgecase
/// Scenario: 时刻往返绝不延后，最多提前99ns，再次转换稳定。
/// Given 纪元、负100ns、纪元前后1秒和共同极值附近的可构造输入
/// When 转计数、还原并再转计数
/// Then 还原时刻不晚于输入、差值小于100ns，第二次计数相同。
#[test]
fn test_deadline_roundtrip_never_delays() {
    protocol_test_inner::test_deadline_roundtrip_never_delays();
}

/// 契约：slice-protocol / selection_to_message。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @critical
/// Scenario Outline: 消息拥有原始Trace、来源及对齐期限，不保存来源借用。
/// Given 非对称16字节Trace或首/尾单非零Trace、来源“  节点é/e组合字符🦀\0 ”或纯空白、共同极值和纪元两侧期限
/// When 将业务值转换并释放原来源
/// Then 三消息字段精确保持Trace顺序、原来源字节和固定100ns计数，过期期限仍合法。
#[test]
fn test_send_preserves_owned_business_content() {
    protocol_test_inner::test_send_preserves_owned_business_content();
}

/// 契约：slice-protocol / 发送校验固定首错。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @negative
/// Scenario Outline: 全零Trace优先于空来源及非法期限，空来源优先于期限。
/// Given 全零或合法Trace、空或非空source及各平台可构造期限；Linux还包括域外期限
/// When 转换业务消息
/// Then 全零总为InvalidTraceId；合法Trace加空source总为EmptyIdentity且field为source。
#[test]
fn test_send_rejection_priority() {
    protocol_test_inner::test_send_rejection_priority();
}

/// 契约：slice-protocol / 发送期限范围映射；Linux可构造共同域外时刻。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @negative
/// Scenario: 身份合法时域外期限映射为DeadlineOutOfRange。
/// Given 合法Trace、来源“发送者”、共同下界前1ns或上界后100ns
/// When 转换业务消息
/// Then 均返回DeadlineOutOfRange，不创建合法消息。
#[cfg(target_os = "linux")]
#[test]
fn test_send_maps_deadline_range_error() {
    protocol_test_inner::test_send_maps_deadline_range_error();
}

/// 契约：slice-protocol / 发送期限规范化。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @edgecase
/// Scenario Outline: 消息携带向下规范化的期限，不读now或逐跳续期。
/// Given 纪元后99ns、前1ns、前101ns和共同上界后99ns
/// When 转换合法业务值
/// Then 消息期限依次为0、-1、-2及共同上界。
#[cfg(target_os = "linux")]
#[test]
fn test_send_normalizes_deadline() {
    protocol_test_inner::test_send_normalizes_deadline();
}

/// 契约：slice-protocol / validate_selection_message、DecodedSelection::into_parts。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @critical
/// Scenario Outline: 完整合法消息消费后交出原决定和来源。
/// Given 非对称Trace、包含非ASCII/组合字符/NUL/前后空白的原始UTF-8或单个非零Trace字节，以及共同域期限
/// When 校验拥有式消息，再经into_parts消费合法结果
/// Then Trace、精确期限及原来源文本不变；过期期限仍通过协议校验，不重复校验或另造字段移动测试。
#[test]
fn test_receive_preserves_decision_and_source() {
    protocol_test_inner::test_receive_preserves_decision_and_source();
}

/// 契约：slice-protocol / 严格UTF-8与实际原因。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @negative
/// Scenario Outline: 不合法UTF-8拒绝，不能用有损替换得到身份。
/// Given 合法Trace、合法期限、来源包含孤立续字节/过长编码/代理项/超范围码点/截断多字节
/// When 消费消息并校验
/// Then 返回InvalidIdentityUtf8，field为source，保存与std严格校验相同的valid_up_to/error_len。
#[test]
fn test_receive_rejects_malformed_utf8() {
    protocol_test_inner::test_receive_rejects_malformed_utf8();
}

/// 契约：slice-protocol / 接收固定首错。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @negative
/// Scenario Outline: 多项非法时Trace优先于来源，来源优先于期限。
/// Given 全零或合法Trace、非法UTF-8或空来源和i64::MIN/上界加1等非法期限
/// When 消费消息并校验
/// Then 全零总InvalidTraceId；合法Trace加非法UTF-8为InvalidIdentityUtf8，加空来源为EmptyIdentity(source)。
#[test]
fn test_receive_rejection_priority() {
    protocol_test_inner::test_receive_rejection_priority();
}

/// 契约：slice-protocol / 接收期限范围映射。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @negative
/// Scenario Outline: 合法身份之后的非法计数拒绝且保持范围分类。
/// Given 合法Trace/来源与i64::MIN、共同下界减1、共同上界加1、i64::MAX
/// When 校验消息
/// Then 返回DeadlineOutOfRange而非平台表示错误，不交出DecodedSelection。
#[test]
fn test_receive_rejects_illegal_deadlines() {
    protocol_test_inner::test_receive_rejects_illegal_deadlines();
}

/// 契约：slice-protocol / 合法转换、校验和消费组合。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @critical
/// Scenario: 发送消息校验与消费后再次发送保持同一规范化内容。
/// Given 原始Trace、非ASCII来源及平台可构造的非对齐或对齐期限
/// When 发送转换、校验、into_parts后再次转换
/// Then 三消息字段逐项相同，来源原句保留；此处不声称Fory编码/传输或binding发布通过。
#[test]
fn test_receive_sent_message_roundtrip() {
    protocol_test_inner::test_receive_sent_message_roundtrip();
}

/// 契约：slice-protocol / ProtocolError::fmt。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @critical
/// Scenario: 五类错误显示可区分的真实分类和必要字段，不伪造原因。
/// Given InvalidTraceId、EmptyIdentity(source)、非法UTF-8、期限越界及期限平台不可表示
/// When 经Display格式化
/// Then 文本分别表达TraceId、source为空、source/UTF-8及实际原因、期限范围、期限不可表示，分类文本不同。
#[test]
fn test_error_display_classifies_actual_failure() {
    protocol_test_inner::test_error_display_classifies_actual_failure();
}

/// 契约：slice-protocol / ProtocolError::fmt与错误不滞留输入。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @negative
/// Scenario: 非法来源不进入显示文本，已有UTF-8原因仍可读。
/// Given 来源字节含“不要泄露/来源秘密”及0xff，Trace和期限合法
/// When 校验失败后格式化错误
/// Then 显示包含实际Utf8Error文本，不包含完整来源或秘密前缀。
#[test]
fn test_error_display_does_not_echo_invalid_input() {
    protocol_test_inner::test_error_display_does_not_echo_invalid_input();
}

/// 契约：slice-protocol / fmt::Error传播。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @negative
/// Scenario Outline: 写入器拒绝时原样传播格式化错误。
/// Given 五类协议错误及从0到输出长度减1的每种不足容量写入器，另有容量充足对照
/// When 经fmt::write格式化
/// Then 失败写入返回fmt::Error且不继续写入，充足容量写入成功并与Display一致。
#[test]
fn test_error_display_propagates_writer_failure() {
    protocol_test_inner::test_error_display_propagates_writer_failure();
}

/// 契约：slice-protocol / ProtocolError::source。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @critical
/// Scenario: UTF-8分类返回错误内部实际原因的借用，不克隆或另造源。
/// Given 接收非法来源产生的InvalidIdentityUtf8及其已保存Utf8Error
/// When 经Error::source读取原因
/// Then 能下转为Utf8Error且指针与保存原因相同，valid_up_to/error_len准确，源自身没有更深原因。
#[test]
fn test_error_source_borrows_actual_utf8_cause() {
    protocol_test_inner::test_error_source_borrows_actual_utf8_cause();
}

/// 契约：slice-protocol / 无原因错误。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @edgecase
/// Scenario Outline: 非UTF-8分类没有虚构底层原因。
/// Given InvalidTraceId、EmptyIdentity(source)、DeadlineOutOfRange、DeadlineNotRepresentable
/// When 经Error::source查询
/// Then 每次均None，不格式化或修改错误。
#[test]
fn test_error_source_is_none_for_other_kinds() {
    protocol_test_inner::test_error_source_is_none_for_other_kinds();
}
