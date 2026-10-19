//! slice-registration 测试声明：场景与同名实现一一对应，声明面只透传。
//! @api:src/protocol.rs @impl:src/protocol_inner.rs
//! @test:tests/protocol/registration_test.rs
//! @design-doc:design/tracing-coordinator/07-design/call-chain.md#slice-registration
//! 复用既有协议测试模块的合法作用域，不扩大消息/字段可见性。
//! 只证明登记及独立codec表示，不证明服务初始化、通信或失败候选回滚。

#[path = "registration_test_inner.rs"]
mod registration_test_inner;

/// 契约：slice-registration / 普通Result错误的实际构建前提。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @edgecase
/// Scenario: 记录依赖的实际编译开关，并校准真实codec及登记错误观测。
/// Given 实际fory-core 0.14.1、真实宿主类型在新候选上登记4300
/// When 读取PANIC_ON_ERROR并用两个真实codec往返宿主值、触发重复和冲突
/// Then 开关为false，值精确保持，重复/冲突为实际TypeError；此例仅校准夹具，不作为本库登记的绿色证据。
#[test]
fn test_registration_dependency_build_and_observation_controls() {
    registration_test_inner::test_registration_dependency_build_and_observation_controls();
}

/// 契约：slice-registration / register_fory_types及SELECTION_TYPE_ID。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @critical
/// Scenario: 新候选中的唯一消息以固定4300进入真实编码表示。
/// Given 独占、未发布且未编解码的Fory默认候选和非对称Trace/来源“节点-A”/期限-100ns
/// When 经协议壳调用register_fory_types一次，之后才通过真实Fory编码解码消息
/// Then 返回Ok，编码的结构类型号为1100815即固定4300的默认结构表示，消费后Trace/来源/期限不变。
#[test]
fn test_registration_fresh_candidate_uses_fixed_4300() {
    registration_test_inner::test_registration_fresh_candidate_uses_fixed_4300();
}

/// 契约：slice-registration / 候选可含其它库类型，成功后宿主完成其余登记。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @critical
/// Scenario: 本库与登记前后的宿主类型在同一真实表共存。
/// Given 候选已登记HostEntry=4299且尚未编解码
/// When 完成本库一次登记，再登记HostTail=4311，最后发布为DefaultCodec
/// Then 三种真实类型均可使用，HostEntry.marker=2712847316、HostTail.value=-1234567890及消息内容精确保留。
#[test]
fn test_registration_preserves_shared_table_types_before_and_after() {
    registration_test_inner::test_registration_preserves_shared_table_types_before_and_after();
}

/// 契约：slice-registration / 4301至4310仅文档预留，不登记占位类型。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @edgecase
/// Scenario Outline: 一次集中登记不会占用文档预留号。
/// Given 每例独立的新Fory候选，Examples为4301至4310每个整数
/// When 本库登记完成后，宿主把HostEntry登记在该例编号上
/// Then 宿主登记成功，真实表同时编码固定4300的消息与对应编号的HostEntry，不以批量占位扩张登记范围。
#[test]
fn test_registration_leaves_document_only_reserved_ids_unregistered() {
    registration_test_inner::test_registration_leaves_document_only_reserved_ids_unregistered();
}

/// 契约：slice-registration / 同一实际表只登记一次，重复不是幂等成功。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @negative
/// Scenario: 同一候选重复集中登记原样返回Fory重复类型错误。
/// Given 候选首次集中登记已成功，此后仍未发布或编解码
/// When 再调用一次register_fory_types
/// Then 返回fory::Error::TypeError，载荷精确为本消息Rust类型已登记的依赖原因；不包装、吞错或报告幂等成功，失败候选不再使用。
#[test]
fn test_registration_duplicate_returns_actual_fory_error() {
    registration_test_inner::test_registration_duplicate_returns_actual_fory_error();
}

/// 契约：slice-registration / 固定4300冲突保留实际错误，不探号或换号。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @negative
/// Scenario: 其它类型预占4300时集中登记失败。
/// Given 未发布候选已将Occupant登记为4300，尚未用于编解码
/// When 经协议壳集中登记本库消息
/// Then 返回实际TypeError，完整原因指出内部类型号1100815冲突；不改用4301或其它号，丢弃失败候选且不宣称它保持原样。
#[test]
fn test_registration_conflict_returns_actual_fory_error() {
    registration_test_inner::test_registration_conflict_returns_actual_fory_error();
}

/// 契约：slice-registration / 普通失败停止发布，失败候选处置归宿主。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @negative
/// Scenario: 遵守契约的宿主在登记失败时保留原已发布表。
/// Given 宿主已有仅含HostEntry=4299的真实DefaultCodec，另有Occupant=4300的独占候选
/// When 宿主调用集中登记并只在全部必要登记成功后替换发布槽
/// Then 冲突原样返回、发布槽仍指向旧表且旧业务可用；新合法候选的成功对照确会发布。本例证明宿主停止发布的消费方式，不给库函数虚构发布或回滚能力。
#[test]
fn test_registration_host_stops_publishing_failed_candidate() {
    registration_test_inner::test_registration_host_stops_publishing_failed_candidate();
}

/// 契约：slice-registration / DefaultCodec独立表示往返及真实默认表。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @critical
/// Scenario Outline: 两个独立登记的默认codec精确保持消息内容。
/// Given 两个不同Fory候选，宿主类型按不同顺序登记；消息含非对称或首/尾单非零Trace，原始非ASCII/空白/长来源，期限含共同两极、-100ns、纪元和1秒100ns
/// When 各自集中登记并发布为DefaultCodec，再双向独立编码解码；Linux另验证-101ns规范化为-200ns
/// Then 实际默认快照分别为对应已登记表、真实类型号为固定4300、所有业务值精确保留；未登记表的编码负面对照失败。
#[test]
fn test_registration_default_codec_independent_representation_roundtrips() {
    registration_test_inner::test_registration_default_codec_independent_representation_roundtrips();
}

/// 契约：slice-registration / MultiLayerCodec独立表示往返及真实目标表。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @critical
/// Scenario Outline: 两个多层codec只使用明确目标层中的已登记表往返消息。
/// Given 两端默认表均未登记本消息，独立目标表已按集中清单完成登记，目标层名为tracing-coordinator
/// When 验证缺层回退不能编码消息的负面对照，再显式发布目标表、核对has_layer/layer_fory并克隆切层预绑定
/// Then 当前实际快照各自等于已登记目标表，双向消息与默认codec相同样本精确保真；不以层名或switch_layer成功代替真实表证据。
#[test]
fn test_registration_multi_layer_codec_independent_representation_roundtrips() {
    registration_test_inner::test_registration_multi_layer_codec_independent_representation_roundtrips();
}

/// 契约：slice-registration / Fory派生默认值与业务合法性分离。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @edgecase
/// Scenario: 派生默认表示可编解码但不产生合法业务结果。
/// Given 成功集中登记并发布的真实DefaultCodec及SelectionMessage的ForyDefault值
/// When 经真实codec编码解码后交协议校验
/// Then 仍返回InvalidTraceId，不能把Fory派生默认或解码成功当作业务已合法。
#[test]
fn test_registration_derived_default_does_not_bypass_validation() {
    registration_test_inner::test_registration_derived_default_does_not_bypass_validation();
}

/// 契约：slice-registration / register_fory_types成功登记不改变宿主配置（R1）。
/// @api:src/protocol.rs @impl:src/protocol_inner.rs @edgecase
/// Scenario Outline: 成功集中登记保持未发布候选的全部可查询配置。
/// Given 独占、未发布且未用于编解码的真实Fory候选，六项配置为下表值且已通过公开查询确认
/// When 经协议壳调用register_fory_types一次并要求成功
/// Then 通过Fory六个公开查询逐项确认配置仍为原值；仅要求成功候选保持配置，不要求失败候选保持原样。
/// Examples:
/// | compatible | share_meta | xlang | compress_string | max_dyn_depth | check_struct_version |
/// | true       | true       | true  | true            | 17            | false                |
/// | false      | false      | false | false           | 29            | true                 |
#[test]
fn test_registration_preserves_non_default_host_configuration() {
    registration_test_inner::test_registration_preserves_non_default_host_configuration();
}
