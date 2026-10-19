//! slice-registration 测试实现：只调用冻结protocol壳与依赖公开API。
//! @api:src/protocol.rs @impl:src/protocol_inner.rs
//! @test:tests/protocol/registration_test.rs
//! @design-doc:design/tracing-coordinator/07-design/call-chain.md#slice-registration
//! 本模块不导入生产芯、不构造服务/协调器、不补登记逻辑；宿主发布夹具只演示调用方责任。
//! 候选在所有登记完成前不编解码；重复/冲突后的候选直接丢弃，不测试或假定回滚。

use crate::protocol::{
    register_fory_types, selection_to_message, validate_selection_message, ProtocolError,
    SelectionMessage, SELECTION_TYPE_ID,
};
use fory::{Fory, ForyDefault, Reader};
use pi_service::{DefaultCodec, MultiLayerCodec, ServiceCodec};
use std::{any::TypeId, sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};

/// 真实宿主共表类型：4299与4300不冲突，字段值用于观察原登记是否仍可使用。
#[derive(fory::ForyObject)]
struct HostEntry {
    marker: u32,
}

/// 宿主在本库登记之后才登记的真实类型，保护后续必要登记仍可完成。
#[derive(fory::ForyObject)]
struct HostTail {
    value: i64,
}

/// 独立类型用于真实占用4300；没有自定义序列化器、替身表或注入错误。
#[derive(fory::ForyObject)]
struct Occupant {
    code: u32,
}

/// 不对称Trace用于暴露倒序、截断或替换，包含非ASCII字节值。
const TRACE: [u8; 16] = [1, 35, 69, 103, 137, 171, 205, 239, 254, 220, 186, 152, 118, 84, 50, 16];
const LAYER: &str = "tracing-coordinator";

/// 记录的是依赖编译期常量，不拿测试进程当前环境推断构建开关。
/// 此前置失败为环境阻塞，不是register_fory_types的目标RED；不捕获依赖panic冒充Err。
fn require_normal_fory_errors() {
    let panic_on_error = fory_core::error::should_panic_on_error();
    eprintln!("slice-registration: fory-core=0.14.1 FORY_PANIC_ON_ERROR={panic_on_error}");
    assert!(!panic_on_error, "本轮普通错误矩阵要求依赖构建时未设置FORY_PANIC_ON_ERROR，值0亦启用");
}

/// 精确匹配依赖TypeError载荷，保留分类和完整原因；其它错误或成功都不能冒充预期失败。
fn assert_type_error(result: Result<(), fory::Error>, expected: &str) {
    match result {
        Err(fory::Error::TypeError(reason)) => assert_eq!(reason.as_ref(), expected),
        Err(other) => panic!("必须原样返回Fory TypeError，实际为{other:?}"),
        Ok(()) => panic!("重复或冲突不得返回成功"),
    }
}

/// 普通默认结构模式下依赖的真实冲突原因；1100815为4300对应的实际结构类型号。
fn conflict_reason() -> &'static str {
    "Type ID 1100815 conflicts with already registered type. Please use a different type ID."
}

/// 只读真实编码对象头：默认Rust模式的位图2、非空标记-1、随后为varuint32类型号。
/// 使用Fory公开Reader，不复制编码器或消息字段序列化；这不是任意输入/EOF安全测试。
fn assert_wire_type_id(bytes: &[u8], expected: u32) {
    let mut reader = Reader::new(bytes);
    assert_eq!(reader.read_u8().unwrap(), 2, "应使用冻结的默认Rust同结构模式");
    assert_eq!(reader.read_i8().unwrap(), -1, "应为实际非空消息而不是派生默认替代");
    assert_eq!(reader.read_varuint32().unwrap(), expected, "真实使用表的登记编号不符");
}

/// 通过已验收协议出口产生消息，不手工构造消息字段或DecodedSelection。
fn business_message(trace: [u8; 16], source: &str, expires_at: SystemTime) -> SelectionMessage {
    let source = pi_logger::TraceNodeId::new(source);
    selection_to_message(pi_logger::TraceSelection {
        trace_id: pi_logger::TraceId::from_bytes(trace),
        expires_at,
    }, &source).unwrap()
}

/// 只经合法校验/消费出口观测业务结果，预期来自场景字面量而非反向生产算法。
fn assert_business_content(message: SelectionMessage, trace: [u8; 16], source: &str, expires_at: SystemTime) {
    let (selection, identity) = validate_selection_message(message).unwrap().into_parts();
    assert_eq!(selection.trace_id.to_bytes(), trace);
    assert_eq!(selection.expires_at, expires_at);
    assert_eq!(identity.as_str(), source);
}

/// 真实未登记默认表应拒绝本消息编码；该负面控制不通过构造错误或catch_unwind实现。
fn assert_unregistered_encode_rejected<C>(codec: &C)
where
    C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>,
{
    let message = business_message(TRACE, "负面对照", UNIX_EPOCH);
    match codec.default_encode(&message) {
        Err(pi_service::ServiceError::CodecError(reason)) => {
            let expected = format!(
                "fory encode failed: TypeId {:?} not found in type_id_index, maybe you forgot to register some types (type: {})",
                TypeId::of::<SelectionMessage>(), std::any::type_name::<SelectionMessage>(),
            );
            assert_eq!(reason, expected, "必须是当前消息缺少真实类型登记的依赖原因");
        }
        Err(other) => panic!("负面对照的失败原因不符：{other:?}"),
        Ok(_) => panic!("没有集中登记的真实使用表不能编码本消息"),
    }
}

/// 依赖校准独立于生产登记桩：真实宿主类型、真实表和真实两种codec，绿色不计产品登记证据。
pub(super) fn test_registration_dependency_build_and_observation_controls() {
    require_normal_fory_errors();
    let mut fory = Fory::default();
    fory.register::<HostEntry>(4300).unwrap();
    // 该候选尚未编解码，重复/冲突的结果只校准依赖；失败后整表丢弃。
    let expected_duplicate = format!("rs_struct:{:?} already registered", TypeId::of::<HostEntry>());
    assert_type_error(fory.register::<HostEntry>(4300), &expected_duplicate);
    drop(fory);
    let mut conflicting = Fory::default();
    conflicting.register::<HostEntry>(4300).unwrap();
    assert_type_error(conflicting.register::<Occupant>(4300), conflict_reason());
    drop(conflicting);

    // 使用另一份从未失败的候选校准编码对象头，不继续使用失败候选。
    let mut valid = Fory::default();
    valid.register::<HostEntry>(4300).unwrap();
    let valid = Arc::new(valid);
    let default = DefaultCodec::new(valid.clone());
    let bytes = default.default_encode(&HostEntry { marker: 0xa1b2c3d4 }).unwrap();
    assert_wire_type_id(&bytes, 1_100_815);
    let decoded: HostEntry = default.default_decode(bytes).unwrap();
    assert_eq!(decoded.marker, 0xa1b2c3d4);
    let mut multi = MultiLayerCodec::new(Arc::new(Fory::default()));
    multi.register_layer_fory(LAYER, valid.clone());
    assert!(multi.has_layer(LAYER));
    assert!(Arc::ptr_eq(&*multi.layer_fory(LAYER).unwrap(), &valid));
    multi.switch_layer(LAYER).unwrap();
    assert!(Arc::ptr_eq(&*multi.current_fory(), &valid));
    let bytes = multi.default_encode(&HostEntry { marker: 0x10203040 }).unwrap();
    assert_wire_type_id(&bytes, 1_100_815);
    let decoded: HostEntry = multi.default_decode(bytes).unwrap();
    assert_eq!(decoded.marker, 0x10203040);
    assert_unregistered_encode_rejected(&DefaultCodec::default());
}

/// 空候选登记成功后才开始首次编解码；固定字面量同时保护常量及真实wire编号。
pub(super) fn test_registration_fresh_candidate_uses_fixed_4300() {
    require_normal_fory_errors();
    assert_eq!(SELECTION_TYPE_ID, 4300);
    let mut candidate = Fory::default();
    let result: Result<(), fory::Error> = register_fory_types(&mut candidate);
    result.expect("新候选应完成一次集中登记");
    let deadline = UNIX_EPOCH.checked_sub(Duration::from_nanos(100)).unwrap();
    let message = business_message(TRACE, "节点-A", deadline);
    let bytes = candidate.serialize(&message).unwrap();
    assert_wire_type_id(&bytes, 1_100_815);
    let decoded: SelectionMessage = candidate.deserialize(&bytes).unwrap();
    assert_business_content(decoded, TRACE, "节点-A", deadline);
}

/// 共表所有登记必须先完成：本例不在登记前编码HostEntry以探测候选。
pub(super) fn test_registration_preserves_shared_table_types_before_and_after() {
    require_normal_fory_errors();
    let mut candidate = Fory::default();
    candidate.register::<HostEntry>(4299).unwrap();
    register_fory_types(&mut candidate).unwrap();
    candidate.register::<HostTail>(4311).unwrap();
    let published = Arc::new(candidate);
    let codec = DefaultCodec::new(published.clone());
    assert!(Arc::ptr_eq(&*codec.default_codec(), &published));
    let bytes = codec.default_encode(&HostEntry { marker: 0xa1b2c3d4 }).unwrap();
    let before: HostEntry = codec.default_decode(bytes).unwrap();
    assert_eq!(before.marker, 0xa1b2c3d4);
    let bytes = codec.default_encode(&HostTail { value: -1_234_567_890 }).unwrap();
    let after: HostTail = codec.default_decode(bytes).unwrap();
    assert_eq!(after.value, -1_234_567_890);
    let bytes = codec.default_encode(&business_message(TRACE, "共表节点", UNIX_EPOCH)).unwrap();
    assert_wire_type_id(&bytes, 1_100_815);
    assert_business_content(codec.default_decode(bytes).unwrap(), TRACE, "共表节点", UNIX_EPOCH);
}

/// 每个预留编号使用全新候选，不在同表重复登记HostEntry或回退换号。
pub(super) fn test_registration_leaves_document_only_reserved_ids_unregistered() {
    require_normal_fory_errors();
    for reserved_id in 4301..=4310 {
        let mut candidate = Fory::default();
        register_fory_types(&mut candidate).unwrap();
        candidate.register::<HostEntry>(reserved_id).unwrap();
        let host_bytes = candidate.serialize(&HostEntry { marker: reserved_id }).unwrap();
        assert_wire_type_id(&host_bytes, (reserved_id << 8) + 15);
        let host: HostEntry = candidate.deserialize(&host_bytes).unwrap();
        assert_eq!(host.marker, reserved_id);
        let bytes = candidate.serialize(&business_message(TRACE, "预留号对照", UNIX_EPOCH)).unwrap();
        assert_wire_type_id(&bytes, 1_100_815);
        assert_business_content(candidate.deserialize(&bytes).unwrap(), TRACE, "预留号对照", UNIX_EPOCH);
    }
}

/// 非幂等误用边界：两次调用之间没有发布或编解码；Err后不检验候选保持原样。
pub(super) fn test_registration_duplicate_returns_actual_fory_error() {
    require_normal_fory_errors();
    let mut candidate = Fory::default();
    register_fory_types(&mut candidate).unwrap();
    let expected = format!("rs_struct:{:?} already registered", TypeId::of::<SelectionMessage>());
    assert_type_error(register_fory_types(&mut candidate), &expected);
}

/// 真正预占固定号的冲突；不以已登记本消息的重复错误替代数字冲突。
pub(super) fn test_registration_conflict_returns_actual_fory_error() {
    require_normal_fory_errors();
    let mut candidate = Fory::default();
    candidate.register::<Occupant>(4300).unwrap();
    assert_type_error(register_fory_types(&mut candidate), conflict_reason());
}

/// 仅模拟宿主冷路径的消费方式：库函数返回真实结果后，宿主才决定是否发布。
/// 不向候选补写本消息，不吞错/回滚/重试；失败候选在?退出时释放，旧表由宿主持有。
fn host_publish_candidate(codec: &DefaultCodec, mut candidate: Fory) -> Result<(), fory::Error> {
    register_fory_types(&mut candidate)?;
    candidate.register::<HostTail>(4311)?;
    codec.reset_fory(Arc::new(candidate));
    Ok(())
}

/// 旧表可用和成功对照排除“发布夹具从不工作”的假阳性；不声称库可控制外部发布者。
pub(super) fn test_registration_host_stops_publishing_failed_candidate() {
    require_normal_fory_errors();
    let mut old = Fory::default();
    old.register::<HostEntry>(4299).unwrap();
    let old = Arc::new(old);
    let codec = DefaultCodec::new(old.clone());
    let mut candidate = Fory::default();
    candidate.register::<Occupant>(4300).unwrap();
    assert_type_error(host_publish_candidate(&codec, candidate), conflict_reason());
    assert!(Arc::ptr_eq(&*codec.fory(), &old), "失败候选不得替换原发布槽");
    let bytes = codec.default_encode(&HostEntry { marker: 123 }).unwrap();
    let old_value: HostEntry = codec.default_decode(bytes).unwrap();
    assert_eq!(old_value.marker, 123);
    assert_unregistered_encode_rejected(&codec);
    // 新候选是另一实例，并非对失败候选重试；成功后应真正替换旧槽。
    let mut fresh = Fory::default();
    fresh.register::<HostEntry>(4299).unwrap();
    host_publish_candidate(&codec, fresh).unwrap();
    assert!(!Arc::ptr_eq(&*codec.fory(), &old));
    let bytes = codec.default_encode(&HostTail { value: -19 }).unwrap();
    let tail: HostTail = codec.default_decode(bytes).unwrap();
    assert_eq!(tail.value, -19);
    let bytes = codec.default_encode(&business_message(TRACE, "已发布新表", UNIX_EPOCH)).unwrap();
    assert_wire_type_id(&bytes, 1_100_815);
    assert_business_content(codec.default_decode(bytes).unwrap(), TRACE, "已发布新表", UNIX_EPOCH);
}

/// 两端独立候选，用不同宿主登记顺序排除偶然依赖全局序号；所有登记先于首次使用。
fn registered_shared_candidate(reverse_order: bool) -> Arc<Fory> {
    let mut candidate = Fory::default();
    if reverse_order {
        candidate.register::<HostTail>(4311).unwrap();
        register_fory_types(&mut candidate).unwrap();
        candidate.register::<HostEntry>(4299).unwrap();
    } else {
        candidate.register::<HostEntry>(4299).unwrap();
        register_fory_types(&mut candidate).unwrap();
        candidate.register::<HostTail>(4311).unwrap();
    }
    Arc::new(candidate)
}

/// 仅保存场景输入和独立规范化预期，不保存消息/codec或内部状态。
struct Sample {
    trace: [u8; 16],
    source: String,
    input_deadline: SystemTime,
    expected_deadline: SystemTime,
}

/// 共同两极、正负/零、稀疏Trace和跨短长缓冲的有界数据，不是压力或性能测量。
fn samples() -> Vec<Sample> {
    let lower = UNIX_EPOCH.checked_sub(Duration::from_secs(11_644_473_600)).unwrap();
    let upper = UNIX_EPOCH.checked_add(Duration::new(910_692_730_085, 477_580_700)).unwrap();
    let negative = UNIX_EPOCH.checked_sub(Duration::from_nanos(100)).unwrap();
    let positive = UNIX_EPOCH.checked_add(Duration::new(1, 100)).unwrap();
    let mut first = [0; 16];
    first[0] = 1;
    let mut last = [0; 16];
    last[15] = 1;
    #[cfg(target_os = "linux")]
    let (fractional, normalized) = (
        UNIX_EPOCH.checked_sub(Duration::from_nanos(101)).unwrap(),
        UNIX_EPOCH.checked_sub(Duration::from_nanos(200)).unwrap(),
    );
    #[cfg(not(target_os = "linux"))]
    let (fractional, normalized) = (negative, negative);
    vec![
        Sample { trace: TRACE, source: "  节点é/e\u{301}🦀\0 ".into(), input_deadline: lower, expected_deadline: lower },
        Sample { trace: first, source: " ".into(), input_deadline: negative, expected_deadline: negative },
        Sample { trace: last, source: "not-a-peer-id".into(), input_deadline: UNIX_EPOCH, expected_deadline: UNIX_EPOCH },
        Sample { trace: TRACE, source: "上界节点".into(), input_deadline: upper, expected_deadline: upper },
        Sample { trace: TRACE, source: "长来源/节点🦀".repeat(64), input_deadline: positive, expected_deadline: positive },
        Sample { trace: last, source: "负余数/节点".into(), input_deadline: fractional, expected_deadline: normalized },
    ]
}

/// 双向但同一种真实codec；每个方向重新构造消息，不让发送对象替代接收对象。
fn assert_independent_roundtrips<C>(left: &C, right: &C)
where
    C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>,
{
    for sample in samples() {
        for (sender, receiver) in [(left, right), (right, left)] {
            let message = business_message(sample.trace, &sample.source, sample.input_deadline);
            let bytes = sender.default_encode(&message).unwrap();
            assert_wire_type_id(&bytes, 1_100_815);
            drop(message);
            let received: SelectionMessage = receiver.default_decode(bytes).unwrap();
            assert_business_content(received, sample.trace, &sample.source, sample.expected_deadline);
        }
    }
}

/// 两个DefaultCodec必须实际指向两个独立、已经完成登记的表，不借用同一对象冒充两端。
pub(super) fn test_registration_default_codec_independent_representation_roundtrips() {
    require_normal_fory_errors();
    assert_unregistered_encode_rejected(&DefaultCodec::default());
    let left_table = registered_shared_candidate(false);
    let right_table = registered_shared_candidate(true);
    assert!(!Arc::ptr_eq(&left_table, &right_table));
    let left = DefaultCodec::new(left_table.clone());
    let right = DefaultCodec::new(right_table.clone());
    assert!(Arc::ptr_eq(&*left.default_codec(), &left_table));
    assert!(Arc::ptr_eq(&*right.default_codec(), &right_table));
    assert_independent_roundtrips(&left, &right);
}

/// 显式核对实际目标层及预绑定快照；所有Arc均归测试宿主，不扩张本库资源持有。
pub(super) fn test_registration_multi_layer_codec_independent_representation_roundtrips() {
    require_normal_fory_errors();
    let left_default = Arc::new(Fory::default());
    let right_default = Arc::new(Fory::default());
    let left_template = MultiLayerCodec::new(left_default.clone());
    let right_template = MultiLayerCodec::new(right_default.clone());
    // 真实缺层仍可能Ok且current_layer显示请求名：反证“层名/切换成功=表已登记”。
    assert!(!left_template.has_layer(LAYER));
    assert!(left_template.layer_fory(LAYER).is_none());
    let mut missing = left_template.clone();
    missing.switch_layer(LAYER).unwrap();
    assert_eq!(missing.current_layer().as_str(), LAYER);
    assert!(Arc::ptr_eq(&*missing.current_fory(), &left_default));
    assert_unregistered_encode_rejected(&missing);
    let left_table = registered_shared_candidate(false);
    let right_table = registered_shared_candidate(true);
    assert!(!Arc::ptr_eq(&left_table, &right_table));
    left_template.register_layer_fory(LAYER, left_table.clone());
    right_template.register_layer_fory(LAYER, right_table.clone());
    assert!(left_template.has_layer(LAYER));
    assert!(right_template.has_layer(LAYER));
    assert!(Arc::ptr_eq(&*left_template.layer_fory(LAYER).unwrap(), &left_table));
    assert!(Arc::ptr_eq(&*right_template.layer_fory(LAYER).unwrap(), &right_table));
    let mut left = left_template.clone();
    let mut right = right_template.clone();
    left.switch_layer(LAYER).unwrap();
    right.switch_layer(LAYER).unwrap();
    assert!(Arc::ptr_eq(&*left.default_codec(), &left_table));
    assert!(Arc::ptr_eq(&*right.default_codec(), &right_table));
    assert!(Arc::ptr_eq(&*left_template.current_fory(), &left_default));
    assert!(Arc::ptr_eq(&*right_template.current_fory(), &right_default));
    assert_independent_roundtrips(&left, &right);
}

/// 只消费真实派生与真实解码值，正常业务校验仍是唯一合法结果来源。
pub(super) fn test_registration_derived_default_does_not_bypass_validation() {
    require_normal_fory_errors();
    let mut candidate = Fory::default();
    register_fory_types(&mut candidate).unwrap();
    let codec = DefaultCodec::new(Arc::new(candidate));
    let value = SelectionMessage::fory_default();
    let bytes = codec.default_encode(&value).unwrap();
    assert_wire_type_id(&bytes, 1_100_815);
    let decoded: SelectionMessage = codec.default_decode(bytes).unwrap();
    assert!(matches!(validate_selection_message(decoded), Err(ProtocolError::InvalidTraceId)));
}

/// R1：两份独立非默认候选覆盖六项公开配置，布尔项的合法两值及非默认深度17/29均有预期。
/// compatible会联动share_meta，且兼容模式禁用版本检查，故分两组合法装配核对。
/// 全程不发布、不编解码、不捕获panic；只在登记返回Ok后检查配置，不保证失败候选原样。
pub(super) fn test_registration_preserves_non_default_host_configuration() {
    require_normal_fory_errors();
    let examples = [
        (
            Fory::default()
                .compatible(true)
                .xlang(true)
                .compress_string(true)
                .max_dyn_depth(17)
                .check_struct_version(false),
            (true, true, true, true, 17, false),
        ),
        (
            Fory::default()
                .compatible(false)
                .xlang(false)
                .compress_string(false)
                .max_dyn_depth(29)
                .check_struct_version(true),
            (false, false, false, false, 29, true),
        ),
    ];
    for (mut candidate, (compatible, share_meta, xlang, compress_string, max_dyn_depth, check_struct_version)) in examples {
        // 同一组逐项查询同时用于初态和成功后态，预期为场景字面量，不读取私有字段或修改候选。
        let assert_configuration = |fory: &Fory| {
            assert_eq!(fory.is_compatible(), compatible, "兼容模式必须保持宿主设定");
            assert_eq!(fory.is_share_meta(), share_meta, "元数据共享配置必须保持宿主设定");
            assert_eq!(fory.is_xlang(), xlang, "跨语言配置必须保持宿主设定");
            assert_eq!(fory.is_compress_string(), compress_string, "字符串压缩配置必须保持宿主设定");
            assert_eq!(fory.get_max_dyn_depth(), max_dyn_depth, "动态嵌套深度必须保持宿主设定");
            assert_eq!(fory.is_check_struct_version(), check_struct_version, "结构版本检查必须保持宿主设定");
        };
        assert_configuration(&candidate);
        register_fory_types(&mut candidate).expect("非默认配置的合法候选应成功完成集中登记");
        assert_configuration(&candidate);
    }
}
