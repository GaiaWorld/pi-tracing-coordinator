//! slice-protocol 测试实现：固定字面量为预期，真实SystemTime/业务类型为输入。
//! @api:src/protocol.rs @impl:src/protocol_inner.rs
//! @test:tests/protocol/protocol_test.rs
//! @design-doc:design/tracing-coordinator/07-design/call-chain.md#slice-protocol
//! 仅测试已冻结protocol壳；不调用生产芯，不启动codec、runtime、binding或记录器。

use crate::protocol::{
    deadline_from_100ns, deadline_to_100ns, selection_to_message, validate_selection_message,
    DeadlineError, ProtocolError, SelectionMessage,
};
use std::{
    error::Error,
    fmt,
    str::Utf8Error,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// 固定相对纪元输入；调用方提供已知可表示的时差，不读取当前时间。
fn at_offset(before: bool, seconds: u64, nanos: u32) -> SystemTime {
    let offset = Duration::new(seconds, nanos);
    let value = if before {
        UNIX_EPOCH.checked_sub(offset)
    } else {
        UNIX_EPOCH.checked_add(offset)
    };
    value.expect("固定平台输入应可构造；构造失败不是目标行为红线")
}

/// 跨平台的共同域下界，来自1601与1970之间11644473600秒的独立事实。
fn lower_bound() -> SystemTime {
    at_offset(true, 11_644_473_600, 0)
}

/// 共同域上界的独立秒/纳秒字面量，不调用被测反向转换产生预期。
fn upper_bound() -> SystemTime {
    at_offset(false, 910_692_730_085, 477_580_700)
}

/// 对齐输入验证正负符号、跨秒和纪元；不以生产算法重复计算预期。
pub(super) fn test_deadline_to_aligned_epoch_sides() {
    let examples = [
        (false, 0, 0, 0),
        (false, 0, 100, 1),
        (true, 0, 100, -1),
        (false, 0, 999_999_900, 9_999_999),
        (true, 0, 999_999_900, -9_999_999),
        (false, 1, 0, 10_000_000),
        (true, 1, 0, -10_000_000),
        (false, 1, 100, 10_000_001),
        (true, 1, 100, -10_000_001),
    ];
    for (before, seconds, nanos, expected) in examples {
        assert_eq!(
            deadline_to_100ns(at_offset(before, seconds, nanos)).unwrap(),
            expected
        );
    }
}

/// Linux保留纳秒，Windows构造时已丢弃余数，故不冒充Windows负余数证据。
#[cfg(target_os = "linux")]
pub(super) fn test_deadline_to_floors_fractional_ticks() {
    let examples = [
        (0, 1, 0, -1),
        (0, 99, 0, -1),
        (0, 101, 1, -2),
        (0, 199, 1, -2),
        (1, 1, 10_000_000, -10_000_001),
    ];
    for (seconds, nanos, positive, negative) in examples {
        assert_eq!(
            deadline_to_100ns(at_offset(false, seconds, nanos)).unwrap(),
            positive
        );
        assert_eq!(
            deadline_to_100ns(at_offset(true, seconds, nanos)).unwrap(),
            negative
        );
    }
}

/// 实际构造能力分别断言；Windows合法SystemTime域与协议域一致，不能伪造外侧对象。
pub(super) fn test_deadline_to_common_bounds_and_outside() {
    assert_eq!(
        deadline_to_100ns(lower_bound()).unwrap(),
        -116_444_736_000_000_000
    );
    assert_eq!(
        deadline_to_100ns(upper_bound()).unwrap(),
        9_106_927_300_854_775_807
    );
    #[cfg(target_os = "linux")]
    {
        for value in [
            lower_bound()
                .checked_sub(Duration::from_nanos(100))
                .unwrap(),
            upper_bound()
                .checked_add(Duration::from_nanos(100))
                .unwrap(),
            at_offset(false, i64::MAX as u64, 999_999_999),
            at_offset(true, 1_u64 << 63, 0),
        ] {
            assert!(matches!(
                deadline_to_100ns(value),
                Err(DeadlineError::OutOfRange)
            ));
        }
    }
    #[cfg(target_os = "windows")]
    {
        assert!(lower_bound()
            .checked_sub(Duration::from_nanos(100))
            .is_none());
        assert!(upper_bound()
            .checked_add(Duration::from_nanos(100))
            .is_none());
    }
}

/// 规范化发生在范围判断之前；上界不足100ns与下界之前1ns呈非对称结果。
#[cfg(target_os = "linux")]
pub(super) fn test_deadline_to_normalizes_before_range_check() {
    for nanos in 1..=99 {
        let input = upper_bound()
            .checked_add(Duration::from_nanos(nanos))
            .unwrap();
        assert_eq!(deadline_to_100ns(input).unwrap(), 9_106_927_300_854_775_807);
    }
    assert!(matches!(
        deadline_to_100ns(
            upper_bound()
                .checked_add(Duration::from_nanos(100))
                .unwrap()
        ),
        Err(DeadlineError::OutOfRange)
    ));
    assert!(matches!(
        deadline_to_100ns(lower_bound().checked_sub(Duration::from_nanos(1)).unwrap()),
        Err(DeadlineError::OutOfRange)
    ));
}

/// 两目标平台均能还原这些共同域计数；字面量时刻为独立权威预期。
pub(super) fn test_deadline_from_exact_counts() {
    let examples = [
        (-116_444_736_000_000_000, lower_bound()),
        (9_106_927_300_854_775_807, upper_bound()),
        (0, UNIX_EPOCH),
        (1, at_offset(false, 0, 100)),
        (-1, at_offset(true, 0, 100)),
        (9_999_999, at_offset(false, 0, 999_999_900)),
        (-9_999_999, at_offset(true, 0, 999_999_900)),
        (10_000_000, at_offset(false, 1, 0)),
        (-10_000_000, at_offset(true, 1, 0)),
        (10_000_001, at_offset(false, 1, 100)),
        (-10_000_001, at_offset(true, 1, 100)),
    ];
    for (count, expected) in examples {
        let actual = deadline_from_100ns(count).unwrap();
        assert_eq!(actual, expected, "count={count}");
        assert_eq!(deadline_to_100ns(actual).unwrap(), count);
    }
}

/// 全i64输入域的外侧代表；故意包含MIN，保护先检查范围与避免MIN直接取负。
pub(super) fn test_deadline_from_rejects_out_of_range() {
    for count in [
        i64::MIN,
        -116_444_736_000_000_001,
        9_106_927_300_854_775_808,
        i64::MAX,
    ] {
        assert!(
            matches!(deadline_from_100ns(count), Err(DeadlineError::OutOfRange)),
            "count={count}"
        );
    }
}

/// 100个余数是一个刻度的穷举，不是负载/性能测试；Windows只提供实际100ns输入。
pub(super) fn test_deadline_roundtrip_never_delays() {
    let anchors = [
        lower_bound(),
        at_offset(true, 1, 0),
        at_offset(true, 0, 100),
        UNIX_EPOCH,
        at_offset(false, 1, 0),
        upper_bound()
            .checked_sub(Duration::from_nanos(100))
            .unwrap(),
    ];
    #[cfg(target_os = "linux")]
    let remainders = 0..100;
    #[cfg(not(target_os = "linux"))]
    let remainders = 0..1;
    for anchor in anchors {
        for nanos in remainders.clone() {
            let input = anchor.checked_add(Duration::from_nanos(nanos)).unwrap();
            let count = deadline_to_100ns(input).unwrap();
            let restored = deadline_from_100ns(count).unwrap();
            assert!(restored <= input);
            assert!(input.duration_since(restored).unwrap() < Duration::from_nanos(100));
            assert_eq!(deadline_to_100ns(restored).unwrap(), count);
        }
    }
}

/// 非对称字节能够暴露端序颠倒、截断或重新生成Trace的错误。
const TRACE_BYTES: [u8; 16] = [
    1, 35, 69, 103, 137, 171, 205, 239, 254, 220, 186, 152, 118, 84, 50, 16,
];

/// 业务入参均通过真实pi_logger类型构造，不提供生产缺失行为。
fn selection(trace_id: [u8; 16], expires_at: SystemTime) -> pi_logger::TraceSelection {
    pi_logger::TraceSelection {
        trace_id: pi_logger::TraceId::from_bytes(trace_id),
        expires_at,
    }
}

/// 不同内容身份与两端期限独立核验消息字段；原来源释放后消息仍拥有字节。
pub(super) fn test_send_preserves_owned_business_content() {
    let sources = ["  节点é/e\u{301}🦀\0 ", " ", "not-a-peer-id", "发送者"];
    let mut first_nonzero = [0; 16];
    first_nonzero[0] = 1;
    let mut last_nonzero = [0; 16];
    last_nonzero[15] = 1;
    for trace_id in [TRACE_BYTES, first_nonzero, last_nonzero] {
        for raw in sources {
            for (expires_at, expected) in [
                (lower_bound(), -116_444_736_000_000_000),
                (at_offset(true, 0, 100), -1),
                (UNIX_EPOCH, 0),
                (at_offset(false, 1, 100), 10_000_001),
                (upper_bound(), 9_106_927_300_854_775_807),
            ] {
                let source = pi_logger::TraceNodeId::new(raw);
                let message =
                    selection_to_message(selection(trace_id, expires_at), &source).unwrap();
                drop(source);
                assert_eq!(message.trace_id, trace_id);
                assert_eq!(message.source, raw.as_bytes());
                assert_eq!(message.expires_at_100ns, expected);
            }
        }
    }
}

/// 同时非法时只观测首错；不通过mock/插桩改变实际期限调用顺序。
pub(super) fn test_send_rejection_priority() {
    let mut deadlines = vec![UNIX_EPOCH, lower_bound(), upper_bound()];
    if let Some(value) = lower_bound().checked_sub(Duration::from_nanos(100)) {
        deadlines.push(value);
    }
    if let Some(value) = upper_bound().checked_add(Duration::from_nanos(100)) {
        deadlines.push(value);
    }
    for expires_at in deadlines {
        for raw in ["", "发送者"] {
            let source = pi_logger::TraceNodeId::new(raw);
            assert!(matches!(
                selection_to_message(selection([0; 16], expires_at), &source),
                Err(ProtocolError::InvalidTraceId)
            ));
        }
        let empty = pi_logger::TraceNodeId::new("");
        assert!(matches!(
            selection_to_message(selection(TRACE_BYTES, expires_at), &empty),
            Err(ProtocolError::EmptyIdentity { field: "source" })
        ));
    }
}

/// 只有通过前两项才观察期限范围错；Windows没有合法SystemTime域外输入。
#[cfg(target_os = "linux")]
pub(super) fn test_send_maps_deadline_range_error() {
    let source = pi_logger::TraceNodeId::new("发送者");
    for value in [
        lower_bound().checked_sub(Duration::from_nanos(1)).unwrap(),
        upper_bound()
            .checked_add(Duration::from_nanos(100))
            .unwrap(),
    ] {
        assert!(matches!(
            selection_to_message(selection(TRACE_BYTES, value), &source),
            Err(ProtocolError::DeadlineOutOfRange)
        ));
    }
}

/// 发送层必须实际调用期限规范化，不保留Linux残余精度或采样当前时间。
#[cfg(target_os = "linux")]
pub(super) fn test_send_normalizes_deadline() {
    let source = pi_logger::TraceNodeId::new("发送者");
    for (expires_at, expected) in [
        (at_offset(false, 0, 99), 0),
        (at_offset(true, 0, 1), -1),
        (at_offset(true, 0, 101), -2),
        (
            upper_bound().checked_add(Duration::from_nanos(99)).unwrap(),
            9_106_927_300_854_775_807,
        ),
    ] {
        let message = selection_to_message(selection(TRACE_BYTES, expires_at), &source).unwrap();
        assert_eq!(message.expires_at_100ns, expected);
    }
}

/// 模拟标准宏已交付的消息数据；仅构造待校验表示，不手工构造合法交接结果。
fn message(trace_id: [u8; 16], source: &[u8], expires_at_100ns: i64) -> SelectionMessage {
    SelectionMessage {
        trace_id,
        source: source.to_vec(),
        expires_at_100ns,
    }
}

/// 用合法出口消费并检查原业务值，into_parts不单造字段镜像测试。
pub(super) fn test_receive_preserves_decision_and_source() {
    let mut first_nonzero = [0; 16];
    first_nonzero[0] = 1;
    let mut last_nonzero = [0; 16];
    last_nonzero[15] = 1;
    for trace_id in [TRACE_BYTES, first_nonzero, last_nonzero] {
        for raw in ["  节点é/e\u{301}🦀\0 ", " ", "not-a-peer-id"] {
            for (count, expected) in [
                (-116_444_736_000_000_000, lower_bound()),
                (-1, at_offset(true, 0, 100)),
                (0, UNIX_EPOCH),
                (10_000_001, at_offset(false, 1, 100)),
                (9_106_927_300_854_775_807, upper_bound()),
            ] {
                let decoded =
                    validate_selection_message(message(trace_id, raw.as_bytes(), count)).unwrap();
                let (selection, source) = decoded.into_parts();
                assert_eq!(selection.trace_id.to_bytes(), trace_id);
                assert_eq!(selection.expires_at, expected);
                assert_eq!(source.as_str(), raw);
            }
        }
    }
}

/// 每项来自UTF-8编码规则；标准库原因是独立参考，不复制协议检查算法。
pub(super) fn test_receive_rejects_malformed_utf8() {
    let examples: &[&[u8]] = &[
        &[0x80],
        &[0xff],
        &[0xc0, 0xaf],
        &[0xed, 0xa0, 0x80],
        &[0xf4, 0x90, 0x80, 0x80],
        &[b'A', 0xe2, 0x82],
        &[b'A', 0xe2, b'('],
    ];
    for raw in examples {
        let expected = std::str::from_utf8(raw).unwrap_err();
        match validate_selection_message(message(TRACE_BYTES, raw, 0)) {
            Err(ProtocolError::InvalidIdentityUtf8 { field, source }) => {
                assert_eq!(field, "source");
                assert_eq!(source.valid_up_to(), expected.valid_up_to());
                assert_eq!(source.error_len(), expected.error_len());
            }
            Err(error) => panic!("错误分类不符：{error:?}"),
            Ok(_) => panic!("不合法UTF-8不得产生合法交接结果"),
        }
    }
}

/// 输入按值移入；错误不返还消息/缓冲。消费由真实签名及无输入载荷错误共同约束。
pub(super) fn test_receive_rejection_priority() {
    for count in [
        i64::MIN,
        -116_444_736_000_000_001,
        0,
        9_106_927_300_854_775_808,
        i64::MAX,
    ] {
        for raw in [&[][..], &[0xff][..], "来源".as_bytes()] {
            assert!(matches!(
                validate_selection_message(message([0; 16], raw, count)),
                Err(ProtocolError::InvalidTraceId)
            ));
        }
        assert!(matches!(
            validate_selection_message(message(TRACE_BYTES, &[0xff], count)),
            Err(ProtocolError::InvalidIdentityUtf8 {
                field: "source",
                ..
            })
        ));
        assert!(matches!(
            validate_selection_message(message(TRACE_BYTES, &[], count)),
            Err(ProtocolError::EmptyIdentity { field: "source" })
        ));
    }
}

/// 所有i64计数可作为解码字段交付；业务入口必须先判协议域而非平台操作。
pub(super) fn test_receive_rejects_illegal_deadlines() {
    for count in [
        i64::MIN,
        -116_444_736_000_000_001,
        9_106_927_300_854_775_808,
        i64::MAX,
    ] {
        assert!(matches!(
            validate_selection_message(message(TRACE_BYTES, "来源".as_bytes(), count)),
            Err(ProtocolError::DeadlineOutOfRange)
        ));
    }
}

/// 只组合真实协议出口，不补codec/传输或记录器；再次发送不得再改变期限。
pub(super) fn test_receive_sent_message_roundtrip() {
    #[cfg(target_os = "linux")]
    let expires_at = at_offset(true, 0, 101);
    #[cfg(not(target_os = "linux"))]
    let expires_at = at_offset(true, 0, 200);
    let source = pi_logger::TraceNodeId::new("  本跳/节点🦀 ");
    let sent = selection_to_message(selection(TRACE_BYTES, expires_at), &source).unwrap();
    let expected_count = sent.expires_at_100ns;
    let expected_source = sent.source.clone();
    let (received, received_source) = validate_selection_message(sent).unwrap().into_parts();
    assert_eq!(received.trace_id.to_bytes(), TRACE_BYTES);
    assert_eq!(received_source.as_str(), source.as_str());
    let again = selection_to_message(received, &received_source).unwrap();
    assert_eq!(again.trace_id, TRACE_BYTES);
    assert_eq!(again.expires_at_100ns, expected_count);
    assert_eq!(again.source, expected_source);
    assert_eq!(again.expires_at_100ns, -2);
}

/// 真实业务入口生成UTF-8错误，避免把直接构造错误当完整接收证据。
fn invalid_source_error() -> ProtocolError {
    let mut raw = "不要泄露/来源秘密".as_bytes().to_vec();
    raw.push(0xff);
    match validate_selection_message(message(TRACE_BYTES, &raw, 0)) {
        Err(error) => error,
        Ok(_) => panic!("非法来源不能产生合法结果"),
    }
}

/// NotRepresentable在当前两平台共同域无可达输入；直接构造仅证明trait分类，不伪称平台失败。
fn errors() -> [ProtocolError; 5] {
    [
        ProtocolError::InvalidTraceId,
        ProtocolError::EmptyIdentity { field: "source" },
        invalid_source_error(),
        ProtocolError::DeadlineOutOfRange,
        ProtocolError::DeadlineNotRepresentable,
    ]
}

/// 只保护语义关键词及分类区分，不锁死完整标点/句式；UTF-8原因来自实际保存值。
pub(super) fn test_error_display_classifies_actual_failure() {
    let errors = errors();
    let texts: Vec<String> = errors.iter().map(ToString::to_string).collect();
    assert!(texts[0].contains("TraceId"));
    assert!(
        texts[0].contains("不能")
            || texts[0].contains("无效")
            || texts[0].to_lowercase().contains("invalid")
    );
    assert!(texts[1].contains("source"));
    assert!(texts[1].contains("空") || texts[1].to_lowercase().contains("empty"));
    assert!(texts[2].contains("source") && texts[2].contains("UTF-8"));
    assert!(
        texts[2].contains("不是合法")
            || texts[2].contains("非法")
            || texts[2].contains("无效")
            || texts[2].to_lowercase().contains("invalid")
    );
    if let ProtocolError::InvalidIdentityUtf8 { source, .. } = &errors[2] {
        assert!(texts[2].contains(&source.to_string()));
    } else {
        panic!("第三项须为实际UTF-8错误");
    }
    assert!(texts[3].contains("期限") || texts[3].to_lowercase().contains("deadline"));
    assert!(
        texts[3].contains("超出")
            || texts[3].contains("越界")
            || texts[3].to_lowercase().contains("out of range")
    );
    assert!(texts[4].contains("期限") || texts[4].to_lowercase().contains("deadline"));
    assert!(
        texts[4].contains("无法")
            || texts[4].contains("不能")
            || texts[4].contains("不可")
            || texts[4].to_lowercase().contains("not represent")
            || texts[4].to_lowercase().contains("unrepresentable")
    );
    assert!(texts[4].contains("表示") || texts[4].to_lowercase().contains("represent"));
    for left in 0..texts.len() {
        for right in (left + 1)..texts.len() {
            assert_ne!(texts[left], texts[right]);
        }
    }
}

/// 业务错误不能滞留/回显原始非法来源，保留原因不等于保留输入。
pub(super) fn test_error_display_does_not_echo_invalid_input() {
    let error = invalid_source_error();
    let text = error.to_string();
    assert!(!text.contains("不要泄露"));
    assert!(!text.contains("来源秘密"));
    match error {
        ProtocolError::InvalidIdentityUtf8 { source, .. } => {
            assert!(text.contains(&source.to_string()))
        }
        other => panic!("错误分类不符：{other:?}"),
    }
}

/// 只在格式化输出边界拒绝写入，不mock协议内部；失败后再次调用会立即暴露继续写的错误。
struct LimitedWriter {
    remaining: usize,
    failed: bool,
    output: String,
}

impl fmt::Write for LimitedWriter {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        assert!(!self.failed, "目标已经失败后不应继续写入");
        if text.len() > self.remaining {
            self.failed = true;
            return Err(fmt::Error);
        }
        self.remaining -= text.len();
        self.output.push_str(text);
        Ok(())
    }
}

/// fmt::Error为无载荷值，精确Err匹配是原样传播的可观察证据，另有成功对照。
pub(super) fn test_error_display_propagates_writer_failure() {
    for error in errors() {
        let expected = error.to_string();
        // 穷举每个不足输出字节数的容量，覆盖前缀、静态字段及UTF-8实际原因写出失败。
        for capacity in 0..expected.len() {
            let mut writer = LimitedWriter {
                remaining: capacity,
                failed: false,
                output: String::new(),
            };
            assert_eq!(
                fmt::write(&mut writer, format_args!("{error}")),
                Err(fmt::Error)
            );
            assert!(writer.failed);
        }
        let mut writer = LimitedWriter {
            remaining: 1024,
            failed: false,
            output: String::new(),
        };
        assert_eq!(fmt::write(&mut writer, format_args!("{error}")), Ok(()));
        assert!(!writer.failed);
        assert_eq!(writer.output, error.to_string());
    }
}

/// 同一实际原因地址验证source没有跳层、包装、复制或临时构造。
pub(super) fn test_error_source_borrows_actual_utf8_cause() {
    let error = invalid_source_error();
    let saved = match &error {
        ProtocolError::InvalidIdentityUtf8 { source, .. } => source,
        other => panic!("错误分类不符：{other:?}"),
    };
    let cause = error.source().expect("UTF-8分类必须有实际原因");
    let actual = cause
        .downcast_ref::<Utf8Error>()
        .expect("必须直接是Utf8Error");
    assert!(std::ptr::eq(actual, saved));
    assert_eq!(actual.valid_up_to(), "不要泄露/来源秘密".len());
    assert_eq!(actual.error_len(), Some(1));
    assert!(cause.source().is_none());
}

/// 其余四类只表达当前分类，没有可合法借出的下层原因。
pub(super) fn test_error_source_is_none_for_other_kinds() {
    for error in [
        ProtocolError::InvalidTraceId,
        ProtocolError::EmptyIdentity { field: "source" },
        ProtocolError::DeadlineOutOfRange,
        ProtocolError::DeadlineNotRepresentable,
    ] {
        assert!(error.source().is_none());
        assert!(error.source().is_none());
    }
}

// slice-registration：仅在tests侧复用现有协议测试作用域，不修改生产挂载或冻结壳。
#[path = "registration_test.rs"]
mod registration_tests;

// slice-binding：只在tests侧复用既有crate内入口，行为断言只经冻结binding壳。
#[path = "../binding/binding_test.rs"]
mod binding_tests;
// slice-client：仅tests侧挂载，行为经冻结client壳及真实宿主依赖出口。
#[path = "../client/client_test.rs"]
mod client_tests;

// slice-service：仅tests侧复用既有入口，真实宏与框架经冻结service壳驱动。
#[path = "../service/service_test.rs"]
mod service_tests;
