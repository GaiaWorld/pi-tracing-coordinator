//! slice-client 测试声明：每条Scenario只透传同名实现。
//! @api:src/client.rs @impl:src/client_inner.rs
//! @test:tests/client/client_test.rs
//! @design-doc:design/tracing-coordinator/07-design/call-chain.md#slice-client
//! 仅tests侧挂载，不改变冻结壳/可见性。内部属性保留实现后独立审查。
//! 固定目标A=QmYwAPJzv5CZsnAzt8auVZRnGi2CkEJjQd2B5Hc9EcAm7H，B末字符为J。
//! 依赖校准不是client通过；前置new失败不是其它芯RED。
//! 共同具体值：Trace=0123456789abcdeffedcba9876543210，普通期限=Unix后1秒100ns。
//! 显式source="  显式/节点🦀\0 "；保存local="  保存/节点é/e\u{301}🦀\0 "，空白/NUL/组合字符均原样。
//! 警告固定target=pi_tracing_coordinator::client，WARN，remote原文、service_port=4091、error=实际Display。
//! 需要暂控的宿主Deref/传输/订阅者只是合法依赖出口观察，不实现本库功能，也不模拟远端完成。

#[path = "client_test_inner.rs"]
mod client_test_inner;

/// 契约：slice-client / 真实codec和终端依赖校准。
/// @api:src/client.rs @impl:src/client_inner.rs @edgecase
/// Scenario: 真实codec和终端依赖校准。
/// Given 真实登记4300的两份表、DefaultCodec和ServiceTerminal，目标A/B末尾7H/7J。
/// When 宿主直接向4091提交，再取得未登记编码及SendPortFailed(17)/提交CodecError的真实原因，并对共用阶段断言交换Encode/Submit标签。
/// Then 拥有式消息保真，端口错误真实映射为RemoteServiceNotFound(A,4091)，零RPC重连；原因隔离后正确标签不拘前后均通过，两种互换都准确拒绝，不计client产品证据。
#[test]
fn test_client_fixture_real_codec_terminal_controls() {
    client_test_inner::test_client_fixture_real_codec_terminal_controls();
}

/// 契约：slice-client / 宿主日志观察器启用及过滤校准。
/// @api:src/client.rs @impl:src/client_inner.rs @edgecase
/// Scenario: 宿主日志观察器启用及过滤校准。
/// Given 线程局部订阅者，析构计数2。
/// When 宿主先发固定client target的remote/4091/校准原因警告，再发client_inner的WARN、库根INFO及依赖/相似库名前缀噪声，最后关闭观察发对照。
/// Then 首条固定target/三字段精确；前后三条本库事件均计入同一集合，错误target可见、级别及析构计数2保真，依赖/相似库名及关闭后事件不记录；不计notify证据。
#[test]
fn test_client_fixture_warning_observer_controls() {
    client_test_inner::test_client_fixture_warning_observer_controls();
}

/// 契约：slice-client / 构造只组合并尊重资源归属。
/// @api:src/client.rs @impl:src/client_inner.rs @critical
/// Scenario: 构造不提前验证或发送，并只强持codec。
/// Given Closed binding、未登记DefaultCodec、存活终端和空local。
/// When new后依次释放宿主终端、client，同时保留binding及外部Weak观察。
/// Then 构造无Deref路由提交、不拒绝空local；终端立即析构1且Weak失效，codec在client存活时可升级、client释放后失效；不调用其它client芯或写赋值镜像。
#[test]
fn test_client_new_is_lazy_and_respects_resource_ownership() {
    client_test_inner::test_client_new_is_lazy_and_respects_resource_ownership();
}

/// 契约：slice-client / 显式发送恰一次且内容来源保真。
/// @api:src/client.rs @impl:src/client_inner.rs @critical
/// Scenario: 显式发送恰一次且内容来源保真。
/// Given 未绑定开放binding、真实DefaultCodec、非对称Trace 0123456789abcdeffedcba9876543210、Unix后1秒100ns、来源含首尾空白和NUL。
/// When 向目标A发送一次后释放remote/source/client。
/// Then 一份目标A/4091拥有式消息，独立解码保持Trace、显式原始来源和期限；零日志/RPC重连，不把本地Ok解释为远端完成。
#[test]
fn test_client_explicit_send_preserves_content_once() {
    client_test_inner::test_client_explicit_send_preserves_content_once();
}

/// 契约：slice-client / 多层发送使用保存的预绑定codec。
/// @api:src/client.rs @impl:src/client_inner.rs @critical
/// Scenario: 多层发送使用保存的预绑定codec。
/// Given 默认层未登记，tracing-coordinator层登记4300并已switch完成。
/// When 向目标B发送一次。
/// Then 目标B/4091一份消息独立解码保真，current_layer不变，不靠默认层替代成功。
#[test]
fn test_client_multi_layer_uses_saved_prebound_codec() {
    client_test_inner::test_client_multi_layer_uses_saved_prebound_codec();
}

/// 契约：slice-client / 重复调用分别提交不去重。
/// @api:src/client.rs @impl:src/client_inner.rs @edgecase
/// Scenario: 重复调用分别提交不去重。
/// Given 真实codec及已绑定None的开放binding。
/// When 目标A/来源A、目标A/来源A、目标B/来源B各发送一次。
/// Then 精确三次有序提交且内容对应，零重连/RPC，无跨调用缓存。
#[test]
fn test_client_repeated_calls_are_independent_attempts() {
    client_test_inner::test_client_repeated_calls_are_independent_attempts();
}

/// 契约：slice-client / 空local不影响显式source，通知才拒绝。
/// @api:src/client.rs @impl:src/client_inner.rs @edgecase
/// Scenario: 空local不影响显式source，通知才拒绝。
/// Given 保存空local，合法目标A和选择。
/// When 先显式提供非空来源发送，再notify。
/// Then 只一份显式来源消息；通知返回anyhow内CoordinationSendError及Protocol EmptyIdentity(source)，一条警告而零新增提交。
#[test]
fn test_client_empty_local_is_deferred_and_explicit_source_wins() {
    client_test_inner::test_client_empty_local_is_deferred_and_explicit_source_wins();
}

/// 契约：slice-client / 成功notify采用保存local且无日志。
/// @api:src/client.rs @impl:src/client_inner.rs @critical
/// Scenario: 成功notify采用保存local且无日志。
/// Given local含空白、组合字符、NUL，合法Trace和期限。
/// When notify目标B一次。
/// Then 一份目标B消息来源精确为保存local，Trace/期限不变，零本库事件和其它传输动作。
#[test]
fn test_client_notifier_uses_local_and_has_no_success_warning() {
    client_test_inner::test_client_notifier_uses_local_and_has_no_success_warning();
}

/// 契约：slice-client / 关闭或Owner消失先于后续错误。
/// @api:src/client.rs @impl:src/client_inner.rs @negative
/// Scenario: 关闭或Owner消失先于后续错误。
/// Given 两组独立装配分别close或释放Owner，终端已死、目标0、全零Trace、空source及未登记表。
/// When 各显式发送一次。
/// Then 直接BindingError Closed，零路由提交，各析构1；不合成中毒冒充真实路径。
#[test]
fn test_client_binding_rejection_precedes_all_later_errors() {
    client_test_inner::test_client_binding_rejection_precedes_all_later_errors();
}

/// 契约：slice-client / 终端失效先于解析及业务。
/// @api:src/client.rs @impl:src/client_inner.rs @negative
/// Scenario: 终端失效先于解析及业务。
/// Given Owner开放但终端最后宿主引用释放，目标0与非法业务。
/// When 显式发送。
/// Then Weak失效、终端阶段显示、source=None、零路由提交。
#[test]
fn test_client_terminal_unavailable_precedes_parse_and_protocol() {
    client_test_inner::test_client_terminal_unavailable_precedes_parse_and_protocol();
}

/// 契约：slice-client / 目标解析先错且保留原始链。
/// @api:src/client.rs @impl:src/client_inner.rs @negative
/// Scenario: 目标解析先错且保留原始链。
/// Given Owner和终端可用、Trace全零及source空；目标Examples为空、0、1、空白中文及前导空白合法PeerId。
/// When 逐项原样发送。
/// Then 目标阶段错误，直接原因类型、文本、下层链与真实FromStr一致，不trim，零路由提交。
#[test]
fn test_client_target_parse_precedes_protocol_and_preserves_causes() {
    client_test_inner::test_client_target_parse_precedes_protocol_and_preserves_causes();
}

/// 契约：slice-client / 业务首错先于真实编码失败。
/// @api:src/client.rs @impl:src/client_inner.rs @negative
/// Scenario: 业务首错先于真实编码失败。
/// Given 有效目标、未登记真实codec；合法期限及Linux可构造域外期限。
/// When 分别发送全零Trace/空source与有效Trace/空source。
/// Then 依次Protocol InvalidTraceId及EmptyIdentity(source)，不被期限/Encode/Submit抢先，零路由提交。
#[test]
fn test_client_protocol_priority_precedes_real_encoding_failure() {
    client_test_inner::test_client_protocol_priority_precedes_real_encoding_failure();
}

/// 契约：slice-client / 域外期限先于编码拒绝。
/// @api:src/client.rs @impl:src/client_inner.rs @negative
/// Scenario: 域外期限先于编码拒绝。
/// Given Linux可构造Unix前11644473601秒、有效Trace和source、未登记表。
/// When 向合法目标发送。
/// Then 直接Protocol DeadlineOutOfRange且零提交，不伪造Windows域外对象。
#[cfg(target_os = "linux")]
#[test]
fn test_client_protocol_deadline_error_precedes_encoding() {
    client_test_inner::test_client_protocol_deadline_error_precedes_encoding();
}

/// 契约：slice-client / 真实编码失败保留原因且零提交。
/// @api:src/client.rs @impl:src/client_inner.rs @negative
/// Scenario: 真实编码失败保留原因且零提交。
/// Given FORY_PANIC_ON_ERROR=false，所有前置合法，DefaultCodec未登记。
/// When 显式发送。
/// Then Encode直接ServiceError CodecError含not found in type_id_index且与依赖原因一致、下层源None；零路由提交日志。
#[test]
fn test_client_real_encode_failure_has_no_submission() {
    client_test_inner::test_client_real_encode_failure_has_no_submission();
}

/// 契约：slice-client / 提交保留真实终端错误且不重试。
/// @api:src/client.rs @impl:src/client_inner.rs @negative
/// Scenario: 提交保留真实终端错误且不重试。
/// Given 五组出口先接管字节再返回RuntimeError、CodecError、端口错、未连接或关闭。
/// When 每组显式发送一次。
/// Then 各仅一次提交、Submit直接原因精确保留含终端端口映射，字节保真仍存在，零重连/RPC日志；不推断外部零效果。
#[test]
fn test_client_submit_preserves_terminal_errors_and_never_retries() {
    client_test_inner::test_client_submit_preserves_terminal_errors_and_never_retries();
}

/// 契约：slice-client / 自目标沿真实本地路由。
/// @api:src/client.rs @impl:src/client_inner.rs @edgecase
/// Scenario: 自目标沿真实本地路由。
/// Given 终端本地身份A，无本地runtime，其余合法。
/// When 向目标A发送。
/// Then Submit LocalRuntimeMissing(A)，一次路由查询且远端传输零；无自发送拒绝。
#[test]
fn test_client_self_target_uses_terminal_local_routing() {
    client_test_inner::test_client_self_target_uses_terminal_local_routing();
}

/// 契约：slice-client / 期限向过去规范化经真实编码保真。
/// @api:src/client.rs @impl:src/client_inner.rs @edgecase
/// Scenario: 期限向过去规范化经真实编码保真。
/// Given Linux期限Examples为Unix后99ns、Unix前1ns及前101ns。
/// When 逐项发送并用独立codec解码。
/// Then 三次期限为Unix、前100ns及前200ns，Trace和source不变，不采样now续期。
#[cfg(target_os = "linux")]
#[test]
fn test_client_normalized_deadlines_survive_real_encoding() {
    client_test_inner::test_client_normalized_deadlines_survive_real_encoding();
}

/// 契约：slice-client / 六类错误显示阶段及已有原因。
/// @api:src/client.rs @impl:src/client_inner.rs @critical
/// Scenario: 六类错误显示阶段及已有原因。
/// Given 合法调用取得Binding/终端/解析/Protocol/Encode/Submit六类实际错误。
/// When 逐项Display。
/// Then 六阶段在隔离完整真实原因后的说明仍准确且文本互异；原因不能提供阶段关键词，无Trace或操作号、不增日志提交，不冻结完整句式。
#[test]
fn test_client_error_display_reports_six_stages_and_causes() {
    client_test_inner::test_client_error_display_reports_six_stages_and_causes();
}

/// 契约：slice-client / 格式化任意写失败立即传播。
/// @api:src/client.rs @impl:src/client_inner.rs @negative
/// Scenario: 格式化任意写失败立即传播。
/// Given 合法调用的六类错误、容量0到完整长度减1的写入器及足够容量对照。
/// When 逐项Display写出。
/// Then 不足容量均Err(fmt::Error)且首次失败后无继续写，完整容量成功，零新增日志提交。
#[test]
fn test_client_error_display_propagates_all_writer_failures() {
    client_test_inner::test_client_error_display_propagates_all_writer_failures();
}

/// 契约：slice-client / source保持直接类型化原因。
/// @api:src/client.rs @impl:src/client_inner.rs @critical
/// Scenario: source保持直接类型化原因。
/// Given 合法调用六类错误和独立原因预期。
/// When 重复取source。
/// Then 有原因阶段两次均Some且借用地址相同，终端两次均None，任何混合结果拒绝；Binding/Protocol/ServiceError类型值精确、解析原始链保留、不伪造Fory链，不新增本库日志或提交。
#[test]
fn test_client_error_source_keeps_direct_typed_causes() {
    client_test_inner::test_client_error_source_keeps_direct_typed_causes();
}

/// 契约：slice-client / 六阶段通知失败各一次警告并保持错误。
/// @api:src/client.rs @impl:src/client_inner.rs @negative
/// Scenario: 六阶段通知失败各一次警告并保持错误。
/// Given 六组独立真实装配和启用的宿主线程局部观察器。
/// When 各notify一次触发相应失败。
/// Then 各有可下转型发送错误及准确原因、唯一WARN和固定target及remote/4091/error字段，无Trace操作号；仅Submit传一份字节不重试。
#[test]
fn test_client_notifier_failure_warns_once_and_keeps_error() {
    client_test_inner::test_client_notifier_failure_warns_once_and_keeps_error();
}

/// 契约：slice-client / 空local通知保持六阶段首错。
/// @api:src/client.rs @impl:src/client_inner.rs @negative
/// Scenario: 空local通知保持六阶段首错。
/// Given 三组空local分别Closed、死终端或坏目标0。
/// When 各notify一次。
/// Then 分别Binding、终端、解析错而非空source，各一条匹配警告且零提交。
#[test]
fn test_client_notifier_empty_local_preserves_stage_precedence() {
    client_test_inner::test_client_notifier_empty_local_preserves_stage_precedence();
}

/// 契约：slice-client / 过滤日志不改变返回及提交。
/// @api:src/client.rs @impl:src/client_inner.rs @edgecase
/// Scenario: 过滤日志不改变返回及提交。
/// Given 观察器禁用、出口接管字节后RuntimeError。
/// When notify目标A一次。
/// Then 零记录但返回真实类型错误，只有一份消息source为local，不吞错重试。
#[test]
fn test_client_notifier_filtered_warning_still_returns_typed_error() {
    client_test_inner::test_client_notifier_filtered_warning_still_returns_typed_error();
}

/// 契约：slice-client / notifier只延长client和codec。
/// @api:src/client.rs @impl:src/client_inner.rs @critical
/// Scenario: notifier只延长client和codec。
/// Given 外部notifier持client、codec已移入，Owner和宿主终端后释放。
/// When 通知一次并释放最后notifier，保留错误日志。
/// Then 终端Weak失效析构1，Binding Closed且零提交；codec在notifier存活时可升级、最终失效，不声称TLS回收。
#[test]
fn test_client_notifier_holds_codec_but_not_terminal_or_binding() {
    client_test_inner::test_client_notifier_holds_codec_but_not_terminal_or_binding();
}

/// 契约：slice-client / 成功及全部失败返回释放临时终端。
/// @api:src/client.rs @impl:src/client_inner.rs @critical
/// Scenario: 成功及全部失败返回释放临时终端。
/// Given 六阶段失败及成功各独立装配，client与错误保留。
/// When 发送返回后释放宿主终端。
/// Then 返回后只余宿主强引用，释放后各Weak失效析构1；不以计数替代锁外审查。
#[test]
fn test_client_all_returns_release_temporary_terminal() {
    client_test_inner::test_client_all_returns_release_temporary_terminal();
}

/// 契约：slice-client / 失败先释放最后临时终端再警告。
/// @api:src/client.rs @impl:src/client_inner.rs @critical
/// Scenario: 失败先释放最后临时终端再警告。
/// Given 宿主Deref释放自身常驻终端，临时Arc推进真实提交后RuntimeError。
/// When notify一次。
/// Then 一次提交后Weak失效析构1，唯一警告事件已观测析构1，返回类型错误；独立进程20秒截止。
#[test]
fn test_client_notifier_releases_last_terminal_before_warning() {
    client_test_inner::test_client_notifier_releases_last_terminal_before_warning();
}

/// 契约：slice-client / 准入后关闭先返回本次仍收尾。
/// @api:src/client.rs @impl:src/client_inner.rs @critical
/// Scenario: 准入后关闭先返回本次仍收尾。
/// Given 独立进程中发送暂停真实Deref窗口，等待最多3秒，另一线程close。
/// When close返回后释放发送窗口再发第二次。
/// Then 首个一次提交成功、第二次Binding Closed不增提交，线程均回收，总截止20秒。
#[test]
fn test_client_admitted_send_finishes_after_close() {
    client_test_inner::test_client_admitted_send_finishes_after_close();
}

/// 契约：slice-client / 宿主Deref异常展开且不遗留资源。
/// @api:src/client.rs @impl:src/client_inner.rs @edgecase
/// Scenario: 宿主Deref异常展开且不遗留资源。
/// Given Deref抛出指定宿主Deref测试panic。
/// When 显式发送，仅测试边界catch_unwind。
/// Then 精确宿主panic而非TODO/Err，零提交、临时引用释放、Owner可关闭、宿主释放后Weak失效。
#[test]
fn test_client_dependency_panic_unwinds_without_retaining_terminal() {
    client_test_inner::test_client_dependency_panic_unwinds_without_retaining_terminal();
}

/// 契约：slice-client / subscriber异常不改普通错误或重试。
/// @api:src/client.rs @impl:src/client_inner.rs @edgecase
/// Scenario: subscriber异常不改普通错误或重试。
/// Given 终端接管后Err，宿主观察警告后抛出指定subscriber测试panic。
/// When notify并由测试边界观察展开。
/// Then 精确宿主panic、一条警告和一次既有提交、临时终端释放、无额外动作，不要求继续返回包装错误。
#[test]
fn test_client_subscriber_panic_is_not_converted_or_retried() {
    client_test_inner::test_client_subscriber_panic_is_not_converted_or_retried();
}

/// 契约：slice-client / 最后终端析构异常不回滚提交。
/// @api:src/client.rs @impl:src/client_inner.rs @edgecase
/// Scenario: 最后终端析构异常不回滚提交。
/// Given Deref释放宿主最后引用、真实提交成功、最后终端Drop指定panic。
/// When 显式发送并由测试边界观察展开。
/// Then 精确Drop panic、一次提交、析构1且Weak失效，无伪装Result或回滚；隔离20秒截止。
#[test]
fn test_client_last_terminal_drop_panic_keeps_submitted_effect() {
    client_test_inner::test_client_last_terminal_drop_panic_keeps_submitted_effect();
}
