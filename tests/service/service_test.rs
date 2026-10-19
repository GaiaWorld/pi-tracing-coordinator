//! slice-service声明：自足Scenario与同名实现一一对应，声明只透传。
//! @api:src/service.rs @impl:src/service_inner.rs
//! @test:tests/service/service_test.rs
//! @design-doc:design/tracing-coordinator/07-design/call-chain.md#slice-service
//! 取证与未闭合门禁：[测试证据](../../docs/SLICE_SERVICE_TEST_EVIDENCE.md#slice-service-test-evidence)。
//! 每场景独立进程，仅装一种适配；panic探针即使框架捕获也拒绝意外panic。
//! shutdown只等待采集/导出worker，通知统计是关闭时观察；精确通知全集/来源绝不回送待独立源码审查。
//! 完整coordinator安装归第6块。

#[path = "service_test_inner.rs"]
mod service_test_inner;

/// 契约：slice-service / 真实表示及错误夹具校准。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: 真实表示及错误夹具校准。
/// Given 同号4300的独立远端结构和生产消息，Trace=0123456789abcdeffedcba9876543210及16字节均0x82/原始source="  节点é/e\u{301}🦀\0 "/-100ns及纪元。
/// When 校准副作用后被捕获的标记panic仍可见；对比DefaultCodec编码字节，再由Default/Multi解码，并触发空输入和无目标/default预绑定。
/// Then 标记panic精确记录并消费后探针归零；两编码字节及两codec接收内容精确一致、空输入CodecError、缺层CodecLayerNotFound，不宣称Multi编码或反解PeerMessage，不算service实现GREEN。
#[test]
fn test_service_fixture_wire_representation_and_error_controls() {
    service_test_inner::test_service_fixture_wire_representation_and_error_controls();
}

/// 契约：slice-service / 真实记录器和日志观察校准。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: 真实记录器和日志观察校准。
/// Given 真实start记录器（local=本地/服务-B），Trace=0123456789abcdeffedcba9876543210重复2次及独立16字节均0x83一次。
/// When 真实发布后排空，另用16字节均0x84的Trace登记原始来源及观察邻居并发布，再产生正确/错误本库target及依赖对照日志。
/// Then 3次发布最终选择2，不冒充调用数；独立通知仅交观察/节点-C且Trace/期限保真，选择1/通知1；捕获本库3日志含错误INFO，排除依赖/相似前缀；固定message/category成对接受；raw_source字节/payload Trace/message转义来源三对共6事件均被拒绝；禁用0，导出0。通知精确全集仍待源码审查。
#[test]
fn test_service_fixture_recorder_and_observer_controls() {
    service_test_inner::test_service_fixture_recorder_and_observer_controls();
}

/// 契约：slice-service / 已关闭弱句柄可组合context。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: 已关闭弱句柄可组合context。
/// Given 已关闭binding句柄，空容器和runtime。
/// When new后释放Owner，再由宿主插入context。
/// Then 不升级检查开放性、不提前发布或安装，显式插入后得到同Arc；无升级/锁内部属性另审。
#[test]
fn test_service_context_accepts_closed_handle_without_installing() {
    service_test_inner::test_service_context_accepts_closed_handle_without_installing();
}

/// 契约：slice-service / default：静态身份及元信息且不提前安装。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: default：静态身份及元信息且不提前安装。
/// Given 空runtime，完整类型表及对应getter。
/// When 重复取得静态借用，再显式register。
/// Then 同一static；取得时无context/注册/事件/本库日志，register后Registered、id=svc://local/observability/tracing/coordinator、port4091、layer=tracing-coordinator、Send、version0.1.0；未init返回ServiceNotRunning。
#[test]
fn test_service_default_static_identity_and_metadata() {
    service_test_inner::test_service_default_static_identity_and_metadata();
}

/// 契约：slice-service / default：缺上下文保留旧状态。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: default：缺上下文保留旧状态。
/// Given 真实Registered服务，contexts缺CoordinationServiceContext。
/// When registry.init。
/// Then 实际BindOnlyReadNotFound(4091,context,CoordinationServiceContext)，仍Registered及原元信息，仅原注册事件，不形成Failed。
#[test]
fn test_service_default_missing_context_preserves_registered() {
    service_test_inner::test_service_default_missing_context_preserves_registered();
}

/// 契约：slice-service / default：合法bind_params保留context且codec未绑定。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: default：合法bind_params保留context且codec未绑定。
/// Given new创建context并注入，static/runtime对应同一codec。
/// When 同一static.bind_params，再poll初始化前handler，释放容器/runtime/Owner，最后释放params。
/// Then 精确codec未初始化错误，无typed日志；params存活时context存活，最终释放后context消失；无注册或状态事件。
#[test]
fn test_service_default_bound_params_hold_context() {
    service_test_inner::test_service_default_bound_params_hold_context();
}

/// 契约：slice-service / default：真实宏初始化不等于记录器就绪。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: default：真实宏初始化不等于记录器就绪。
/// Given 真实登记表/context，无记录器。
/// When registry.register/init再poll合法Trace=0123456789abcdeffedcba9876543210通知。
/// Then 同步Registered→Running各一次、元信息准确；一次submit/RecorderUnavailable警告，不把Running当业务成功。
#[test]
fn test_service_default_registry_initialization_without_recorder() {
    service_test_inner::test_service_default_registry_initialization_without_recorder();
}

/// 契约：slice-service / default：Running条目延长params资源生命周期。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: default：Running条目延长params资源生命周期。
/// Given 真实registry已初始化，持有opaque entry快照，未编解码。
/// When close并释放Owner、外部context/runtime/table，最后释放entry。
/// Then close不改Running；entry保留context及codec表，最后entry释放后两者消失，不推导TLS回收。
#[test]
fn test_service_default_running_entry_retains_context_and_codec() {
    service_test_inner::test_service_default_running_entry_retains_context_and_codec();
}

/// 契约：slice-service / default：Future创建和未poll丢弃不处理消息。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: default：Future创建和未poll丢弃不处理消息。
/// Given Running及真实记录器（local=本地/服务-B），Trace=0123456789abcdeffedcba9876543210和独立16字节均0x91 Trace。
/// When 首个on_send不poll即丢弃，再poll独立合法通知。
/// Then 创建/丢弃无日志或选择，真实worker排空后仅合法对照选择1/通知0/导出0，不把去重值当调用数。
#[test]
fn test_service_default_unpolled_future_has_no_business_effect() {
    service_test_inner::test_service_default_unpolled_future_has_no_business_effect();
}

/// 契约：slice-service / default：前置decode失败不进入typed。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: default：前置decode失败不进入typed。
/// Given Running和真实记录器（local=本地/服务-B），真实codec对空Vec返回CodecError。
/// When 框架poll空输入后poll独立16字节均0x92合法通知。
/// Then 空输入无本库typed警告，最终只有合法选择1；框架Ok不当作解码成功，不承诺本库前置错误日志。
#[test]
fn test_service_default_decode_failure_stays_before_typed() {
    service_test_inner::test_service_default_decode_failure_stays_before_typed();
}

/// 契约：slice-service / default：业务非法只产生validate拒绝。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: default：业务非法只产生validate拒绝。
/// Given Running和真实记录器；对端真实编码全零Trace、非法UTF8=[80]/[ff]/[c0 af]/[ed a0 80]/[f4 90 80 80]/[41 e2 82]/[41 e2 28]/UTF8(不要泄露/来源秘密)+ff，空source及期限i64::MIN/-116444736000000001/9106927300854775808/i64::MAX。
/// When 每例及同错误隐私对照各poll一次；零Trace/空source保持错误条件，UTF8换原始字节但保持错误位置/长度，其余变换为source=独立/节点-D、Trace=16字节a7；再poll独立16字节均0x93合法Trace。
/// Then 每项恰一次validate/真实ProtocolError警告，WARN/target=pi_tracing_coordinator::service及必需service_port=4091/stage/error，允许固定附加说明但同错误下附加字段不随私有输入变化，禁止回显source/Trace/操作号，合法对照成功无日志且最终仅选择1；精确零binding/记录器调用另需源码审查。
#[test]
fn test_service_default_invalid_business_is_validate_only() {
    service_test_inner::test_service_default_invalid_business_is_validate_only();
}

/// 契约：slice-service / default：Closed不覆盖protocol首错。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: default：Closed不覆盖protocol首错。
/// Given 已Running但binding已Closed，组合为零Trace+ff+i64::MAX、合法Trace+ff+i64::MIN、合法Trace+空source+i64::MAX、合法Trace+合法source+i64::MIN。
/// When poll四种首错输入，再poll完全合法Trace=0123456789abcdeffedcba9876543210。
/// Then validate依次InvalidTraceId/UTF8/EmptyIdentity/DeadlineOutOfRange，只有合法输入submit/Closed；共5警告。
#[test]
fn test_service_default_protocol_priority_precedes_closed() {
    service_test_inner::test_service_default_protocol_priority_precedes_closed();
}

/// 契约：slice-service / default：未绑定和None均接收拒绝。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: default：未绑定和None均接收拒绝。
/// Given Running，先OpenUnbound，再bind_recorder(None)。
/// When 分别poll Trace=0123456789abcdeffedcba9876543210和source=独立/节点-D、16字节均0x94合法通知。
/// Then 恰2条submit/RecorderUnavailable且附加字段不随私有输入变化；仍Running且发送准入成功；None消耗机会，再绑定RecorderAlreadyBound。
#[test]
fn test_service_default_unbound_and_none_report_unavailable() {
    service_test_inner::test_service_default_unbound_and_none_report_unavailable();
}

/// 契约：slice-service / default：关闭拒绝新接收且不关闭宿主。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: default：关闭拒绝新接收且不关闭宿主。
/// Given Running已绑定真实记录器（local=本地/服务-B），随后owner.close。
/// When poll Trace=0123456789abcdeffedcba9876543210及source=独立/节点-D且16字节均a7的隐私对照，再由宿主直接发布独立16字节均0x95控制Trace。
/// Then 恰2条submit/Closed且附加字段与私有输入无关，Running/context保持；最终只有宿主控制选择1，通知0导出0。
#[test]
fn test_service_default_closed_rejects_without_stopping_host() {
    service_test_inner::test_service_default_closed_rejects_without_stopping_host();
}

/// 契约：slice-service / default：Owner释放后params仍存活但接收关闭。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: default：Owner释放后params仍存活但接收关闭。
/// Given Running/记录器（local=本地/服务-B），context和params仍存活。
/// When 释放唯一Owner再poll Trace=0123456789abcdeffedcba9876543210，并直接发布独立16字节均0x96控制Trace。
/// Then 恰1条submit/Closed，最终仅控制选择1；context不保持Owner开放，精确仅Weak构造另审。
#[test]
fn test_service_default_owner_drop_leaves_weak_context() {
    service_test_inner::test_service_default_owner_drop_leaves_weak_context();
}

/// 契约：slice-service / default：真实false只报告未发布。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: default：真实false只报告未发布。
/// Given 分别source=本地/服务-B自来源、期限UNIX_EPOCH过期、未启用协调、Guard停止装配，每进程只选当前适配。
/// When 每组poll业务合法通知及16字节均a7的同原因隐私对照（非自来源组source换独立/节点-D），可恢复组再poll独立16字节均0x97合法对照。
/// Then 每次恰1条、每组2条submit/EventNotPublished且附加字段与私有输入无关，不猜原因；可恢复组最终选择1，其余0，成功无日志，不重试/补偿。
#[test]
fn test_service_default_recorder_false_is_submit_not_published() {
    service_test_inner::test_service_default_recorder_false_is_submit_not_published();
}

/// 契约：slice-service / default：真实成功保真且无日志。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: default：真实成功保真且无日志。
/// Given 3个不同Trace、source="  节点é/e\u{301}🦀\0 "（保留前后空白），宿主已登记来源及观察邻居（观察/节点-C），期限整秒后30秒。
/// When 逐个poll非对称/首字节单非零/末字节单非零Trace。
/// Then 已收3条均向观察邻居（观察/节点-C），Trace/期限保真且均非原始source；关闭时选择3/通知3/导出0/本库日志0；通知队列可丢弃，精确通知全集/来源绝不回送/精确apply次数另审。
#[test]
fn test_service_default_success_preserves_trace_deadline_and_source() {
    service_test_inner::test_service_default_success_preserves_trace_deadline_and_source();
}

/// 契约：slice-service / default：宿主过滤不影响业务。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: default：宿主过滤不影响业务。
/// Given Running及真实记录器（local=本地/服务-B），线程局部subscriber禁用。
/// When 非法UTF8=ff配合整秒后30秒合法期限，再poll同期限/合法source的独立16字节均0x98通知。
/// Then 保存日志0，排空后仅合法选择1、通知0导出0，过滤不使非法业务被处理。
#[test]
fn test_service_default_filtered_logs_do_not_change_business() {
    service_test_inner::test_service_default_filtered_logs_do_not_change_business();
}

/// 契约：slice-service / default：submit警告允许宿主重入关闭。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: default：submit警告允许宿主重入关闭。
/// Given Running但binding未绑定记录器，subscriber观察后同步close同Owner。
/// When poll合法Trace=0123456789abcdeffedcba9876543210。
/// Then 真实Unavailable原因恰一次submit警告，回调close完成、后续准入Closed、registry仍Running；全部分支资源释放另审。
#[test]
fn test_service_default_submit_log_runs_after_binding_return() {
    service_test_inner::test_service_default_submit_log_runs_after_binding_return();
}

/// 契约：slice-service / multi：静态身份及元信息且不提前安装。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: multi：静态身份及元信息且不提前安装。
/// Given 空runtime，完整类型表及对应getter；MultiLayer正常装配明确目标层及真实表。
/// When 重复取得静态借用，再显式register。
/// Then 同一static；取得时无context/注册/事件/本库日志，register后Registered、id=svc://local/observability/tracing/coordinator、port4091、layer=tracing-coordinator、Send、version0.1.0；未init返回ServiceNotRunning。
#[test]
fn test_service_multi_static_identity_and_metadata() {
    service_test_inner::test_service_multi_static_identity_and_metadata();
}

/// 契约：slice-service / multi：缺上下文保留旧状态。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: multi：缺上下文保留旧状态。
/// Given 真实Registered服务，contexts缺CoordinationServiceContext；MultiLayer正常装配明确目标层及真实表。
/// When registry.init。
/// Then 实际BindOnlyReadNotFound(4091,context,CoordinationServiceContext)，仍Registered及原元信息，仅原注册事件，不形成Failed。
#[test]
fn test_service_multi_missing_context_preserves_registered() {
    service_test_inner::test_service_multi_missing_context_preserves_registered();
}

/// 契约：slice-service / multi：合法bind_params保留context且codec未绑定。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: multi：合法bind_params保留context且codec未绑定。
/// Given new创建context并注入，static/runtime对应同一codec；MultiLayer正常装配明确目标层及真实表。
/// When 同一static.bind_params，再poll初始化前handler，释放容器/runtime/Owner，最后释放params。
/// Then 精确codec未初始化错误，无typed日志；params存活时context存活，最终释放后context消失；无注册或状态事件。
#[test]
fn test_service_multi_bound_params_hold_context() {
    service_test_inner::test_service_multi_bound_params_hold_context();
}

/// 契约：slice-service / multi：真实宏初始化不等于记录器就绪。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: multi：真实宏初始化不等于记录器就绪。
/// Given 真实登记表/context，无记录器；MultiLayer正常装配明确目标层及真实表。
/// When registry.register/init再poll合法Trace=0123456789abcdeffedcba9876543210通知。
/// Then 同步Registered→Running各一次、元信息准确；一次submit/RecorderUnavailable警告，不把Running当业务成功。
#[test]
fn test_service_multi_registry_initialization_without_recorder() {
    service_test_inner::test_service_multi_registry_initialization_without_recorder();
}

/// 契约：slice-service / multi：Running条目延长params资源生命周期。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: multi：Running条目延长params资源生命周期。
/// Given 真实registry已初始化，持有opaque entry快照，未编解码；MultiLayer正常装配明确目标层及真实表。
/// When close并释放Owner、外部context/runtime/table，最后释放entry。
/// Then close不改Running；entry保留context及codec表，最后entry释放后两者消失，不推导TLS回收。
#[test]
fn test_service_multi_running_entry_retains_context_and_codec() {
    service_test_inner::test_service_multi_running_entry_retains_context_and_codec();
}

/// 契约：slice-service / multi：Future创建和未poll丢弃不处理消息。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: multi：Future创建和未poll丢弃不处理消息。
/// Given Running及真实记录器（local=本地/服务-B），Trace=0123456789abcdeffedcba9876543210和独立16字节均0x91 Trace；MultiLayer正常装配明确目标层及真实表。
/// When 首个on_send不poll即丢弃，再poll独立合法通知。
/// Then 创建/丢弃无日志或选择，真实worker排空后仅合法对照选择1/通知0/导出0，不把去重值当调用数。
#[test]
fn test_service_multi_unpolled_future_has_no_business_effect() {
    service_test_inner::test_service_multi_unpolled_future_has_no_business_effect();
}

/// 契约：slice-service / multi：前置decode失败不进入typed。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: multi：前置decode失败不进入typed。
/// Given Running和真实记录器（local=本地/服务-B），真实codec对空Vec返回CodecError；MultiLayer正常装配明确目标层及真实表。
/// When 框架poll空输入后poll独立16字节均0x92合法通知。
/// Then 空输入无本库typed警告，最终只有合法选择1；框架Ok不当作解码成功，不承诺本库前置错误日志。
#[test]
fn test_service_multi_decode_failure_stays_before_typed() {
    service_test_inner::test_service_multi_decode_failure_stays_before_typed();
}

/// 契约：slice-service / multi：业务非法只产生validate拒绝。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: multi：业务非法只产生validate拒绝。
/// Given Running和真实记录器；对端真实编码全零Trace、非法UTF8=[80]/[ff]/[c0 af]/[ed a0 80]/[f4 90 80 80]/[41 e2 82]/[41 e2 28]/UTF8(不要泄露/来源秘密)+ff，空source及期限i64::MIN/-116444736000000001/9106927300854775808/i64::MAX；MultiLayer正常装配明确目标层及真实表。
/// When 每例及同错误隐私对照各poll一次；零Trace/空source保持错误条件，UTF8换原始字节但保持错误位置/长度，其余变换为source=独立/节点-D、Trace=16字节a7；再poll独立16字节均0x93合法Trace。
/// Then 每项恰一次validate/真实ProtocolError警告，WARN/target=pi_tracing_coordinator::service及必需service_port=4091/stage/error，允许固定附加说明但同错误下附加字段不随私有输入变化，禁止回显source/Trace/操作号，合法对照成功无日志且最终仅选择1；精确零binding/记录器调用另需源码审查。
#[test]
fn test_service_multi_invalid_business_is_validate_only() {
    service_test_inner::test_service_multi_invalid_business_is_validate_only();
}

/// 契约：slice-service / multi：Closed不覆盖protocol首错。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: multi：Closed不覆盖protocol首错。
/// Given 已Running但binding已Closed，组合为零Trace+ff+i64::MAX、合法Trace+ff+i64::MIN、合法Trace+空source+i64::MAX、合法Trace+合法source+i64::MIN；MultiLayer正常装配明确目标层及真实表。
/// When poll四种首错输入，再poll完全合法Trace=0123456789abcdeffedcba9876543210。
/// Then validate依次InvalidTraceId/UTF8/EmptyIdentity/DeadlineOutOfRange，只有合法输入submit/Closed；共5警告。
#[test]
fn test_service_multi_protocol_priority_precedes_closed() {
    service_test_inner::test_service_multi_protocol_priority_precedes_closed();
}

/// 契约：slice-service / multi：未绑定和None均接收拒绝。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: multi：未绑定和None均接收拒绝。
/// Given Running，先OpenUnbound，再bind_recorder(None)；MultiLayer正常装配明确目标层及真实表。
/// When 分别poll Trace=0123456789abcdeffedcba9876543210和source=独立/节点-D、16字节均0x94合法通知。
/// Then 恰2条submit/RecorderUnavailable且附加字段不随私有输入变化；仍Running且发送准入成功；None消耗机会，再绑定RecorderAlreadyBound。
#[test]
fn test_service_multi_unbound_and_none_report_unavailable() {
    service_test_inner::test_service_multi_unbound_and_none_report_unavailable();
}

/// 契约：slice-service / multi：关闭拒绝新接收且不关闭宿主。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: multi：关闭拒绝新接收且不关闭宿主。
/// Given Running已绑定真实记录器（local=本地/服务-B），随后owner.close；MultiLayer正常装配明确目标层及真实表。
/// When poll Trace=0123456789abcdeffedcba9876543210及source=独立/节点-D且16字节均a7的隐私对照，再由宿主直接发布独立16字节均0x95控制Trace。
/// Then 恰2条submit/Closed且附加字段与私有输入无关，Running/context保持；最终只有宿主控制选择1，通知0导出0。
#[test]
fn test_service_multi_closed_rejects_without_stopping_host() {
    service_test_inner::test_service_multi_closed_rejects_without_stopping_host();
}

/// 契约：slice-service / multi：Owner释放后params仍存活但接收关闭。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: multi：Owner释放后params仍存活但接收关闭。
/// Given Running/记录器（local=本地/服务-B），context和params仍存活；MultiLayer正常装配明确目标层及真实表。
/// When 释放唯一Owner再poll Trace=0123456789abcdeffedcba9876543210，并直接发布独立16字节均0x96控制Trace。
/// Then 恰1条submit/Closed，最终仅控制选择1；context不保持Owner开放，精确仅Weak构造另审。
#[test]
fn test_service_multi_owner_drop_leaves_weak_context() {
    service_test_inner::test_service_multi_owner_drop_leaves_weak_context();
}

/// 契约：slice-service / multi：真实false只报告未发布。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: multi：真实false只报告未发布。
/// Given 分别source=本地/服务-B自来源、期限UNIX_EPOCH过期、未启用协调、Guard停止装配，每进程只选当前适配；MultiLayer正常装配明确目标层及真实表。
/// When 每组poll业务合法通知及16字节均a7的同原因隐私对照（非自来源组source换独立/节点-D），可恢复组再poll独立16字节均0x97合法对照。
/// Then 每次恰1条、每组2条submit/EventNotPublished且附加字段与私有输入无关，不猜原因；可恢复组最终选择1，其余0，成功无日志，不重试/补偿。
#[test]
fn test_service_multi_recorder_false_is_submit_not_published() {
    service_test_inner::test_service_multi_recorder_false_is_submit_not_published();
}

/// 契约：slice-service / multi：真实成功保真且无日志。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: multi：真实成功保真且无日志。
/// Given 3个不同Trace、source="  节点é/e\u{301}🦀\0 "（保留前后空白），宿主已登记来源及观察邻居（观察/节点-C），期限整秒后30秒；MultiLayer正常装配明确目标层及真实表。
/// When 逐个poll非对称/首字节单非零/末字节单非零Trace。
/// Then 已收3条均向观察邻居（观察/节点-C），Trace/期限保真且均非原始source；关闭时选择3/通知3/导出0/本库日志0；通知队列可丢弃，精确通知全集/来源绝不回送/精确apply次数另审。
#[test]
fn test_service_multi_success_preserves_trace_deadline_and_source() {
    service_test_inner::test_service_multi_success_preserves_trace_deadline_and_source();
}

/// 契约：slice-service / multi：宿主过滤不影响业务。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: multi：宿主过滤不影响业务。
/// Given Running及真实记录器（local=本地/服务-B），线程局部subscriber禁用；MultiLayer正常装配明确目标层及真实表。
/// When 非法UTF8=ff配合整秒后30秒合法期限，再poll同期限/合法source的独立16字节均0x98通知。
/// Then 保存日志0，排空后仅合法选择1、通知0导出0，过滤不使非法业务被处理。
#[test]
fn test_service_multi_filtered_logs_do_not_change_business() {
    service_test_inner::test_service_multi_filtered_logs_do_not_change_business();
}

/// 契约：slice-service / multi：submit警告允许宿主重入关闭。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: multi：submit警告允许宿主重入关闭。
/// Given Running但binding未绑定记录器，subscriber观察后同步close同Owner；MultiLayer正常装配明确目标层及真实表。
/// When poll合法Trace=0123456789abcdeffedcba9876543210。
/// Then 真实Unavailable原因恰一次submit警告，回调close完成、后续准入Closed、registry仍Running；全部分支资源释放另审。
#[test]
fn test_service_multi_submit_log_runs_after_binding_return() {
    service_test_inner::test_service_multi_submit_log_runs_after_binding_return();
}

/// 契约：slice-service / 预绑定真实失败形成带params的Failed。
/// @api:src/service.rs @impl:src/service_inner.rs @critical
/// Scenario: 预绑定真实失败形成带params的Failed。
/// Given MultiLayer只有unrelated层，无目标/default，context真实注入且Registered。
/// When registry使用同一static绑定params后init调用codec_for_layer。
/// Then 实际CodecLayerNotFound(tracing-coordinator)，Registered→Failed；释放runtime后Failed entry仍持context，codec表已释放，entry释放后context消失。
#[test]
fn test_service_multi_prebind_failure_retains_failed_params() {
    service_test_inner::test_service_multi_prebind_failure_retains_failed_params();
}

/// 契约：slice-service / default的Running不证明类型登记。
/// @api:src/service.rs @impl:src/service_inner.rs @negative
/// Scenario: default预绑定和Running不能代替真实登记。
/// Given 合法context及DefaultCodec空消息类型表。
/// When 同一static经真实registry初始化为Running后，实际poll已登记发送端产生的Trace=0123456789abcdeffedcba9876543210字节。
/// Then 真实接收codec为CodecError，无typed日志，仍Running；独立宿主0x99 Trace对照成功，排空后仅控制选择1、通知0导出0。
#[test]
fn test_service_default_running_does_not_prove_registration() {
    service_test_inner::test_service_default_running_does_not_prove_registration();
}

/// 契约：slice-service / multi的Running不证明类型登记。
/// @api:src/service.rs @impl:src/service_inner.rs @negative
/// Scenario: multi预绑定和Running不能代替真实登记。
/// Given 合法context及空消息类型表；MultiLayer明确存在tracing-coordinator目标层。
/// When 同一static经真实registry初始化为Running后，实际poll已登记发送端产生的Trace=0123456789abcdeffedcba9876543210字节。
/// Then 真实接收codec为CodecError，无typed日志，仍Running；独立宿主0x99 Trace对照成功，排空后仅控制选择1、通知0导出0。
#[test]
fn test_service_multi_running_does_not_prove_registration() {
    service_test_inner::test_service_multi_running_does_not_prove_registration();
}
