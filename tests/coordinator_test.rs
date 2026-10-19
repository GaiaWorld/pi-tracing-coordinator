//! slice-coordinator声明：自足Scenario与同名实现一一对应，只透传。
//! @api:src/coordinator.rs @impl:src/coordinator_inner.rs
//! @test:tests/coordinator_test.rs
//! @design-doc:design/tracing-coordinator/07-design/call-chain.md#slice-coordinator
//! 根公开重导出经真实装配路由消费；不可见证据见docs/SLICE_COORDINATOR_TEST_EVIDENCE.md。

#[path = "coordinator/coordinator_test_inner.rs"]
mod coordinator_test_inner;

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: fixture_wire_route_and_observers
/// Given 已登记真实表、两具体codec、配对runtime/terminal，固定Trace=0123456789abcdeffedcba9876543210/Unix+2秒/原始SOURCE。
/// When 校准既有表示/路由/panic/日志，并用8个正确完整表述和40个反向/错误语义校准Display判定器。
/// Then 真实对象头1100815、内容保真、先排队后poll及远端4091保持；每个正确表述仅匹配自身类；包括“编码层并不缺失”“上下文未被预占”的40反例全拒绝；未知完整文案不按子串猜测。仅夹具校准。
#[test]
fn test_coordinator_fixture_wire_route_and_observers() {
    coordinator_test_inner::test_coordinator_fixture_wire_route_and_observers();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: fixture_real_recorder_controls
/// Given 真实start的A/B记录器及独立受控通知线程记录器，retention60秒、worker_poll5ms、无Span，通道3秒截止。
/// When 发布0x41/0x42校准独立选择；另由真实通知线程逐次返回Ok/Err/Err/Err校准透明观测器。
/// Then A/B各选择1且无导出；回调开始但未返回时无报告，返回报告精确绑定remote/Trace/期限/结果；相反结果及错误身份/Trace/期限、末两次均Err冒充首Ok均拒绝。仅夹具校准。
#[test]
fn test_coordinator_fixture_real_recorder_controls() {
    coordinator_test_inner::test_coordinator_fixture_real_recorder_controls();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: default_constructor_is_lazy
/// Given 宿主登记发布并核对默认Arc表，terminal/runtime匹配，local为空。
/// When 公开new_default后首次绑定context、宿主inventory注册并init。
/// Then 构造后零事件/注册/发送，首次context成功且不注册；安装后真实接收报告记录器不可用。
#[test]
fn test_coordinator_default_constructor_is_lazy() {
    coordinator_test_inner::test_coordinator_default_constructor_is_lazy();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: multi_constructor_is_lazy
/// Given 目标tracing-coordinator层真实Arc已核，默认表未登记，环境匹配。
/// When 公开new_multi_layer后首次绑定及inventory安装。
/// Then 构造不发布/注册/发送，目标层实际安装成功，未绑定接收不可用，不采用默认表。
#[test]
fn test_coordinator_multi_constructor_is_lazy() {
    coordinator_test_inner::test_coordinator_multi_constructor_is_lazy();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: multi_missing_layer_rejects_without_fallback
/// Given Examples：有正确登记default但缺目标、仅unrelated层且无default及目标。
/// When 分别公开new_multi_layer。
/// Then 均返回缺层且source=None，零服务事件/发送；正确default不豁免明确目标缺失。
#[test]
fn test_coordinator_multi_missing_layer_rejects_without_fallback() {
    coordinator_test_inner::test_coordinator_multi_missing_layer_rejects_without_fallback();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: default_notifier_shares_client_and_is_weak
/// Given 默认协调器及两个notifier，期限Unix+2秒。
/// When 比较trait Arc并notify一次，再释放coordinator及终端后使用残留notifier。
/// Then 同Arc、首次字节含保存local；终端析构1；后续Binding关闭优先，零追加提交、一次失败日志。
#[test]
fn test_coordinator_default_notifier_shares_client_and_is_weak() {
    coordinator_test_inner::test_coordinator_default_notifier_shares_client_and_is_weak();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: multi_notifier_shares_client_and_is_weak
/// Given 明确多层协调器及两个notifier。
/// When notify后释放coordinator/终端再通知。
/// Then 同trait Arc、目标codec及local保真，终端析构1，残留通知Closed且无追加提交。
#[test]
fn test_coordinator_multi_notifier_shares_client_and_is_weak() {
    coordinator_test_inner::test_coordinator_multi_notifier_shares_client_and_is_weak();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: notifier_is_injected_into_real_logger
/// Given 默认notifier经只观测并原样委托的包装注入真实logger，启用记录器，无Span/文件日志/指标，返回报告3秒截止。
/// When 绑定记录器、登记REMOTE并发布0x51，等该Trace的生产notify实际返回Ok后才close，再发布独立0x52并等其返回。
/// Then 两份报告分别精确匹配REMOTE/Trace/期限及Ok、CoordinationSendError(Closed)；初始化零发送，提交仅1且local/内容保真，累计2尝试1失败只作辅证；残留notifier不保活终端并Closed。
#[test]
fn test_coordinator_notifier_is_injected_into_real_logger() {
    coordinator_test_inner::test_coordinator_notifier_is_injected_into_real_logger();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: logger_without_recorder_binds_none
/// Given 真实logger trace禁用且已有协调器notifier。
/// When 宿主确认guards无记录器后绑定None、安装并重复绑定。
/// Then Running但接收不可用，重复返回真实绑定原因；显式发送仍1。
#[test]
fn test_coordinator_logger_without_recorder_binds_none() {
    coordinator_test_inner::test_coordinator_logger_without_recorder_binds_none();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: logger_error_remains_host_responsibility
/// Given 真实logger启用记录器但传入空local，协调器发送接入已构造。
/// When 宿主接收身份错误，再绑定None和指定发送。
/// Then 真实logger错误不消费绑定机会、不关闭设施，首次None成功且发送1。
#[test]
fn test_coordinator_logger_error_remains_host_responsibility() {
    coordinator_test_inner::test_coordinator_logger_error_remains_host_responsibility();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: default_some_binding_keeps_original_recorder
/// Given 默认协调器及不同真实A/B记录器和Guard。
/// When Some(A)后Some(B)/None重复，再接收、close及宿主直接发布。
/// Then 重复不替换A，接收只进A；B直接对照有效；关闭优先重复且不关闭记录器，最终A选2/B选1。
#[test]
fn test_coordinator_default_some_binding_keeps_original_recorder() {
    coordinator_test_inner::test_coordinator_default_some_binding_keeps_original_recorder();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: multi_some_binding_keeps_original_recorder
/// Given 目标层协调器及真实A/B记录器。
/// When Some(A)后Some(B)/None重复、目标codec接收并关闭。
/// Then 重复真实原因不替换A，真实接收A1、宿主对照B1；关闭后宿主A仍可用。
#[test]
fn test_coordinator_multi_some_binding_keeps_original_recorder() {
    coordinator_test_inner::test_coordinator_multi_some_binding_keeps_original_recorder();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: none_consumes_binding_opportunity
/// Given 默认协调器及真实A/B记录器。
/// When None后Some(B)/None重复、安装接收及关闭。
/// Then None消耗机会、重复均拒绝、接收不可用；宿主A/B直接各选1，关闭后绑定Closed。
#[test]
fn test_coordinator_none_consumes_binding_opportunity() {
    coordinator_test_inner::test_coordinator_none_consumes_binding_opportunity();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: default_context_preoccupation_keeps_original_owner
/// Given 同一默认runtime中原A已绑定真实记录器、候选B独立。
/// When A发布后A重复及B绑定/安装拒绝，关闭B后宿主inventory安装。
/// Then 同Arc不幂等、预占source=None；B收尾不影响A接收选择1，关闭A后Closed；旧Arc精确身份另审。
#[test]
fn test_coordinator_default_context_preoccupation_keeps_original_owner() {
    coordinator_test_inner::test_coordinator_default_context_preoccupation_keeps_original_owner();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: multi_context_preoccupation_keeps_original_owner
/// Given 同一目标层环境的原A与候选B。
/// When A首次发布后A/B重复拒绝，关闭B后安装。
/// Then 不覆盖原拥有者，接收仍A1；关闭A才Closed，拒绝不额外注册。
#[test]
fn test_coordinator_multi_context_preoccupation_keeps_original_owner() {
    coordinator_test_inner::test_coordinator_multi_context_preoccupation_keeps_original_owner();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: default_closed_context_has_zero_publication
/// Given 同一默认runtime的关闭A及开放B。
/// When A绑定/安装拒绝、B首次发布，再A绑定。
/// Then A零事件且未预占B，已有context后A仍Closed优先；B安装接收不可用，零查询/锁另审。
#[test]
fn test_coordinator_default_closed_context_has_zero_publication() {
    coordinator_test_inner::test_coordinator_default_closed_context_has_zero_publication();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: multi_closed_context_has_zero_publication
/// Given 同一多层runtime关闭A及开放B。
/// When A绑定/安装拒绝，B首次绑定成功，再A绑定。
/// Then 关闭优先context预占，拒绝未发布；B真实安装Running且接收不可用。
#[test]
fn test_coordinator_multi_closed_context_has_zero_publication() {
    coordinator_test_inner::test_coordinator_multi_closed_context_has_zero_publication();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: default_explicit_install_and_route
/// Given 默认登记表、配对环境、未绑定记录器。
/// When install后先接收，再绑定Some接收独立Trace及显式发送。
/// Then 精确Registered→Running两同步事件，id=svc://local/observability/tracing/coordinator/layer/Send/0.1.0/4091；未绑定拒绝、绑定选1、指定发送1；重装context拒绝。
#[test]
fn test_coordinator_default_explicit_install_and_route() {
    coordinator_test_inner::test_coordinator_default_explicit_install_and_route();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: multi_explicit_install_and_route
/// Given 目标层已核Arc、默认表未登记。
/// When 显式安装、先未绑定接收，再Some接收及指定发送。
/// Then 正确目标codec实际收发；元信息4091、两同步状态事件；重复预占且零新事件。
#[test]
fn test_coordinator_multi_explicit_install_and_route() {
    coordinator_test_inner::test_coordinator_multi_explicit_install_and_route();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: default_inventory_install_and_route
/// Given 默认协调器、空registry。
/// When 绑定后按无关layer加载，再按公开layer加载，全部成功才init4091。
/// Then 无关layer无服务；加载Registered、init才Running；实际收发保真，重复显式安装不重注册。
#[test]
fn test_coordinator_default_inventory_install_and_route() {
    coordinator_test_inner::test_coordinator_default_inventory_install_and_route();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: multi_inventory_install_and_route
/// Given 多层明确目标表及空registry。
/// When 宿主绑定、按layer过滤inventory、全部成功才init。
/// Then 每场景只一种适配，Registered/Running分开且真实收发保真，元信息与4091一致。
#[test]
fn test_coordinator_multi_inventory_install_and_route() {
    coordinator_test_inner::test_coordinator_multi_inventory_install_and_route();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: default_conflict_keeps_context_and_old_service
/// Given 宿主服务已占4091且Running，本次默认候选未发布。
/// When 候选install冲突，宿主close/丢弃候选。
/// Then 保留实际RegisterPortConflict4091、已发布context及旧id/状态/事件；旧服务仍调用1，不覆盖/换号/回滚。
#[test]
fn test_coordinator_default_conflict_keeps_context_and_old_service() {
    coordinator_test_inner::test_coordinator_default_conflict_keeps_context_and_old_service();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: multi_conflict_keeps_context_and_old_service
/// Given 多层宿主旧服务占4091且Running。
/// When 候选显式安装冲突后关闭丢弃。
/// Then 保留实际Service原因、context仍预占、旧服务调用1、零新状态事件。
#[test]
fn test_coordinator_multi_conflict_keeps_context_and_old_service() {
    coordinator_test_inner::test_coordinator_multi_conflict_keeps_context_and_old_service();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: default_inventory_first_error_preserves_prefix
/// Given 已发布context，实际过滤三条为本库4091与宿主4090/4089，预占实际第二项。
/// When 按实际inventory顺序加载两layer。
/// Then 第二项真实冲突，第一项Registered保留，第二项旧Registered，第三项不存在；仅两注册事件，无init，context保留。
#[test]
fn test_coordinator_default_inventory_first_error_preserves_prefix() {
    coordinator_test_inner::test_coordinator_default_inventory_first_error_preserves_prefix();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: multi_inventory_first_error_preserves_prefix
/// Given 目标层context已发布、三个真实库存条目，预占实际第二项。
/// When 宿主按实际顺序加载。
/// Then 首错停止、前缀保留、后项不注册、无Running/Failed；不假定库存排序。
#[test]
fn test_coordinator_multi_inventory_first_error_preserves_prefix() {
    coordinator_test_inner::test_coordinator_multi_inventory_first_error_preserves_prefix();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: inventory_missing_context_preserves_registered
/// Given 宿主inventory注册Default但漏context，属于框架装配负面对照。
/// When registry.init失败后构造协调器、绑定context再init。
/// Then 真实BindOnlyReadNotFound4091/context/CoordinationServiceContext，仍Registered且1事件；补齐后Running，不冒充合法install失败。
#[test]
fn test_coordinator_inventory_missing_context_preserves_registered() {
    coordinator_test_inner::test_coordinator_inventory_missing_context_preserves_registered();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: default_explicit_source_wins_over_empty_local
/// Given 默认local为空，指定REMOTE/SOURCE/固定Trace和Unix+2秒。
/// When 公开send_selection一次。
/// Then 目标4091一次提交、独立对端字节与显式source一致，零额外日志。
#[test]
fn test_coordinator_default_explicit_source_wins_over_empty_local() {
    coordinator_test_inner::test_coordinator_default_explicit_source_wins_over_empty_local();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: multi_explicit_source_is_preserved
/// Given 目标多层表、保存local不同于显式SOURCE，固定Trace/Unix+2秒。
/// When 公开指定发送一次。
/// Then 正确目标codec、目标/source/字节保真，1提交、零额外日志/RPC/重连/探测。
#[test]
fn test_coordinator_multi_explicit_source_is_preserved() {
    coordinator_test_inner::test_coordinator_multi_explicit_source_is_preserved();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: send_preserves_actual_submit_error
/// Given 真实终端传输保存字节后返回CodecError(提交阶段/真实宿主原因)。
/// When 公开send_selection。
/// Then 公开CoordinationSendError直接原因保留实际ServiceError载荷；副作用已存在、总提交1且无重试/额外日志。
#[test]
fn test_coordinator_send_preserves_actual_submit_error() {
    coordinator_test_inner::test_coordinator_send_preserves_actual_submit_error();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: self_target_uses_real_local_route
/// Given 默认协调器已安装并Some真实记录器，SELF等于配对终端身份，宿主提供TaskSubmitter。
/// When send到SELF返回后宿主才poll已接管Future。
/// Then 远端0、队列1、同步返回时选择0；实际poll后选1，无自目标拒绝/同步等待。
#[test]
fn test_coordinator_self_target_uses_real_local_route() {
    coordinator_test_inner::test_coordinator_self_target_uses_real_local_route();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: default_close_preserves_shared_host
/// Given 协调服务与宿主4090均Running，真实记录器及残留notifier。
/// When 公开close两次，再新发送/绑定/安装/接收及宿主通信。
/// Then 新协调均Closed、notifier仍同Arc、registry Running；宿主本地第二次和记录器直接选择有效，远端0。
#[test]
fn test_coordinator_default_close_preserves_shared_host() {
    coordinator_test_inner::test_coordinator_default_close_preserves_shared_host();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: multi_close_preserves_shared_host
/// Given 多层协调及宿主4090均Running、真实记录器。
/// When 重复close后使用协调与宿主出口。
/// Then 新协调拒绝、宿主通信/记录器仍有效，不注销context/static/params，元信息4091保持。
#[test]
fn test_coordinator_multi_close_preserves_shared_host() {
    coordinator_test_inner::test_coordinator_multi_close_preserves_shared_host();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: default_drop_preserves_shared_host
/// Given 默认已安装并接收1、notifier/registry/Guard存活。
/// When 自动丢弃coordinator后通知/接收及宿主4090。
/// Then 新协调Closed、旧状态和共享设施仍可用；字段顺序/无额外Drop另审。
#[test]
fn test_coordinator_default_drop_preserves_shared_host() {
    coordinator_test_inner::test_coordinator_default_drop_preserves_shared_host();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: multi_drop_preserves_shared_host
/// Given 多层已安装并接收1，notifier及宿主设施仍存活。
/// When 自动析构后使用协调和宿主通信。
/// Then 新协调拒绝、宿主第二次及记录器直接选择有效，不关闭共享设施。
#[test]
fn test_coordinator_multi_drop_preserves_shared_host() {
    coordinator_test_inner::test_coordinator_multi_drop_preserves_shared_host();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: default_admitted_send_finishes_after_close
/// Given 默认notifier已准入并暂停真实Deref，通道3秒截止。
/// When 另线程close先返回，再释放暂停。
/// Then close不等在途；恢复1提交且local/Trace/期限保真，后续Closed、总提交仍1。
#[test]
fn test_coordinator_default_admitted_send_finishes_after_close() {
    coordinator_test_inner::test_coordinator_default_admitted_send_finishes_after_close();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: multi_admitted_send_finishes_after_close
/// Given 多层notifier已准入并暂停真实Deref。
/// When close先返回再继续。
/// Then 在途1提交、新通知Closed；不是安装竞争或压力测试。
#[test]
fn test_coordinator_multi_admitted_send_finishes_after_close() {
    coordinator_test_inner::test_coordinator_multi_admitted_send_finishes_after_close();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: default_admitted_send_finishes_after_drop
/// Given 默认notifier已准入暂停，唯一coordinator仍由宿主持有。
/// When 另线程drop owner先返回再恢复。
/// Then 析构不等待在途，在途1提交且新通知Closed；不是精确字段顺序的单独证明。
#[test]
fn test_coordinator_default_admitted_send_finishes_after_drop() {
    coordinator_test_inner::test_coordinator_default_admitted_send_finishes_after_drop();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: error_display_classifies_four_actual_failures
/// Given 根公开操作产生重复Binding/Service端口冲突/缺层/context预占四类错误。
/// When Display移除恰一次完整真实原因后，以同一已校准判定器匹配整段外层表述的同义分类集合。
/// Then 四类互斥且保留真实原因、无日志；“并不缺失/未被预占”、未初始化/已删除/混合分类均拒绝；仅归一化大小写/空白/两端分隔符，未知完整文案拒绝并待人工核验。
#[test]
fn test_coordinator_error_display_classifies_four_actual_failures() {
    coordinator_test_inner::test_coordinator_error_display_classifies_four_actual_failures();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: error_display_propagates_every_writer_failure
/// Given 公开操作四错误、受限fmt::Write首次失败后禁止继续写。
/// When 遍历0至完整输出减1容量并足容量对照。
/// Then 不足均Err(fmt::Error)，足容量完整文本成功；不把前置TODO作格式化RED。
#[test]
fn test_coordinator_error_display_propagates_every_writer_failure() {
    coordinator_test_inner::test_coordinator_error_display_propagates_every_writer_failure();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: error_source_borrows_direct_causes
/// Given 公开操作四错误，实际Binding重复和Service4091冲突。
/// When 每错误连续两次source。
/// Then 前两Some/Some同地址及直接原因；Service下转型载荷正确、Binding无下层；缺层/预占None/None。同一保存对象另审。
#[test]
fn test_coordinator_error_source_borrows_direct_causes() {
    coordinator_test_inner::test_coordinator_error_source_borrows_direct_causes();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: default_does_not_retain_host_runtime_or_terminal
/// Given 默认协调器/notifier、runtime标记Arc，未编解码排除TLS。
/// When 释放宿主rig，再coordinator、notifier。
/// Then 终端析构1且runtime标记失效；codec表可随notifier暂存，最后notifier释放表失效。
#[test]
fn test_coordinator_default_does_not_retain_host_runtime_or_terminal() {
    coordinator_test_inner::test_coordinator_default_does_not_retain_host_runtime_or_terminal();
}

/// 契约：slice-coordinator
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: multi_does_not_retain_host_runtime_or_terminal
/// Given 多层协调器/notifier及runtime标记，未编解码。
/// When 释放宿主rig、coordinator、notifier。
/// Then 不强持终端/runtime；允许codec层表延长持有，最后所有本场景codec释放后表失效，不宣称TLS清理。
#[test]
fn test_coordinator_multi_does_not_retain_host_runtime_or_terminal() {
    coordinator_test_inner::test_coordinator_multi_does_not_retain_host_runtime_or_terminal();
}
