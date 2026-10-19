//! slice-final声明面：Scenario自足，注册单元只同名透传。
//! @api:src/coordinator.rs @impl:src/coordinator_inner.rs @test:tests/final_test.rs
//! @design-doc:design/tracing-coordinator/07-design/call-chain.md#slice-final
//! 根公开面组合；未覆盖部署矩阵与内部审查责任见本地取证记录。

#[path = "final/final_test_inner.rs"]
mod final_test_inner;

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: default_full_chain_separates_completion
/// Given Default双宿主各自登记4300，唯一B接收服务Running，真实记录器对Trace0x21登记A/C，期限为整秒后30秒。
/// When A公开指定B发送后，由宿主再把接管字节交B真实on_send并poll。
/// Then 本地受理时B选择0；真实接收后选择1且C通知回调实际返回Ok；首跳source=A，后续source=B，Trace/期限保真。150ms有界观察只有一次后续提交；不冒充真实网络。
#[test]
fn test_final_default_full_chain_separates_completion() {
    final_test_inner::test_final_default_full_chain_separates_completion();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: multi_full_chain_separates_completion
/// Given Multi明确目标层登记4300，默认层未登记，唯一B服务Running，Trace0x21邻居A/C及真实记录器。
/// When A指定B发送，由宿主独立交付真实字节并poll。
/// Then 接管与实际接收/Worker处理分开；B只通知C，来源A/B、Trace及30秒期限保真，不误用默认层。
#[test]
fn test_final_multi_full_chain_separates_completion() {
    final_test_inner::test_final_multi_full_chain_separates_completion();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @negative
/// Scenario: no_designated_call_has_no_notification
/// Given 公开A/B与B记录器，Trace0x22尚未选择。
/// When 先有限观察，再由宿主只登记C，最后一次指定发送作有效对照。
/// Then 指定前两段150ms均零发送/选择/通知；指定后真实通知C且选择1。仅宿主登记不能让本库自行选择或通知。
#[test]
fn test_final_no_designated_call_has_no_notification() {
    final_test_inner::test_final_no_designated_call_has_no_notification();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @edgecase
/// Scenario: receive_may_finish_before_send_returns
/// Given Default合法装配，宿主选择在send调用栈内pollB真实接收，Trace0x23有效。
/// When A调用公开send_selection。
/// Then 返回前宿主已经实际poll完一次接收，随后真实Worker选择1；不能保证接收晚于方法返回，非性能证明。
#[test]
fn test_final_receive_may_finish_before_send_returns() {
    final_test_inner::test_final_receive_may_finish_before_send_returns();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @edgecase
/// Scenario: repeated_calls_are_not_cross_call_exactly_once
/// Given 同一合法Trace0x24、期限/source=A。
/// When 宿主两次独立调用相同公开发送。
/// Then 终端出口两份同内容消息；真实记录器去重后选择1，不以去重效果宣称本库跨调用exactly-once。
#[test]
fn test_final_repeated_calls_are_not_cross_call_exactly_once() {
    final_test_inner::test_final_repeated_calls_are_not_cross_call_exactly_once();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @negative
/// Scenario: submit_error_preserves_taken_bytes_without_retry
/// Given 宿主传输接管Trace0x25后返回RuntimeError原原因。
/// When A公开发送后，宿主仍交付已接管字节。
/// Then 联合核对Submit外层阶段及真实RuntimeError原因；同因Encode/混合/成功/错因负对照拒绝，仅一次提交且真实接收可选择1；无重试，不推定外部零效果。
#[test]
fn test_final_submit_error_preserves_taken_bytes_without_retry() {
    final_test_inner::test_final_submit_error_preserves_taken_bytes_without_retry();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @negative
/// Scenario: send_preconditions_have_zero_submission
/// Given 关闭/终端失效/坏目标/零Trace/空source为独立或组合前置，Trace0x26作合法控制。
/// When 经根公开发送触发准入、升级、解析及业务转换拒绝；不破坏稳定登记前提伪造编码失败。
/// Then 各前置零提交，保持冻结首错与直接原因；独立有效B发送恰有一份合法字节。
#[test]
fn test_final_send_preconditions_have_zero_submission() {
    final_test_inner::test_final_send_preconditions_have_zero_submission();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @negative
/// Scenario: default_decode_rejection_stays_before_business
/// Given default真实装配，独立对端codec已确认空输入CodecError，Trace0x31未来期限有效。
/// When 实际poll空输入，再由根公开发送合法控制。
/// Then 前置拒绝无typed日志/选择，合法控制选择1，150ms无额外通知；不推任意输入安全。
#[test]
fn test_final_default_decode_rejection_stays_before_business() {
    final_test_inner::test_final_default_decode_rejection_stays_before_business();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @negative
/// Scenario: default_invalid_business_never_publishes
/// Given default真实装配，独立真实Fory输入为零Trace、非法UTF8、空source、共同域两侧越界期限。
/// When 逐例经公开runtime实际poll，再由根公开发送Trace0x32。
/// Then 每例只一次validate真实原因日志、选择0；合法控制选择1；精确零binding调用由独立源码审查补证。
#[test]
fn test_final_default_invalid_business_never_publishes() {
    final_test_inner::test_final_default_invalid_business_never_publishes();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @negative
/// Scenario: default_unbound_receiver_has_zero_publication
/// Given default唯一接收服务Running，绑定状态=unbound，真实记录器存活，Trace0x37有效。
/// When 根公开发送并交付B，再以宿主记录器公开入口作有效对照。
/// Then B只一次submit/RecorderUnavailable日志，选择0且不通知；宿主直接选择0x38仍有效，首次绑定后独立Trace0x39可正常处理。
#[test]
fn test_final_default_unbound_receiver_has_zero_publication() {
    final_test_inner::test_final_default_unbound_receiver_has_zero_publication();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @negative
/// Scenario: default_none_receiver_has_zero_publication
/// Given default唯一接收服务Running，绑定状态=none，真实记录器存活，Trace0x37有效。
/// When 根公开发送并交付B，再以宿主记录器公开入口作有效对照。
/// Then B只一次submit/RecorderUnavailable日志，选择0且不通知；宿主直接选择0x38仍有效。
#[test]
fn test_final_default_none_receiver_has_zero_publication() {
    final_test_inner::test_final_default_none_receiver_has_zero_publication();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @negative
/// Scenario: default_closed_receiver_has_zero_publication
/// Given default唯一接收服务Running，绑定状态=closed，真实记录器存活，Trace0x37有效。
/// When 根公开发送并交付B，再以宿主记录器公开入口作有效对照。
/// Then B只一次submit/Closed日志，选择0且不通知；宿主直接选择0x38仍有效。
#[test]
fn test_final_default_closed_receiver_has_zero_publication() {
    final_test_inner::test_final_default_closed_receiver_has_zero_publication();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: default_expired_decision_does_not_propagate
/// Given default真实装配；过期Trace0x42/Unix纪元与有效Trace0x41/30秒均由宿主登记下游C。
/// When 先发送过期决定并有限观察，再发送有效决定作负面控制。
/// Then 过期决定确实转换编码并提交一次，B只报EventNotPublished，150ms零选择/传播；有效控制实际通知C并选择1，后续150ms无过期通知。本库不加now判断。
#[test]
fn test_final_default_expired_decision_does_not_propagate() {
    final_test_inner::test_final_default_expired_decision_does_not_propagate();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @negative
/// Scenario: default_unpublished_keeps_only_real_classification
/// Given default真实自来源和已停Guard两种false条件，中间有独立合法Trace0x45控制。
/// When 三次公开发送走真实接收。
/// Then 两种false只一次submit/EventNotPublished，不猜原因/不重试；控制选择1，最终提交3且无通知。
#[test]
fn test_final_default_unpublished_keeps_only_real_classification() {
    final_test_inner::test_final_default_unpublished_keeps_only_real_classification();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: default_close_preserves_other_host_business
/// Given defaultB唯一接收服务、独立4090业务、活跃Guard及A已提交通知。
/// When 关闭B两次，交付旧字节并继续宿主业务。
/// Then 新准入Closed；4091保持Running、4090前后各处理一次，宿主记录器仍选新Trace；close不注销或停止共享设施。
#[test]
fn test_final_default_close_preserves_other_host_business() {
    final_test_inner::test_final_default_close_preserves_other_host_business();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @edgecase
/// Scenario: default_admitted_send_finishes_after_close
/// Given default发送已准入后在合法Deref暂停，通道3秒截止。
/// When 先close并确认返回，再恢复发送并交付B。
/// Then close不等在途；恰一次提交及真实选择1，后续notify返回Closed且零追加，不把close当撤回屏障。
#[test]
fn test_final_default_admitted_send_finishes_after_close() {
    final_test_inner::test_final_default_admitted_send_finishes_after_close();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @negative
/// Scenario: multi_decode_rejection_stays_before_business
/// Given multi真实装配，独立对端codec已确认空输入CodecError，Trace0x31未来期限有效。
/// When 实际poll空输入，再由根公开发送合法控制。
/// Then 前置拒绝无typed日志/选择，合法控制选择1，150ms无额外通知；不推任意输入安全。
#[test]
fn test_final_multi_decode_rejection_stays_before_business() {
    final_test_inner::test_final_multi_decode_rejection_stays_before_business();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @negative
/// Scenario: multi_invalid_business_never_publishes
/// Given multi真实装配，独立真实Fory输入为零Trace、非法UTF8、空source、共同域两侧越界期限。
/// When 逐例经公开runtime实际poll，再由根公开发送Trace0x32。
/// Then 每例只一次validate真实原因日志、选择0；合法控制选择1；精确零binding调用由独立源码审查补证。
#[test]
fn test_final_multi_invalid_business_never_publishes() {
    final_test_inner::test_final_multi_invalid_business_never_publishes();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @negative
/// Scenario: multi_unbound_receiver_has_zero_publication
/// Given multi唯一接收服务Running，绑定状态=unbound，真实记录器存活，Trace0x37有效。
/// When 根公开发送并交付B，再以宿主记录器公开入口作有效对照。
/// Then B只一次submit/RecorderUnavailable日志，选择0且不通知；宿主直接选择0x38仍有效，首次绑定后独立Trace0x39可正常处理。
#[test]
fn test_final_multi_unbound_receiver_has_zero_publication() {
    final_test_inner::test_final_multi_unbound_receiver_has_zero_publication();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @negative
/// Scenario: multi_none_receiver_has_zero_publication
/// Given multi唯一接收服务Running，绑定状态=none，真实记录器存活，Trace0x37有效。
/// When 根公开发送并交付B，再以宿主记录器公开入口作有效对照。
/// Then B只一次submit/RecorderUnavailable日志，选择0且不通知；宿主直接选择0x38仍有效。
#[test]
fn test_final_multi_none_receiver_has_zero_publication() {
    final_test_inner::test_final_multi_none_receiver_has_zero_publication();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @negative
/// Scenario: multi_closed_receiver_has_zero_publication
/// Given multi唯一接收服务Running，绑定状态=closed，真实记录器存活，Trace0x37有效。
/// When 根公开发送并交付B，再以宿主记录器公开入口作有效对照。
/// Then B只一次submit/Closed日志，选择0且不通知；宿主直接选择0x38仍有效。
#[test]
fn test_final_multi_closed_receiver_has_zero_publication() {
    final_test_inner::test_final_multi_closed_receiver_has_zero_publication();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: multi_expired_decision_does_not_propagate
/// Given multi真实装配；过期Trace0x42/Unix纪元与有效Trace0x41/30秒均由宿主登记下游C。
/// When 先发送过期决定并有限观察，再发送有效决定作负面控制。
/// Then 过期决定确实转换编码并提交一次，B只报EventNotPublished，150ms零选择/传播；有效控制实际通知C并选择1，后续150ms无过期通知。本库不加now判断。
#[test]
fn test_final_multi_expired_decision_does_not_propagate() {
    final_test_inner::test_final_multi_expired_decision_does_not_propagate();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @negative
/// Scenario: multi_unpublished_keeps_only_real_classification
/// Given multi真实自来源和已停Guard两种false条件，中间有独立合法Trace0x45控制。
/// When 三次公开发送走真实接收。
/// Then 两种false只一次submit/EventNotPublished，不猜原因/不重试；控制选择1，最终提交3且无通知。
#[test]
fn test_final_multi_unpublished_keeps_only_real_classification() {
    final_test_inner::test_final_multi_unpublished_keeps_only_real_classification();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: multi_close_preserves_other_host_business
/// Given multiB唯一接收服务、独立4090业务、活跃Guard及A已提交通知。
/// When 关闭B两次，交付旧字节并继续宿主业务。
/// Then 新准入Closed；4091保持Running、4090前后各处理一次，宿主记录器仍选新Trace；close不注销或停止共享设施。
#[test]
fn test_final_multi_close_preserves_other_host_business() {
    final_test_inner::test_final_multi_close_preserves_other_host_business();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @edgecase
/// Scenario: multi_admitted_send_finishes_after_close
/// Given multi发送已准入后在合法Deref暂停，通道3秒截止。
/// When 先close并确认返回，再恢复发送并交付B。
/// Then close不等在途；恰一次提交及真实选择1，后续notify返回Closed且零追加，不把close当撤回屏障。
#[test]
fn test_final_multi_admitted_send_finishes_after_close() {
    final_test_inner::test_final_multi_admitted_send_finishes_after_close();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @edgecase
/// Scenario: explicit_source_is_preserved_without_normalizing_identity
/// Given 保存local=A，原始source含空白、é/组合音符、🦀及NUL；0x56与独立0x59均由宿主登记原始source/C邻居。
/// When 0x56以原source发送并真实接收，再用0x59以C为来源作同装配对照。
/// Then 首跳原字节保真；0x56只实际通知C且后续source=B，不回原来源；0x59实际通知原身份并返回真实目标解析失败。最终选择2、回调2/失败1、下游提交1，禁止trim或local覆盖。
#[test]
fn test_final_explicit_source_is_preserved_without_normalizing_identity() {
    final_test_inner::test_final_explicit_source_is_preserved_without_normalizing_identity();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @edgecase
/// Scenario: linux_normalizes_once_and_preserves_deadline
/// Given Linux未来整秒+123456789ns、Trace0x57，真实记录器已登记C。
/// When 根公开发送、真实接收及后续通知。
/// Then 首跳期限恰提前89ns，后续同一123456700ns期限，来源A/B及目标B/C保真，真实回调Ok。
#[cfg(target_os = "linux")]
#[test]
fn test_final_linux_normalizes_once_and_preserves_deadline() {
    final_test_inner::test_final_linux_normalizes_once_and_preserves_deadline();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @edgecase
/// Scenario: aligned_deadline_survives_each_boundary
/// Given Multi合法装配，未来整秒+123456700ns、Trace0x58且登记C。
/// When 公开发送、真实宏接收及记录器后续通知。
/// Then 首跳和后续期限逐值相同，来源A/B、目标B/C，真实回调Ok并选择1。
#[test]
fn test_final_aligned_deadline_survives_each_boundary() {
    final_test_inner::test_final_aligned_deadline_survives_each_boundary();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: cold_start_fixed_identity_does_not_restore_memory
/// Given 父测试wait旧进程退出，再以相同PeerId=B冷启；旧0x61曾选择、0x62仅登记C。
/// When 真实Guard关闭panic校准后执行旧/新阶段；新进程探测旧选择E/旧关联，再处理当前Trace0x64；各阶段同步stop并drop Pair后检查panic。
/// Then 初始选择/重放0，不复活旧选择、不继承0x62的C；当前控制实际通知C且source=B；完成标记只在Pair/Guard收尾且无意外panic后输出。仅内存冷启证明。
#[test]
fn test_final_cold_start_fixed_identity_does_not_restore_memory() {
    final_test_inner::test_final_cold_start_fixed_identity_does_not_restore_memory();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: cold_start_changed_identity_does_not_restore_memory
/// Given 旧进程B已wait退出，新进程合法变化身份D，旧选择0x61和关联0x62曾存在。
/// When 先校准真实Guard关闭panic捕获；新进程探测旧选择E/旧关联并处理当前控制，各阶段同步stop并drop Pair后检查panic。
/// Then 无恢复旧选择/关联或重放；新通知向C提交source=D；完成标记在全部同步收尾且无意外panic后输出，不增加代际协议或假称通知flush。
#[test]
fn test_final_cold_start_changed_identity_does_not_restore_memory() {
    final_test_inner::test_final_cold_start_changed_identity_does_not_restore_memory();
}
