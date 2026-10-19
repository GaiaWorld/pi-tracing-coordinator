//! slice-binding 测试声明：每条Scenario只透传同名实现，断言和夹具均在binding_test_inner.rs。
//! @api:src/binding.rs @impl:src/binding_inner.rs
//! @test:tests/binding/binding_test.rs
//! @design-doc:design/tracing-coordinator/07-design/call-chain.md#slice-binding
//! 复用既有tests挂载，在crate内消费pub(crate)冻结壳；不扩大可见性、不新增Gherkin或设计实体。
//! 真实记录器场景与生命周期场景使用独立子进程，避免全局初始化冲突和TODO Drop二次panic影响其它用例。
//! 用户已确认：不可见内部属性由真实行为测试与实现后独立源码审查组合取证，审查闭合前不通过。
//! 取证约定与剩余责任：docs/SLICE_BINDING_TEST_EVIDENCE.md#slice-binding-test-evidence。

#[path = "binding_test_inner.rs"]
mod binding_test_inner;

/// 契约：slice-binding / 真实记录器夹具校准，不计为binding实现证据。
/// @api:src/binding.rs @impl:src/binding_inner.rs @edgecase
/// Scenario: 真实记录器夹具校准，不计为binding实现证据。
/// Given 独立子进程通过pi_logger公开初始化建立真实记录器和宿主守卫，本地为本地/节点-B，未登记相邻关系。
/// When 直接向依赖提交自来源、纪元过期、合法未来决定，再由宿主停止记录器。
/// Then 前两次false，合法决定true且3秒内selected_traces=1，停止后false；通知数0。本例只校准依赖前提。
#[test]
fn test_binding_real_recorder_fixture_controls() {
    binding_test_inner::test_binding_real_recorder_fixture_controls();
}

/// 契约：slice-binding / 两个新对象的绑定与关闭互不影响。
/// @api:src/binding.rs @impl:src/binding_inner.rs @critical
/// Scenario: 两个新对象的绑定与关闭互不影响。
/// Given 通过new创建两个独立对象及其弱接入，合法Trace字节为01/23/45/67/89/ab/cd/ef/fe/dc/ba/98/76/54/32/10。
/// When 先分别发送准入，再让第一个绑定None并关闭。
/// Then 初始两者发送均Ok、接收RecorderUnavailable；第一个关闭后Closed，第二个仍可发送、接收仍不可用且首次绑定仍成功。
#[test]
fn test_binding_new_creates_independent_open_unbound_owners() {
    binding_test_inner::test_binding_new_creates_independent_open_unbound_owners();
}

/// 契约：slice-binding / 建立和克隆弱句柄不消耗一次绑定。
/// @api:src/binding.rs @impl:src/binding_inner.rs @critical
/// Scenario: 建立和克隆弱句柄不消耗一次绑定。
/// Given 新Owner、两次handle结果及第一个句柄的clone。
/// When 首次绑定None，随后经三句柄操作并关闭Owner。
/// Then 首次绑定成功，三句柄发送均Ok、接收均RecorderUnavailable；重复绑定RecorderAlreadyBound；关闭后三者均Closed。
#[test]
fn test_binding_handles_and_clones_preserve_binding_opportunity() {
    binding_test_inner::test_binding_handles_and_clones_preserve_binding_opportunity();
}

/// 契约：slice-binding / 关闭后仍可建立和复制弱接入但不能重开。
/// @api:src/binding.rs @impl:src/binding_inner.rs @edgecase
/// Scenario: 关闭后仍可建立和复制弱接入但不能重开。
/// Given 已经close的新Owner。
/// When 调用handle并clone，再发送准入、接收合法输入和绑定None。
/// Then handle/clone正常返回，所有后续语义操作均Closed，不因复制重新开放。
#[test]
fn test_binding_handle_and_clone_after_close_do_not_reopen() {
    binding_test_inner::test_binding_handle_and_clone_after_close_do_not_reopen();
}

/// 契约：slice-binding / 首次None消耗机会且重复不能改为Some。
/// @api:src/binding.rs @impl:src/binding_inner.rs @critical
/// Scenario: 首次None消耗机会且重复不能改为Some。
/// Given 新Owner、合法交接结果及真实可发布记录器。
/// When 首次绑定None，再分别绑定None和Some。
/// Then 首次Ok，两次重复均RecorderAlreadyBound；发送仍Ok、接收仍RecorderUnavailable；宿主关闭并等待Worker退出后selected_traces=0，此统计不证明精确调用次数。
#[test]
fn test_binding_first_none_cannot_be_replaced_by_none_or_some() {
    binding_test_inner::test_binding_first_none_cannot_be_replaced_by_none_or_some();
}

/// 契约：slice-binding / 首次Some不被失败重复绑定清空。
/// @api:src/binding.rs @impl:src/binding_inner.rs @critical
/// Scenario: 首次Some不被失败重复绑定清空。
/// Given 两个公开start创建的独立真实记录器A/B，本地身份分别为本地/节点-B及原始SOURCE；新Owner首次绑定A。
/// When 再次绑定B被拒绝后立即交接非对称Trace，绑定None被拒绝后立即交接全5a的不同Trace，来源均为SOURCE；另直接给B发布全a5的Trace、来源本地/节点-B作存活对照。
/// Then 两次重复均RecorderAlreadyBound，两次binding接收均Ok；关闭并等待双方Worker退出后A选择数精确为2、B精确为1、无通知或Span导出，证明每次失败绑定后均保留A。
#[test]
fn test_binding_first_some_survives_rejected_some_and_none() {
    binding_test_inner::test_binding_first_some_survives_rejected_some_and_none();
}

/// 契约：slice-binding / 健康锁下Closed优先于重复绑定。
/// @api:src/binding.rs @impl:src/binding_inner.rs @negative
/// Scenario: 健康锁下Closed优先于重复绑定。
/// Given 独立的未绑定、已绑定None、已绑定Some三个Owner均已关闭。
/// When 分别再绑定None和Some。
/// Then 所有六次均Closed，不能返回RecorderAlreadyBound或重开；宿主关闭并等待Worker退出后selected_traces=0。
#[test]
fn test_binding_closed_precedes_already_bound_for_every_healthy_state() {
    binding_test_inner::test_binding_closed_precedes_already_bound_for_every_healthy_state();
}

/// 契约：slice-binding / 三种开放状态的发送准入返回单位成功。
/// @api:src/binding.rs @impl:src/binding_inner.rs @critical
/// Scenario: 三种开放状态的发送准入返回单位成功。
/// Given 独立Owner处于未绑定、绑定None或绑定Some，真实记录器选择和通知数初始0。
/// When 每个Owner分别执行三次admit_send。
/// Then 每次均Result<(),BindingError>的Ok；宿主关闭并等待Worker退出后选择和通知数仍0，不把它解释为网络发送已完成。
#[test]
fn test_binding_admit_send_is_unit_success_in_all_open_states() {
    binding_test_inner::test_binding_admit_send_is_unit_success_in_all_open_states();
}

/// 契约：slice-binding / 未绑定和明确None拒绝合法接收。
/// @api:src/binding.rs @impl:src/binding_inner.rs @negative
/// Scenario: 未绑定和明确None拒绝合法接收。
/// Given 两个独立Owner分别未绑定和已绑定None，输入均通过第1块合法出口构造。
/// When 分别调用submit_received。
/// Then 均RecorderUnavailable，随后admit_send仍Ok；输入按值消费，不伪造DecodedSelection。
#[test]
fn test_binding_receive_rejects_unbound_and_explicit_none() {
    binding_test_inner::test_binding_receive_rejects_unbound_and_explicit_none();
}

/// 契约：slice-binding / 关闭优先拒绝接收且真实记录器成功对照可用。
/// @api:src/binding.rs @impl:src/binding_inner.rs @negative
/// Scenario: 关闭优先拒绝接收且真实记录器成功对照可用。
/// Given 已绑定真实记录器的Owner已关闭，以及共享该记录器的新开放对照Owner。
/// When 先向关闭Owner交接非对称Trace的合法未来决定，再向对照Owner交接字节全为a5的不同Trace。
/// Then 关闭Owner返回Closed且发送也Closed；对照返回Ok；宿主关闭并等待Worker退出后选择数精确为1，拒绝Trace的额外发布不能被对照去重掩盖；精确零/一次调用仍需独立证据。
#[test]
fn test_binding_receive_closed_rejection_has_live_recorder_control() {
    binding_test_inner::test_binding_receive_closed_rejection_has_live_recorder_control();
}

/// 契约：slice-binding / 开放Some把合法决定交给真实记录器并返回成功。
/// @api:src/binding.rs @impl:src/binding_inner.rs @critical
/// Scenario: 开放Some把合法决定交给真实记录器并返回成功。
/// Given 真实记录器已安装协调器，Owner已绑定Some，来源为原始非ASCII/组合字符/NUL/空白身份且不同于本地。
/// When 经submit_received交接合法Trace和2100-01-01绝对期限。
/// Then 返回Ok，真实worker在3秒内选择该Trace，notification_attempts=0；不据此声称导出完成或精确调用计数。
#[test]
fn test_binding_receive_publishes_to_real_recorder() {
    binding_test_inner::test_binding_receive_publishes_to_real_recorder();
}

/// 契约：slice-binding / 三个不同合法Trace分别形成独立选择效果。
/// @api:src/binding.rs @impl:src/binding_inner.rs @critical
/// Scenario: 三个不同合法Trace分别形成独立选择效果。
/// Given 开放Some及真实记录器，宿主为非对称Trace、首字节单1、末字节单1分别登记观测/节点-C；来源为同一原始非ASCII身份，绝对期限为当前整秒后30秒且小于60秒窗口。
/// When 逐项经合法协议出口和submit_received交接，并等待真实通知线程交出业务值。
/// Then 各调用Ok，3秒内每条通知的目标均为观测/节点-C、Trace字节及绝对期限精确等于输入，worker选择数依次1/2/3；不把去重统计当apply_remote_selection调用次数。
#[test]
fn test_binding_receive_keeps_distinct_valid_decisions() {
    binding_test_inner::test_binding_receive_keeps_distinct_valid_decisions();
}

/// 契约：slice-binding / 记录器自来源拒绝映射为未发布。
/// @api:src/binding.rs @impl:src/binding_inner.rs @negative
/// Scenario: 记录器自来源拒绝映射为未发布。
/// Given Owner绑定真实记录器，记录器本地身份精确等于原始来源字符串。
/// When 先交接非对称Trace的自来源决定，再以不同来源本地/节点-B交接字节全为a5的不同Trace。
/// Then 第一次EventNotPublished而非Closed或RecorderUnavailable；成功对照Ok；宿主关闭并等待Worker退出后选择数精确为1，没有额外不同Trace选择。
#[test]
fn test_binding_receive_self_source_is_event_not_published() {
    binding_test_inner::test_binding_receive_self_source_is_event_not_published();
}

/// 契约：slice-binding / 协议合法但已过期决定映射为未发布。
/// @api:src/binding.rs @impl:src/binding_inner.rs @negative
/// Scenario: 协议合法但已过期决定映射为未发布。
/// Given 开放Some，期限为Unix纪元的合法DecodedSelection。
/// When 提交非对称Trace的纪元决定，再提交字节全为a5的不同Trace和合法未来期限对照。
/// Then 过期EventNotPublished；未来对照Ok；宿主关闭并等待Worker退出后选择数精确为1，binding不续期或自行解释false唯一原因。
#[test]
fn test_binding_receive_expired_decision_is_event_not_published() {
    binding_test_inner::test_binding_receive_expired_decision_is_event_not_published();
}

/// 契约：slice-binding / Some未安装协调器仍准确返回未发布。
/// @api:src/binding.rs @impl:src/binding_inner.rs @negative
/// Scenario: Some未安装协调器仍准确返回未发布。
/// Given 真实记录器已构建但尚未enable_coordination，Owner已绑定Some。
/// When 先交接非对称Trace的合法未来决定，再由宿主安装协调器并交接字节全为a5的不同Trace。
/// Then 未安装时EventNotPublished；安装后Ok；宿主关闭并等待Worker退出后选择数精确为1，不把Some等同发布已就绪。
#[test]
fn test_binding_receive_uninstalled_recorder_is_event_not_published() {
    binding_test_inner::test_binding_receive_uninstalled_recorder_is_event_not_published();
}

/// 契约：slice-binding / 宿主停止记录器后Some接收返回未发布而发送仍可准入。
/// @api:src/binding.rs @impl:src/binding_inner.rs @negative
/// Scenario: 宿主停止记录器后Some接收返回未发布而发送仍可准入。
/// Given Owner已绑定真实记录器，宿主已通过其Guard执行shutdown。
/// When 调用submit_received并另行调用admit_send。
/// Then 接收EventNotPublished、发送Ok、选择数0；记录器停止不等同binding关闭。
#[test]
fn test_binding_receive_stopped_recorder_is_event_not_published() {
    binding_test_inner::test_binding_receive_stopped_recorder_is_event_not_published();
}

/// 契约：slice-binding / 重复关闭保持终态且不停止宿主记录器。
/// @api:src/binding.rs @impl:src/binding_inner.rs @critical
/// Scenario: 重复关闭保持终态且不停止宿主记录器。
/// Given 未绑定、None、Some三个独立Owner和持续存活的宿主Guard。
/// When 每个Owner连续close两次，再经原句柄操作，最后直接调用宿主记录器作存活对照。
/// Then close均正常返回，发送和接收均Closed；真实记录器仍可发布并形成选择数1。本例只验证记录器共享边界，完整宿主组合归第6块。
#[test]
fn test_binding_close_is_idempotent_and_does_not_stop_host_recorder() {
    binding_test_inner::test_binding_close_is_idempotent_and_does_not_stop_host_recorder();
}

/// 契约：slice-binding / Owner析构后原句柄和克隆均拒绝新操作。
/// @api:src/binding.rs @impl:src/binding_inner.rs @critical
/// Scenario: Owner析构后原句柄和克隆均拒绝新操作。
/// Given 未绑定、None、Some三个独立Owner各有原/克隆弱句柄，宿主Guard存活。
/// When 逐个drop Owner，再经原/克隆句柄发送准入及接收合法决定。
/// Then 全部Closed且宿主记录器仍可发布；临时Arc保活下主动关闭不是本场景已证明的结论。
#[test]
fn test_binding_owner_drop_closes_original_and_cloned_handles() {
    binding_test_inner::test_binding_owner_drop_closes_original_and_cloned_handles();
}

/// 契约：slice-binding / 先发送准入的成功不被随后close撤销。
/// @api:src/binding.rs @impl:src/binding_inner.rs @edgecase
/// Scenario: 先发送准入的成功不被随后close撤销。
/// Given 新Owner，工作线程先取得admit_send的单位结果并以通道确认，调用方后续收尾被通道暂停。
/// When 主线程在确认准入返回后close，再释放工作线程收尾。
/// Then close正常返回，新句柄Closed，工作线程保留原Ok；不创建真实发送替身，也不声称接收内部发布窗口已覆盖。
#[test]
fn test_binding_send_admitted_before_close_keeps_unit_success() {
    binding_test_inner::test_binding_send_admitted_before_close_keeps_unit_success();
}

/// 契约：slice-binding / 关闭完成先于两个并发新操作时双方准确拒绝。
/// @api:src/binding.rs @impl:src/binding_inner.rs @edgecase
/// Scenario: 关闭完成先于两个并发新操作时双方准确拒绝。
/// Given 开放Some、真实记录器及等待Barrier的发送/接收线程。
/// When 主线程先close返回再释放Barrier。
/// Then 两个线程分别返回Closed，停止记录器并排空后选择数0；独立子进程20秒硬截止。
#[test]
fn test_binding_close_before_concurrent_operations_rejects_both() {
    binding_test_inner::test_binding_close_before_concurrent_operations_rejects_both();
}

/// 契约：slice-binding / 一次真实接收关闭竞争只有合同允许的结果。
/// @api:src/binding.rs @impl:src/binding_inner.rs @edgecase
/// Scenario: 一次真实接收关闭竞争只有合同允许的结果。
/// Given 开放Some与真实记录器，一个接收线程和关闭者通过Barrier同时起跑。
/// When 仅执行一次submit_received与close竞争。
/// Then 接收只可Ok或Closed；Ok对应排空后选择数1，Closed对应0；所有后续发送Closed。此场景不保证命中不可见内部窗口。
#[test]
fn test_binding_bounded_receive_close_race_has_only_legal_outcomes() {
    binding_test_inner::test_binding_bounded_receive_close_race_has_only_legal_outcomes();
}

/// 契约：slice-binding / 五种错误显示准确且互不混淆。
/// @api:src/binding.rs @impl:src/binding_inner.rs @critical
/// Scenario: 五种错误显示准确且互不混淆。
/// Given Closed、RecorderAlreadyBound、RecorderUnavailable、EventNotPublished、StatePoisoned五类无资源错误。
/// When 逐一经Display格式化。
/// Then 文本分别表达关闭、已经绑定、记录器不可用、事件未发布、中毒，五类不同且不泄露MutexGuard/PoisonError/BindingInner；不锁定整句标点。
#[test]
fn test_binding_error_display_classifies_five_semantic_errors() {
    binding_test_inner::test_binding_error_display_classifies_five_semantic_errors();
}

/// 契约：slice-binding / 每种错误的格式化失败准确传播。
/// @api:src/binding.rs @impl:src/binding_inner.rs @negative
/// Scenario: 每种错误的格式化失败准确传播。
/// Given 五种错误和在0至输出长度减1容量处失败的fmt::Write目标，以及足量成功对照。
/// When 通过fmt::write执行Display。
/// Then 每个不足容量均Err(fmt::Error)，失败后不继续写，足量容量Ok且完整输出；TODO panic不是格式化错误传播证据。
#[test]
fn test_binding_error_display_propagates_formatter_failure() {
    binding_test_inner::test_binding_error_display_propagates_formatter_failure();
}

/// 契约：slice-binding / 五种错误source始终None。
/// @api:src/binding.rs @impl:src/binding_inner.rs @critical
/// Scenario: 五种错误source始终None。
/// Given 五种冻结BindingError值；直接构造StatePoisoned只用于trait分类。
/// When 分别连续两次调用Error::source。
/// Then 全部None，不暴露底层原因、守卫或内部状态；不据此宣称中毒清理分支已验证。
#[test]
fn test_binding_error_source_is_none_for_all_five_errors() {
    binding_test_inner::test_binding_error_source_is_none_for_all_five_errors();
}

/// 契约：slice-binding / 真实记录器内容观测夹具校准，仅作为前置事实。
/// @api:src/binding.rs @impl:src/binding_inner.rs @edgecase
/// Scenario: 经真实通知线程可以观测交接后的Trace与绝对期限。
/// Given 独立子进程内真实记录器及通知观察器，宿主为非对称Trace登记观测/节点-C，期限为当前整秒后30秒且小于60秒窗口。
/// When 直接向记录器发布该Trace、该期限及不同于本地的原始来源。
/// Then 真实通知线程在3秒内交出目标观测/节点-C、精确Trace字节及原绝对期限，选择数为1；不计作binding实现或精确调用次数证据。
#[test]
fn test_binding_real_recorder_notification_fixture_control() {
    binding_test_inner::test_binding_real_recorder_notification_fixture_control();
}

/// 契约：slice-binding / 不同Trace最终效果校准，不计为binding实现或调用计数证据。
/// @api:src/binding.rs @impl:src/binding_inner.rs @edgecase
/// Scenario: 同Trace重复发布会合并而不同Trace在Worker退出后仍可区分。
/// Given 独立子进程中的真实记录器，非对称Trace及字节全为a5的不同Trace，未来期限和非本地来源。
/// When 直接发布非对称Trace两次、不同Trace一次，不等待选择统计就由宿主关闭并等待Worker退出。
/// Then 三次发布均true，最终selected_traces精确为2、notification_attempts为0；校准本轮有界数据的排空观测，不能将2解释为API调用次数。
#[test]
fn test_binding_real_recorder_distinct_final_effect_control() {
    binding_test_inner::test_binding_real_recorder_distinct_final_effect_control();
}

/// 契约：slice-binding / 双真实记录器夹具校准，不计为binding实现证据。
/// @api:src/binding.rs @impl:src/binding_inner.rs @edgecase
/// Scenario: 两个独立真实记录器按各自来源规则发布且关闭互不影响。
/// Given 通过公开start创建真实A/B及各自Guard/Worker，本地身份分别为本地/节点-B及原始SOURCE，没有Span或关联。
/// When 来源SOURCE的非对称Trace向A发布true、向B自来源发布false；关闭A后向B发布全a5不同Trace且来源本地/节点-B。
/// Then B对照true；双方Worker退出后选择数各1，停止后各自拒绝发布，宿主导出关闭各1且导出调用/通知数均0。
#[test]
fn test_binding_real_recorder_independent_instances_control() {
    binding_test_inner::test_binding_real_recorder_independent_instances_control();
}
