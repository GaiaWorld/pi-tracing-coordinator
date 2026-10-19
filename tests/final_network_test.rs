//! slice-final R1：真实QUIC角色的声明面，仅同名透传。
//! @api:src/coordinator.rs @impl:src/coordinator_inner.rs @test:tests/final_network_test.rs
//! @design-doc:design/tracing-coordinator/07-design/call-chain.md#slice-final

#[path = "final_network/final_network_test_inner.rs"]
mod final_network_test_inner;

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: network_default_three_processes_receive_and_forward
/// Given A/B/C为三个真实OS进程，各有独立密钥/UDP端口和唯一Default接收4091；真实宿主登记B的A/C、C的B/A、A的C邻居。
/// When 宿主在角色就绪后仅由A公开发送Trace0x71、共同未来整秒期限到B，后续均由真实记录器通知生产notifier。
/// Then 真实QUIC链A→B→C→A使每端实际选择1；逐跳原字节source=A/B/C及Trace/期限保真，B/C真实回调各Ok一次且不回来源，A无额外通知。同步受理不代替独立接收事实；原静默后A关闭发送准入再登记新探针邻居，真实Worker必须回调原0x71/精确期限并由生产notifier返回Closed，错Trace/期限±100ns负对照拒绝且零追加提交；全角色实际退出。
#[test]
fn test_final_network_default_three_processes_receive_and_forward() {
    final_network_test_inner::test_final_network_default_three_processes_receive_and_forward();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @critical
/// Scenario: network_multi_three_processes_receive_and_forward
/// Given A/B/C三个真实OS进程各在明确tracing-coordinator层登记4300，默认层未登记，独立QUIC宿主和唯一Multi接收4091；邻居与Default场景相同但不共享状态。
/// When 唯一宿主调用由A向B发Trace0x71，真实记录器继续传播C及A。
/// Then A/B/C各实际选择1；三个真实出口的Trace/期限和source=A/B/C逐值相同于预期，B/C回调各成功一次、不向来源回送；无默认层回退、无手工on_send推进。原静默后A仅以新邻居触发真实已保存决定的回调，精确核对0x71/期限/Closed，错Trace和±100ns负对照拒绝、零追加提交。只证明当前同机QUIC装配，不外推跨主机或其它平台。
#[test]
fn test_final_network_multi_three_processes_receive_and_forward() {
    final_network_test_inner::test_final_network_multi_three_processes_receive_and_forward();
}

/// 契约：slice-final
/// @api:src/coordinator.rs @impl:src/coordinator_inner.rs @negative
/// Scenario: network_async_failure_preserves_host_granularity
/// Given A/B两个真实QUIC进程已就绪，另一个新合法PeerId E没有地址且未登记为邻居，A订阅真实宿主Error事件。
/// When A公开发送Trace0x72到E被同步受理，等真实Swarm无地址拨号失败后，再以独立Trace0x73发B作有效控制。
/// Then A实际观察E/Quic/完整NoAddresses且丢弃1条的原始错误；错peer/传输/原因负对照拒绝。未知目标只提交一次、不重试，控制B真实选择1后关闭发送准入并登记新邻居；真实Worker回调必须是0x73及共同精确期限，错Trace/期限±100ns负对照拒绝、生产Closed且零追加提交；B控制屏障写入实际回调值、A逐值核对，不增加产品逐Trace回执或把同步Ok当送达；两个角色实际退出。
#[test]
fn test_final_network_async_failure_preserves_host_granularity() {
    final_network_test_inner::test_final_network_async_failure_preserves_host_granularity();
}
