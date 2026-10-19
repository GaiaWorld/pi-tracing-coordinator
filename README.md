# pi-tracing-coordinator

用于跨进程 Trace 上报通知的协调库，复用宿主的通信环境和飞行记录器（FlightRecorder）。协议、类型登记、绑定、收发接入及公开协调器均已实现。

## 提供的能力

- 向宿主指定的目标发送一次上报决定，或通过既有 `TraceNotifier` 接入日志系统。
- 接收消息后完整校验 TraceId、来源和期限，再交给真实记录器。
- 统一管理一次记录器绑定、接收服务安装与本对象关闭。
- 支持 `DefaultCodec` 与明确选层的 `MultiLayerCodec`；每个进程择一适配。

宿主负责目标选择、发送时机、本跳来源、关联关系及通信环境；附件、Span、导出和持久化沿用上游能力。

## 接入顺序

1. 在独占、尚未发布的真实 Fory 类型表上调用 `register_fory_types`，登记成功后再发布。消息类型号固定为 **4300**；同一张表只登记一次，失败后停止发布该候选。
2. 使用配对的 `ServiceRuntime` 和终端构造 `TracingCoordinator::new_default` 或 `TracingCoordinator::new_multi_layer`。多层方式要求真实存在 `COORDINATION_SERVICE_LAYER`，不隐式回退。终端包装类型须实现 `Deref<Target = ServiceTerminal<C>>`，宿主在使用期保留终端强引用。
3. 取得 `notifier()` 并注入宿主日志初始化；随后调用一次 `bind_recorder(Some(recorder))` 或 `bind_recorder(None)`。**None 同样消耗绑定机会**，此时仍可发送，接收会拒绝。
4. 显式调用 `install_service`；使用宿主 inventory 安装流程时，先调用 `bind_service_context`，再由宿主登记并初始化对应服务。两种方式择一，安装、容器变更和关闭须串行。
5. 使用 `send_selection(remote, selection, source)` 指定发送，或由日志系统调用 notifier。结束时调用 `close()`，或释放协调器；宿主自行管理共享终端、运行时及记录器 Guard。

## 使用边界

- 服务逻辑端口为 **4091**，层名为 `tracing-coordinator`；宿主须保证进程内端口唯一。它不是操作系统端口或 Fory 类型号。
- `remote` 必须是可解析的 PeerId 文本；`source` 只要求非空 UTF-8，不裁剪、不规范化，其真实性由宿主保证。notifier 使用构造时保存的本地身份。
- 期限按 Unix 纪元有符号 100ns 计数传输，向下规范化，最多提前 99ns、绝不延后；不自行续期或复制记录器的时效策略。
- 发送 `Ok` 只表示本地受理，不代表远端完成；前置失败零提交，进入提交步骤时只调用一次，不重试、不缓存补发。
- `close` 拒绝新准入，不等待或撤回已经准入的操作，也不关闭宿主共享设施。
- 安装不是事务：已发布上下文后的注册或初始化失败可能保留前项；宿主应关闭/丢弃本候选，避免在旧容器盲目重试。

完整参数、错误、所有权、同步执行成本及生命周期约定见[公开协调器](src/coordinator.rs)、[发送错误](src/client.rs)和[集中登记](src/protocol.rs)。
