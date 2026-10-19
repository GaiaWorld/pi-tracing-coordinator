# tracing-coordinator · 数据结构与不变量（L7）

**实施状态更新，2026-10-19：** 五模块37个手写实现入口均已完成，原dev-impl七块流程及各切片独立测试/功能审查已结束。L7技术合同、签名/类型/可见性不变。当前验收限分块已有证据与用户最新允许的WSL检查；后续不执行集成、压力、基准或Windows原生构建测试，不把原完整部署矩阵标为通过。见[实施验收矩阵](../../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-matrix)、[稳定出口摘要与现状图](../../../docs/IMPLEMENTATION_API.md#implementation-api)和[当前控制台](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-current)。历史声明期/TDD失败事实仍按当时状态保留。

<a id="data-index"></a>
## 目录

- 当前protocol
  - [理由、符号及边界入口](#data-protocol-typed)
- 当前binding
  - [理由、符号及边界入口](#data-binding-typed)
- 当前client
  - [理由、符号及边界入口](#data-client-typed)
- 当前service
  - [理由、符号及边界入口](#data-service-typed)
- 当前coordinator
  - [理由、符号及边界入口](#data-coordinator)
- [当前期限理由](#data-deadline)
- [历史已替代条目](#data-history)

<a id="data-coordinator"></a>
## 统一协调器表示（已落声明，2026-10-16）

完整字段/表示/不变量/所有权只住[src/coordinator.rs](../../../src/coordinator.rs)的TracingCoordinator、CoordinationError及私有Kind。选择binding首字段、client/context Arc、具体静态Service借用，自动先关闭binding再释放其它字段，不另造Drop/安装状态机。notifier只强client，client/context不强binding/终端，params可延长context/codec，不推全宿主无环或TLS清空。错误私有四类，不额外持资源。完整合同见壳，[接口](api.md#api-coordinator)、[十芯](units.md#units-coordinator)、[图源](call-chain.md#chain-coordinator)、[验证](errors.md#errors-coordinator)。

<a id="data-service-typed"></a>
## 类型化接入第四批：服务表示（已落声明，2026-10-16）

完整WHAT只住[src/service.rs](../../../src/service.rs)的CoordinationServiceContext、宏函数及常量。Context只弱binding；生成params私有，强持context/codec而不提供可写出口。两具体适配共用消息/处理芯，无新错误枚举。选择理由是复用真实宏具体codec出口、避免第二套协议资源；params生命期和TLS回收不同，详见壳合同。[接口](api.md#api-service-typed)、[四芯](units.md#units-service-typed)、[图源](call-chain.md#chain-service-typed)、[验证](errors.md#errors-service-typed)。

<a id="data-client-typed"></a>
## 类型化接入第三批：客户端表示（已落声明，2026-10-16）

完整字段/类型/不变量/资源生命周期只住[src/client.rs](../../../src/client.rs)的CoordinationClient、CoordinationSendError和私有Kind。本册保留表示理由：Weak<T>/CoordinationHandle保持宿主与binding所有权；Arc<C>实际保存冷路径预绑定codec、取代PhantomData；local保存本跳来源。六类错误独立Encode与Submit，隐藏Kind、不公开可写资源出口。

notifier可延长codec持有、TLS回收与对象释放不同、Default/Multi替换/快照边界的完整合同见壳类型及方法。已完成分块实现、行为验证及独立审查，仍不推任意C或全宿主无环。[接口](api.md#api-client-typed)、[五芯](units.md#units-client-typed)、[图源](call-chain.md#chain-client-typed)、[验证](errors.md#errors-client-typed)、[L5持有](../05-modules/deps.md#ownership)。

<a id="data-binding-typed"></a>
## 类型化接入第二批：binding表示（已落声明，2026-10-16）

完整WHAT只住[src/binding.rs](../../../src/binding.rs)的CoordinationBinding、CoordinationHandle、BindingInner、BindingState、BindingError及各方法注释。原protocol字段及准备缓存已移除；本册不保留第二套结构定义。

| 表示符号 | 理由与消费方 |
|---|---|
| CoordinationBinding / Arc<BindingInner> | coordinator独占生命周期，临时Arc不能推迟Owner主动关闭。 |
| CoordinationHandle / Weak<BindingInner> | client/service窄操作，不以共享对象泄露状态。 |
| BindingState / Mutex | 同锁裁决一次Some/None绑定、准入和关闭，避免多裸字段状态拼接。 |
| BindingError | 五类语义错误，不保存PoisonError/守卫，具体源链见壳trait合同。 |

HOW在[十芯](units.md#units-binding-typed)，[时序](call-chain.md#chain-binding-typed)与[验证](errors.md#errors-binding-typed)映射真实边界。十芯已实现且独立审查闭合，见[binding验收](../../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-binding)与[出口摘要](../../../docs/IMPLEMENTATION_API.md#api-binding)；不以Arc/Weak推出整个宿主无环或无泄漏。

<a id="data-protocol-typed"></a>
## 类型化接入第一批：protocol表示（已落声明，2026-10-16）

第1块类型/常量由合法域、保真及错误场景保护，见[摘要](../../../docs/SLICE_PROTOCOL_API.md#sp-types)与[验收](../../../docs/SLICE_PROTOCOL_ACCEPTANCE.md#slice-protocol-matrix)。完整表示、字段、合法域、所有权和可见性只住[src/protocol.rs](../../../src/protocol.rs)，不再复写定义。

| 精确符号 | 表示选择理由 |
|---|---|
| SELECTION_TYPE_ID / SelectionMessage | 固定4300；消息库内可命名满足标准宏，字段私有。三字段保持原业务表示，source字节在业务入口严格校验。 |
| DecodedSelection / into_parts | 待校验表示与合法交接分开，binding无需重新解释字段；保留原名，消费移动、不开放字段。 |
| ProtocolError / DeadlineError | 业务错误与codec错误分层，UTF-8保留实际原因而不滞留输入；期限沿已冻结私有转换。 |

PreparedProtocol/ProtocolPrepareError及准备样本已撤回；protocol无codec资源。登记/转换签名定位见[接口](api.md#api-protocol-typed)，期限来源继续见[期限理由](#data-deadline)。4301–4310仅[文档预留](../../../docs/FORY_TYPE_ID_ALLOCATION_ARCHIVE_20261016.md#fory-allocation-contract)，不建占位类型。

关联：[八芯](units.md#units-protocol-typed)、[图源](call-chain.md#chain-protocol-typed)、[错误/验证](errors.md#errors-protocol-typed)、[L5持有](../05-modules/deps.md#ownership)、[恢复](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-current)。

<a id="data-deadline"></a>
## 期限精确不变量

**2026-10-15首批已落声明。** 期限的权威完整契约、边界常量、DeadlineError及双向签名见[src/protocol.rs](../../../src/protocol.rs)中的MIN_DEADLINE_100NS、MAX_DEADLINE_100NS、DeadlineError、deadline_to_100ns、deadline_from_100ns；实现预期见[src/protocol_inner.rs](../../../src/protocol_inner.rs)同名芯函数。两个期限芯已实现并通过目标两平台定向测试；[摘要](../../../docs/SLICE_PROTOCOL_API.md#sp-deadlines)。

选择理由：Windows FILETIME纪元到Unix纪元偏移为116444736000000000个100ns，共同下界来自1601起点、上界来自i64::MAX减偏移；Linux即使表示更宽也要与Windows互通。向下规范化避免延后宿主决定，宽整数及检查式运算避免平台布局/溢出依赖。证据来自指定nightly标准库源码和整数推导；原设计批次WSL编译仅证明声明引用网；2026-10-17期限两平台行为证据见[验收矩阵](../../../docs/SLICE_PROTOCOL_ACCEPTANCE.md#slice-protocol-matrix)。


<a id="data-history"></a>
## 已替代历史入口

以下仅保留旧锚点兼容跳转，不是当前待办、源码符号存在证明或新实现要求。完整原文及收束前指纹见[历史档案](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#history-data)。

<a id="data-round-one"></a>
- 历史`data-round-one`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#data-round-one)；现行以上方当前条目为准。

<a id="data-round-two"></a>
- 历史`data-round-two`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#data-round-two)；现行以上方当前条目为准。

<a id="data-errors"></a>
- 历史`data-errors`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#data-errors)；现行以上方当前条目为准。

<a id="data-pending"></a>
- 历史`data-pending`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#data-pending)；现行以上方当前条目为准。

<a id="data-protocol-delta"></a>
- 历史`data-protocol-delta`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#data-protocol-delta)；现行以上方当前条目为准。

<a id="data-binding-delta"></a>
- 历史`data-binding-delta`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#data-binding-delta)；现行以上方当前条目为准。

<a id="data-client-delta"></a>
- 历史`data-client-delta`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#data-client-delta)；现行以上方当前条目为准。

关联：[设计索引](../00-index.md)、[L7全链](call-chain.md#chain-final)、[唯一开发上下文](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-current)。
