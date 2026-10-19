# tracing-coordinator · 单元与实现预期（L7）

**实施状态更新，2026-10-19：** 五模块37个手写实现入口均已完成，原dev-impl七块流程及各切片独立测试/功能审查已结束。L7技术合同、签名/类型/可见性不变。当前验收限分块已有证据与用户最新允许的WSL检查；后续不执行集成、压力、基准或Windows原生构建测试，不把原完整部署矩阵标为通过。见[实施验收矩阵](../../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-matrix)、[稳定出口摘要与现状图](../../../docs/IMPLEMENTATION_API.md#implementation-api)和[当前控制台](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-current)。历史声明期/TDD失败事实仍按当时状态保留。

<a id="units-index"></a>
## 目录

- 当前protocol
  - [理由、符号及边界入口](#units-protocol-typed)
- 当前binding
  - [理由、符号及边界入口](#units-binding-typed)
- 当前client
  - [理由、符号及边界入口](#units-client-typed)
- 当前service
  - [理由、符号及边界入口](#units-service-typed)
- 当前coordinator
  - [理由、符号及边界入口](#units-coordinator)
- [历史已替代条目](#units-history)

<a id="units-coordinator"></a>
## 统一协调器十芯（已落声明，2026-10-16）

完整HOW只住[src/coordinator_inner.rs](../../../src/coordinator_inner.rs)，WHAT在[壳](../../../src/coordinator.rs)。精确芯new_default/new_multi_layer/notifier/bind_recorder/bind_service_context/install_service/send_selection/close/fmt_coordination_error/coordination_error_source，均为pub(super)实现，内部委托沿冻结方向。两个构造对应真实内置codec，其余通用约束不需C:Clone；无独立协调器Drop，纯类型无芯。消费者均为公开协调入口，错误trait为该入口使用。总37芯均已实现；coordinator验收见[矩阵](../../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-coordinator)及[出口摘要](../../../docs/IMPLEMENTATION_API.md#api-coordinator)。[接口](api.md#api-coordinator)、[表示](data.md#data-coordinator)、[图源](call-chain.md#chain-coordinator)、[验证](errors.md#errors-coordinator)。

<a id="units-service-typed"></a>
## 类型化接入第四批：服务四芯（已落声明，2026-10-16）

完整HOW只住[src/service_inner.rs](../../../src/service_inner.rs)，WHAT在[壳](../../../src/service.rs)。精确芯new_context/default_service/multi_layer_service/receive_selection，均pub(super)且已实现；两个宏业务函数纯委托同一处理芯。对应消费者为coordinator装配及框架typed入口，类型不造芯。该历史批次为27芯，声明检查通过；该声明批次结束时全库37芯均无功能证据；本块最新状态见头部更新。[接口](api.md#api-service-typed)、[表示](data.md#data-service-typed)、[图源](call-chain.md#chain-service-typed)、[验证](errors.md#errors-service-typed)。

<a id="units-client-typed"></a>
## 类型化接入第三批：客户端五芯（已落声明，2026-10-16）

完整HOW只住[src/client_inner.rs](../../../src/client_inner.rs)，WHAT在[src/client.rs](../../../src/client.rs)。精确芯new/send_selection/notify/fmt_send_error/send_error_source，均pub(super)且已实现；new第二参数Arc<C>。类型无芯、无假接线。五芯隐藏构造/一次提交/错误优先级和trait适配，不引入新层。该历史批次结合protocol8/binding10为23芯；当前全库37芯，声明检查0错误，不是算法证据。[接口](api.md#api-client-typed)、[表示](data.md#data-client-typed)、[时序](call-chain.md#chain-client-typed)、[验证](errors.md#errors-client-typed)。

<a id="units-binding-typed"></a>
## 类型化接入第二批：binding十芯（已落声明，2026-10-16）

完整HOW只住[src/binding_inner.rs](../../../src/binding_inner.rs)，WHAT在[壳](../../../src/binding.rs)。十芯均pub(super)且已实现；纯类型不造芯。

| 精确芯符号 | 壳/理由与消费者 |
|---|---|
| new | CoordinationBinding::new，无资源参数；coordinator构造独立状态。 |
| handle / clone_handle | Owner handle/Handle Clone，窄弱接入可复制、所有权不可复制。 |
| bind_recorder | 一次Some/None与锁外输入释放。 |
| admit_send / submit_received | client准入单位结果/service合法交接，不反调上层。 |
| close / drop_binding | 同锁终态、锁外释放、Owner主动关闭。 |
| fmt_binding_error / binding_error_source | 真实分类与无底层源，避免守卫泄露。 |

initialize_protocol/prepare_protocol两芯已移除。行为及内部属性已按真实测试与独立源码审查联合取证；见[binding验收](../../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-binding)。关联：[接口](api.md#api-binding-typed)、[表示](data.md#data-binding-typed)、[时序](call-chain.md#chain-binding-typed)、[错误验证](errors.md#errors-binding-typed)、[当前状态](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-current)。

<a id="units-protocol-typed"></a>
## 类型化接入第一批：protocol八芯（已落声明，2026-10-16）

完整HOW只住[src/protocol_inner.rs](../../../src/protocol_inner.rs)，WHAT在[壳](../../../src/protocol.rs)。protocol八芯均已实现，集中登记独立于业务转换；不反调其它模块。[逐芯验收](../../../docs/SLICE_PROTOCOL_ACCEPTANCE.md#slice-protocol-matrix)与[稳定出口](../../../docs/SLICE_PROTOCOL_API.md#slice-protocol-api)。

| 精确芯符号 | 壳/理由与消费者 |
|---|---|
| deadline_to_100ns / deadline_from_100ns | 两个私有期限壳，共同域/向下规范化/检查式还原。 |
| register_fory_types | 公开集中登记，宿主组合清单，根直接重导出。 |
| selection_to_message | client消费，业务转换与codec分离。 |
| validate_selection_message | service消费，宏解码后建立合法交接隔离带。 |
| into_parts | DecodedSelection消费出口，binding锁外移动。 |
| fmt_protocol_error / protocol_error_source | 五类业务错误显示和实际UTF-8源。 |

check_protocol_candidate、registration_failed及准备错误两个trait芯已移除。本批当时结合binding十芯及client五芯，共23手写芯全TODO；当前完整五模块为37芯。修改后完整声明检查Finished、9.40s、0错误、本库39警告；修改前回执截断，基线证据不完整，不能用历史50警告或无依据6.45s补齐。编译不证明算法或资源行为。client宿主codec实际持有和编码错误已按后续门禁替代；不建临时协议资源。

关联：[接口](api.md#api-protocol-typed)、[表示](data.md#data-protocol-typed)、[图源](call-chain.md#chain-protocol-typed)、[错误/验证](errors.md#errors-protocol-typed)、[当前状态](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-current)。

<a id="units-history"></a>
## 已替代历史入口

以下仅保留旧锚点兼容跳转，不是当前待办、源码符号存在证明或新实现要求。完整原文及收束前指纹见[历史档案](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#history-units)。

<a id="units-round-one"></a>
- 历史`units-round-one`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#units-round-one)；现行以上方当前条目为准。

<a id="pseudo-prepare"></a>
- 历史`pseudo-prepare`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#pseudo-prepare)；现行以上方当前条目为准。

<a id="pseudo-candidate"></a>
- 历史`pseudo-candidate`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#pseudo-candidate)；现行以上方当前条目为准。

<a id="pseudo-deadline"></a>
- 历史`pseudo-deadline`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#pseudo-deadline)；现行以上方当前条目为准。

<a id="pseudo-convert"></a>
- 历史`pseudo-convert`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#pseudo-convert)；现行以上方当前条目为准。

<a id="units-round-two"></a>
- 历史`units-round-two`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#units-round-two)；现行以上方当前条目为准。

<a id="pseudo-create"></a>
- 历史`pseudo-create`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#pseudo-create)；现行以上方当前条目为准。

<a id="pseudo-bind"></a>
- 历史`pseudo-bind`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#pseudo-bind)；现行以上方当前条目为准。

<a id="pseudo-admit"></a>
- 历史`pseudo-admit`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#pseudo-admit)；现行以上方当前条目为准。

<a id="pseudo-submit"></a>
- 历史`pseudo-submit`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#pseudo-submit)；现行以上方当前条目为准。

<a id="pseudo-close"></a>
- 历史`pseudo-close`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#pseudo-close)；现行以上方当前条目为准。

<a id="units-persistence"></a>
- 历史`units-persistence`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#units-persistence)；现行以上方当前条目为准。

<a id="units-protocol-delta"></a>
- 历史`units-protocol-delta`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#units-protocol-delta)；现行以上方当前条目为准。

<a id="units-binding-delta"></a>
- 历史`units-binding-delta`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#units-binding-delta)；现行以上方当前条目为准。

<a id="units-client-delta"></a>
- 历史`units-client-delta`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#units-client-delta)；现行以上方当前条目为准。

关联：[设计索引](../00-index.md)、[L7全链](call-chain.md#chain-final)、[唯一开发上下文](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-current)。
