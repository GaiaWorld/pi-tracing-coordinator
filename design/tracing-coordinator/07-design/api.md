# tracing-coordinator · 接口面（L7）

**实施状态更新，2026-10-19：** 五模块37个手写实现入口均已完成，原dev-impl七块流程及各切片独立测试/功能审查已结束。L7技术合同、签名/类型/可见性不变。当前验收限分块已有证据与用户最新允许的WSL检查；后续不执行集成、压力、基准或Windows原生构建测试，不把原完整部署矩阵标为通过。见[实施验收矩阵](../../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-matrix)、[稳定出口摘要与现状图](../../../docs/IMPLEMENTATION_API.md#implementation-api)和[当前控制台](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-current)。历史声明期/TDD失败事实仍按当时状态保留。

<a id="api-index"></a>
## 目录

- 当前protocol
  - [理由、符号及边界入口](#api-protocol-typed)
- 当前binding
  - [理由、符号及边界入口](#api-binding-typed)
- 当前client
  - [理由、符号及边界入口](#api-client-typed)
- 当前service
  - [理由、符号及边界入口](#api-service-typed)
- 当前coordinator
  - [理由、符号及边界入口](#api-coordinator)
- [历史已替代条目](#api-history)

<a id="api-coordinator"></a>
## 统一协调器（已落声明，2026-10-16）

当前分块已验收：[稳定出口摘要](../../../docs/IMPLEMENTATION_API.md#api-coordinator) ↔ [独立审查及验收](../../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-coordinator)；[现状组织/依赖/时序](../../../docs/IMPLEMENTATION_API.md#implementation-outline)。以下声明期检查仅保留历史，不替代当前行为证据。

设计/完整源码门禁均确认。完整WHAT只住[src/coordinator.rs](../../../src/coordinator.rs)的TracingCoordinator、CoordinationError/Kind、两个具体构造、六个通用方法及错误trait；HOW只住[十芯](../../../src/coordinator_inner.rs)。根已重导出两个公开类型；无新增依赖。

| 精确符号 | 消费者/理由 |
|---|---|
| TracingCoordinator::new_default / new_multi_layer | 宿主冷路径；两个真实内置codec适配，无新注册trait。 |
| notifier / bind_recorder | 先交既有trait给logger，再一次Some/None绑定，职责不混入logger初始化。 |
| bind_service_context / install_service | inventory/显式二选一；串行安装、部分失败真实返回。 |
| send_selection / close | 单向委托已确认client/binding出口，不保存新状态。 |
| CoordinationError Display / Error | 不透明真实分类/直接原因，不开放资源或Kind。 |

选择固定四字段（binding首位）和自动析构，不再造Drop芯；原始字段/签名/所有权/错误/串行前提/失败后状态/成本见壳。实际类型表核对方法也在两个构造注释：默认当前快照、多层明确has_layer/layer_fory、稳定配置后预绑定。注册非事务、不注销/删context，宿主错误后close/丢弃候选；不将泛型相同或选层Ok当环境/登记证明。

WSL同命令前后CARGO_EXITCODE=0、Finished6.06s/9.05s，本库44/45警告，pi_service均4既有警告；2026-10-16声明批次结束时37手写芯全TODO。完整功能、安全、资源和实际装配均待后续验证。[表示](data.md#data-coordinator)、[十芯](units.md#units-coordinator)、[装配图源](call-chain.md#chain-coordinator)、[验证](errors.md#errors-coordinator)、[恢复](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-current)。

<a id="api-service-typed"></a>
## 类型化接入第四批：标准宏接收服务（已落声明，2026-10-16）

当前分块已验收：[稳定出口摘要](../../../docs/IMPLEMENTATION_API.md#api-service) ↔ [独立审查及验收](../../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-service)；[现状组织/依赖/时序](../../../docs/IMPLEMENTATION_API.md#implementation-outline)。以下声明期检查仅保留历史，不替代当前行为证据。

设计/完整源码门禁均确认，完整WHAT在[src/service.rs](../../../src/service.rs)，HOW在[四芯](../../../src/service_inner.rs)。精确符号：CoordinationServiceContext::new、default_service、multi_layer_service、receive_selection_default/multi_layer（共用receive_selection芯）及COORDINATION_SERVICE_LAYER。两个宏生成类型/params/static均私有；不暴露codec槽或状态。

元信息id=/observability/tracing/coordinator（框架规范化svc://local/observability/tracing/coordinator）、layer=tracing-coordinator、version=0.1.0、send/4091已确认。选择两具体codec薄适配共用业务芯、不写Service实现；context弱持binding、生成params保留上下文/codec，注册/注入/失败收尾由coordinator编排。完整参数/失败/日志/同步poll/资源/成本合同见壳；当前实施验收见本册顶部。

已写L5选定futures 0.3/default-features=false/alloc；根锁仅增加直接futures，无版本变化。WSL同命令前基线CARGO_EXITCODE=0、6.11s、39警告，后0、9.90s、44警告；pi_service均4既有警告。该历史批次为27手写芯；2026-10-16声明批次结束时全库37芯全TODO，宏生成真实代码但未运行。[表示](data.md#data-service-typed)、[四芯](units.md#units-service-typed)、[图源](call-chain.md#chain-service-typed)、[验证](errors.md#errors-service-typed)。

<a id="api-client-typed"></a>
## 类型化接入第三批：客户端替代接口（已落声明，2026-10-16）

当前分块已验收：[稳定出口摘要](../../../docs/IMPLEMENTATION_API.md#api-client) ↔ [独立审查及验收](../../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-client)；[现状组织/依赖/时序](../../../docs/IMPLEMENTATION_API.md#implementation-outline)。以下声明期检查仅保留历史，不替代当前行为证据。

用户设计及完整源码门禁均确认。完整WHAT已迁[src/client.rs](../../../src/client.rs)的CoordinationClient、CoordinationSendError/私有Kind及new/send_selection/TraceNotifier::notify/Display::fmt/Error::source；HOW在[私有芯](../../../src/client_inner.rs)五个同名接缝。本册不另留完整规范。

| 精确符号 | 可见性/理由与消费者 |
|---|---|
| CoordinationClient<T,C> / new | 库内；coordinator移交预绑定Arc<C>，new(terminal,codec,binding,local)，client无需C:Clone。 |
| send_selection / TraceNotifier::notify | 库内方法/既有trait；共用准入→终端→目标→业务转换→default_encode→send；保存source责任及一次提交。 |
| CoordinationSendError / Kind | 公开不透明/私有；独立Encode(ServiceError)，保留宿主实际错误粒度。 |
| Display / Error::source | trait；真实六阶段和直接源，不额外持资源或完整输入。 |

选择Arc持有而非每次查runtime，代价是notifier延长codec持有；不增加trait、直接网络依赖或客户端Clone/Drop/Debug。完整所有权、错误、并发及成本见壳；[表示](data.md#data-client-typed)、[五芯](units.md#units-client-typed)、[图源](call-chain.md#chain-client-typed)、[验证](errors.md#errors-client-typed)。WSL同命令前后CARGO_EXITCODE=0，Finished 6.13s/8.58s，本库均39警告/pi_service4既有警告；该历史批次为23芯；2026-10-16声明批次结束时全库37芯仍TODO，无行为验收。

<a id="api-binding-typed"></a>
## 类型化接入第二批：binding去资源接口（已落声明，2026-10-16）

当前分块已验收：[稳定出口摘要](../../../docs/IMPLEMENTATION_API.md#api-binding) ↔ [独立审查及验收](../../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-binding)；[现状组织/依赖/时序](../../../docs/IMPLEMENTATION_API.md#implementation-outline)。以下声明期检查仅保留历史，不替代当前行为证据。

用户完整设计门禁及完整源码门禁均确认，已落[src/binding.rs](../../../src/binding.rs)，HOW仅住[私有芯](../../../src/binding_inner.rs)。本节不重复完整WHAT。

| 精确符号 | 可见性/芯 | 选择理由与消费者 |
|---|---|---|
| CoordinationBinding::new | 库内/new | coordinator创建独立状态；无协议资源参数。 |
| CoordinationBinding::handle | 库内/handle | client/service只取得受限弱接入。 |
| CoordinationBinding::bind_recorder / close | 库内/bind_recorder、close | 统一所有者管理一次绑定与终态，不让调用方拼接裸状态。 |
| CoordinationHandle::admit_send | 库内/admit_send | client只取得本次准入，返回单位结果，无token/资源。 |
| CoordinationHandle::submit_received | 库内/submit_received | service交接protocol已校验结果，锁外一次发布。 |
| Handle Clone / Owner Drop | trait/clone_handle、drop_binding | 弱接入可复制，生命周期所有权不可复制，Owner主动关闭。 |
| BindingError Display / Error | trait/fmt_binding_error、binding_error_source | 真实语义分类、不泄露守卫或输入。 |

完整参数、返回、失败状态、顺序、并发、所有权、关闭/中毒/成本合同见上述壳类型/方法注释。PreparedProtocol相关字段和准备缓存/入口已从源码撤回，不保留临时桥接资源。关联：[表示](data.md#data-binding-typed)、[十芯](units.md#units-binding-typed)、[图源](call-chain.md#chain-binding-typed)、[验证](errors.md#errors-binding-typed)。

<a id="api-protocol-typed"></a>
## 类型化接入第一批：protocol替代接口（已落声明，2026-10-16）

当前分块已验收：[稳定出口摘要](../../../docs/IMPLEMENTATION_API.md#api-protocol) ↔ [独立审查及验收](../../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-protocol)；[现状组织/依赖/时序](../../../docs/IMPLEMENTATION_API.md#implementation-outline)。以下声明期检查仅保留历史，不替代当前行为证据。

第1块7芯及第2块集中登记均已实现并完成分块验收；[七出口摘要](../../../docs/SLICE_PROTOCOL_API.md#slice-protocol-api)逐项回链本节与[验收矩阵](../../../docs/SLICE_PROTOCOL_ACCEPTANCE.md#slice-protocol-matrix)。用户完整设计及源码门禁均确认；完整WHAT已迁[src/protocol.rs](../../../src/protocol.rs)，HOW只住[私有芯](../../../src/protocol_inner.rs)。根[src/lib.rs](../../../src/lib.rs)直接重导出集中登记，不经coordinator包装Fory参数。

<a id="api-register-typed"></a>
### 集中登记定位

register_fory_types为公开函数，真实签名、独占未发布候选、共表/固定4300、非幂等、失败不发布、panic/成本合同见壳同名条目；唯一清单避免两端漂移。内置登记号常量仍私有；宿主无需命名SelectionMessage。芯同名，已实现并验收；见[登记摘要](../../../docs/IMPLEMENTATION_API.md#api-registration)与[登记验收](../../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-registration)。

<a id="api-selection-to-message"></a>
### 发送转换定位

selection_to_message为库内自由函数，完整selection/source参数、合法域、校验顺序、所有权、失败状态及复杂度见壳同名条目；芯同名。把业务转换从codec编码分离，client只消费消息，不复制校验规则。

<a id="api-validate-selection-message"></a>
### 接收校验定位

validate_selection_message为库内自由函数，拥有式消息、严格UTF-8/Trace/期限、结果构造权限与失败消费合同见壳同名条目；芯同名。标准宏负责前置解码，本入口建立交接隔离带。

### 交接、错误与迁移

DecodedSelection::into_parts、ProtocolError Display/Error及两个期限私有函数完整WHAT见壳，芯为into_parts、fmt_protocol_error、protocol_error_source及两个期限同名函数。选择窄自由函数而不是无资源协议对象或新trait；消息仅库内可见、字段私有，宿主公开面只增加集中登记。

PreparedProtocol、ProtocolPrepareError/Kind、check_protocol_candidate、registration_failed及旧准备trait芯已从源码移除；ProtocolError::Codec已撤回，client宿主编码已由独立Encode(ServiceError)分类；五模块替代声明均已落地，当前合同以上方源码指针为准。

本批修改后WSL指定nightly的check -p pi-tracing-coordinator --lib --locked --offline完整回执为Finished、9.40s、0错误、本库39个未消费声明警告及pi_service4个既有警告。修改前检查已调用，但回执截断，未保存其Cargo退出码/Finished/耗时；此前6.45s和“前后均通过”缺少完整证据，现撤销该表述。历史完整快照为6.31s/50警告，不能替代本批修改前基线。该历史批次为23芯；2026-10-16本批结束时全库37芯均TODO，不是功能/安全/生命周期验收。关联：[表示](data.md#data-protocol-typed)、[八芯](units.md#units-protocol-typed)、[图源](call-chain.md#chain-protocol-typed)、[错误/验证](errors.md#errors-protocol-typed)、[恢复](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-current)。

<a id="api-state"></a>
## 当前声明状态

五模块11源码/37芯均已实现；公开出口由[src/lib.rs](../../../src/lib.rs)汇总。生产直接依赖仍7项，后续测试增加的固定开发依赖及锁扩展见当前控制台。各块行为、独立审查与验收见顶部矩阵；不再以历史声明check描述当前实现。最终[切片](call-chain.md#implementation-slices)是实现阶段的顺序输入；独立L9、R10复核及用户结论确认均已完成，见[收口记录](../09-review.md#l9-design-closure)；未变更技术合同。

<a id="api-history"></a>
## 已替代历史入口

以下仅保留旧锚点兼容跳转，不是当前待办、源码符号存在证明或新实现要求。完整原文及收束前指纹见[历史档案](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#history-api)。

<a id="api-round-one"></a>
- 历史`api-round-one`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#api-round-one)；现行以上方当前条目为准。

<a id="api-prepare-protocol"></a>
- 历史`api-prepare-protocol`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#api-prepare-protocol)；现行以上方当前条目为准。

<a id="api-register-types"></a>
- 历史`api-register-types`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#api-register-types)；现行以上方当前条目为准。

<a id="api-candidate-check"></a>
- 历史`api-candidate-check`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#api-candidate-check)；现行以上方当前条目为准。

<a id="api-selection-conversion"></a>
- 历史`api-selection-conversion`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#api-selection-conversion)；现行以上方当前条目为准。

<a id="api-round-two"></a>
- 历史`api-round-two`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#api-round-two)；现行以上方当前条目为准。

<a id="api-pending-delta"></a>
- 历史`api-pending-delta`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#api-pending-delta)；现行以上方当前条目为准。

<a id="api-protocol-delta"></a>
- 历史`api-protocol-delta`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#api-protocol-delta)；现行以上方当前条目为准。

<a id="api-binding-delta"></a>
- 历史`api-binding-delta`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#api-binding-delta)；现行以上方当前条目为准。

<a id="api-client-delta"></a>
- 历史`api-client-delta`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#api-client-delta)；现行以上方当前条目为准。

<a id="api-client-new"></a>
- 历史`api-client-new`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#api-client-new)；现行以上方当前条目为准。

<a id="api-client-send"></a>
- 历史`api-client-send`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#api-client-send)；现行以上方当前条目为准。

<a id="api-client-notify"></a>
- 历史`api-client-notify`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#api-client-notify)；现行以上方当前条目为准。

<a id="api-client-error-traits"></a>
- 历史`api-client-error-traits`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#api-client-error-traits)；现行以上方当前条目为准。

关联：[设计索引](../00-index.md)、[L7全链](call-chain.md#chain-final)、[唯一开发上下文](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-current)。
