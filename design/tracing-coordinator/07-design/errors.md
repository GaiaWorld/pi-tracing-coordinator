# tracing-coordinator · 防御与错误（L7）

**实施状态更新，2026-10-19：** 五模块37个手写实现入口均已完成，原dev-impl七块流程及各切片独立测试/功能审查已结束。L7技术合同、签名/类型/可见性不变。当前验收限分块已有证据与用户最新允许的WSL检查；后续不执行集成、压力、基准或Windows原生构建测试，不把原完整部署矩阵标为通过。见[实施验收矩阵](../../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-matrix)、[稳定出口摘要与现状图](../../../docs/IMPLEMENTATION_API.md#implementation-api)和[当前控制台](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-current)。历史声明期/TDD失败事实仍按当时状态保留。

<a id="errors-index"></a>
## 目录

- 当前protocol
  - [理由、符号及边界入口](#errors-protocol-typed)
- 当前binding
  - [理由、符号及边界入口](#errors-binding-typed)
- 当前client
  - [理由、符号及边界入口](#errors-client-typed)
- 当前service
  - [理由、符号及边界入口](#errors-service-typed)
- 当前coordinator
  - [理由、符号及边界入口](#errors-coordinator)
- [最终错误与资源责任](#errors-final)
- [唯一验证落点](#errors-verification-ownership)
- [历史已替代条目](#errors-history)

<a id="errors-coordinator"></a>
## 统一协调器错误与验证（已落声明，2026-10-16）

不透明CoordinationError及四类私有原因见[表示](data.md#data-coordinator)，完整失败/源链/安装合同见[接口](api.md#api-coordinator)。普通策略仅返回真实错误；显式发送直接沿CoordinationSendError。宿主处理安装错误并close/丢弃候选，不自动rollback/换号/吞错。已有context拒绝保持原值，部分发布错误保留已完成前项，bind_params与init失败状态不可混同。

唯一归属见[第6块逐符号表](call-chain.md#slice-coordinator)，薄委托及根出口由同块真实公开装配保护。

原验证计划：两构造、缺层明确拒绝不fallback、真实类型表/预绑定、runtime终端匹配前提、notifier共享和弱生命周期、Some/None/重复绑定、显式及inventory装配、重复context、4091真实冲突、部分失败状态、close/自动字段析构顺序、保留共享环境。对应分块验证及独立审查现已完成，见[coordinator验收](../../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-coordinator)；静态声明check不替代行为；覆盖率/CRAP禁止。

来源为已核pi_service0.6.2 codec_for_layer/has_layer/layer_fory/ServiceContexts和Registry register/init；无按owner删除上下文或静态注销，串行安装是合法调用前提不是新的并发原子实现。[装配图](call-chain.md#chain-coordinator)、[十芯](units.md#units-coordinator)、[L5装配](../05-modules/decisions.md#assembly)、[当前状态](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-current)。

<a id="errors-service-typed"></a>
## 类型化接入第四批：接收错误与验证（已落声明，2026-10-16）

完整错误/失败后状态见[接口](api.md#api-service-typed)，不加接收错误枚举。业务入口后唯一普通策略为尝试一次分阶段拒绝日志后结束；宏前置错误由框架处理，初始化缺上下文由装配调用者接收。日志和依赖panic不冒充普通拒绝。

唯一归属见[第5块逐符号表](call-chain.md#slice-service)；公开协调器组合另归第6块。

验证责任（分块结果见[service验收](../../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-service)）：两种真实宏装配及元信息一致、缺上下文/前置decode失败、所有protocol业务非法输入零记录器调用、关闭/无记录器/false真实分类和一次日志、成功一次记录器调用、params/弱binding/codec生命周期。handler实际poll顺序和锁外日志需真实框架证据，声明检查不代替；全部按后续AGENTS逐层TDD/验收，禁止覆盖率/CRAP。

来源2026-10-16：实际pi_service_macro0.3.2完整591行（相邻文件hash与已核解析包相同）、pi_service0.6.2 Service/Registry/Contexts/codec；宏先default_decode再typed、拥有式最后参数、send返回单位、私有生成类型及两个inventory条目。复用L5已选futures直依赖，版本/宏变化需复核。[图源](call-chain.md#chain-service-typed)、[表示](data.md#data-service-typed)、[四芯](units.md#units-service-typed)、[L5装配](../05-modules/decisions.md#assembly)。

<a id="errors-client-typed"></a>
## 类型化接入第三批：客户端错误与验证（已落声明，2026-10-16）

六类表示及源链见[表示](data.md#data-client-typed)/[接口](api.md#api-client-typed)；错误普通策略仍两种：显式直接返回、notifier尝试日志后返回。顺序binding→终端→目标→业务转换→宿主编码→唯一提交；所有前置零提交，最后恰一次，不重试/替代/缓存成功。编解码ServiceError已可能字符串化Fory原因，不能声称原Fory类型链。

唯一归属见[第4块逐符号表](call-chain.md#slice-client)。

| 验证目标（结果见[client验收](../../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-client)） | 必须证明 |
|---|---|
| 前置各类错误 | 零提交，六阶段及protocol内部优先级，状态变化按binding中毒合同。 |
| 显式/notifier | 共用路径，消息/来源/100ns保真，保存local与本次source分别生效。 |
| 编码/提交 | 真实codec错误为Encode，Submit保留终端错误，恰一次调用，Ok不等于远端完成。 |
| 错误trait/观测 | 实际解析/绑定/协议/编码/服务源链，fmt错误传播，notifier一次普通失败尝试一次日志并返回同一错误。 |
| 关闭/释放 | 有界真实竞争、准入先行可收尾，弱引用不永久保活terminal/binding，codec持有及最终释放不误推TLS回收。 |
| 真实装配 | codec表示往返归第2块；宏装配归第5块；公开协调器端到端组合归第6块。不是仅泛型check。 |

日志/格式化/依赖/析构panic不捕获为普通Err；不保证日志保存、外部全路径不阻塞或无重入。声明检查只验引用网，真实功能/TDD/安全/资源验收按AGENTS.md后续逐层开展；覆盖率/CRAP禁止。只读来源2026-10-16：pi-service/src/codec.rs完整400行、service.rs::codec_for_layer、terminal.rs::send及ServiceError，pi_logger::TraceNotifier；实际版本0.6.2/0.11.2，采用真实签名和错误粒度，不修改上游。原始PeerId/anyhow依据沿旧客户端[来源](api.md#api-client-delta)，版本/feature变化需复核。[图源](call-chain.md#chain-client-typed)、[五芯](units.md#units-client-typed)、[当前状态](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-client-design)。

<a id="errors-binding-typed"></a>
## 类型化接入第二批：binding错误与验证（已落声明，2026-10-16）

完整五类错误及Display/source、失败后状态、优先级、中毒和关闭合同只住[src/binding.rs](../../../src/binding.rs)对应类型/方法，HOW在[私有芯](../../../src/binding_inner.rs)。本册不另保留第二份规范。

唯一归属见[第3块逐符号表](call-chain.md#slice-binding)，消费者组合与共享设施保护明确归第6块。

| 真实边界 | 必需证据（结果见[binding验收](../../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-binding)） |
|---|---|
| 一次绑定 | Some/None都消耗机会、重复保持旧值、健康锁Closed优先，失败输入锁外释放。 |
| 发送/接收准入 | 未绑定/None可发送但接收拒绝；Weak失效、关闭、中毒真实分类。 |
| 关闭竞争 | 有界真实场景证明先准入可收尾、先关闭拒绝；不是压力测试。 |
| 记录器交接 | 锁外into_parts/一次apply_remote_selection，false语义不猜测/不重试，所有析构锁外。 |
| Owner及共享环境 | Owner主动关闭/局部弱生命周期归第3块；存活notifier/params消费者组合及共享宿主保护归第6块。 |
| 错误trait | 真实五类显示、格式化失败传播、source无底层源，不泄露PoisonError/守卫。 |

记录器Clone共享Arc、通知线程独立推进属于源码事实，不是本库行为验收。修改后check完整回执Finished、9.40s、0错误，本库39警告；修改前回执截断，仅登记证据缺口，不补造耗时/退出码。上述历史check只证明声明引用网；当前分块实现、行为及独立审查见本册顶部验收矩阵。关联：[接口](api.md#api-binding-typed)、[表示](data.md#data-binding-typed)、[十芯](units.md#units-binding-typed)、[图源](call-chain.md#chain-binding-typed)、[当前状态](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-current)。

<a id="errors-protocol-typed"></a>
## 类型化接入第一批：protocol错误与验证（已落声明，2026-10-16）

完整五类ProtocolError、私有DeadlineError、登记Result、Display/source及失败后状态只住[src/protocol.rs](../../../src/protocol.rs)，HOW只住[私有芯](../../../src/protocol_inner.rs)。不再在此复写错误枚举/全合同。ProtocolError::Codec及准备错误模型已移除；编码分类已落client的独立Encode(ServiceError)，宏前置解码沿已确认框架责任。

业务/期限/错误trait归[第1块](call-chain.md#slice-protocol)，已完成21/17场景与审查，见[验收](../../../docs/SLICE_PROTOCOL_ACCEPTANCE.md#slice-protocol-matrix)及[错误摘要](../../../docs/SLICE_PROTOCOL_API.md#sp-errors)；集中登记归[第2块](call-chain.md#slice-registration)，已完成实现及分块验收，见[登记结果](../../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-registration)。

| 验证入口 | 必须证明（当前结果见顶部验收矩阵） |
|---|---|
| 集中登记 | 与其它库共表、固定4300、重复/冲突真实错误，失败候选不发布。 |
| 发送转换 | Trace/非ASCII身份保真、100ns规范化、多项非法固定优先级。 |
| 接收校验 | 全零Trace、非法UTF-8、空来源、期限边界拒绝，不滞留完整输入。 |
| 合法交接 | 第1块证明完整校验及内容消费；binding零/一次发布由第3块负责。 |
| 错误trait | 分类显示、实际UTF-8源链、格式化失败传播。 |
| 两种真实装配 | codec表示往返归第2块；宏装配归第5块；公开协调器端到端组合归第6块。声明check不能替代。 |

期限边界已由本块Windows/Linux定向测试验证；NotRepresentable在目标共同域无可达平台失败输入，保留检查式映射静态审查与trait测试。依赖异常长度分配和TLS/缓冲高水位仍未验证；历史实际FORY_PANIC_ON_ERROR=false，不推广至不同依赖构建。普通Result前提及panic边界以源码合同为准。本块TDD曾观察目标TODO红线并同断言转绿；原装配及真实网络适用场景已在后续切片验证，完整安全、并发和资源结论仍不能由声明check或局部测试推出。后续遵守逐层TDD/相关验证/最终集中阶段，禁止覆盖率/CRAP。

来源：[接口](api.md#api-protocol-typed)、[表示](data.md#data-protocol-typed)、[八芯](units.md#units-protocol-typed)、[图源](call-chain.md#chain-protocol-typed)、[L1责任](../01-requirements/notes.md#typed-static-boundary)、[L5装配](../05-modules/decisions.md#assembly)。2026-10-16核验版本化源码：[Fory登记](https://docs.rs/crate/fory-core/0.14.1/source/src/fory.rs)、[TypeResolver](https://docs.rs/crate/fory-core/0.14.1/source/src/resolver/type_resolver.rs)、[错误开关](https://docs.rs/crate/fory-core/0.14.1/source/src/error.rs)。采用签名、冲突/重复/扩容与异常事实，不复制实现；版本/feature/宏变化需复核。恢复：[当前状态](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-current)。

<a id="errors-state"></a>
## 当前验证状态

37芯全部实现，各块测试/功能独立审查及有限能力验收见[实施矩阵](../../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-matrix)。原完整部署矩阵仍无完整证据；按用户最新指令仅WSL收尾，后续不运行集成/压力/基准或Windows构建测试，排除项不记通过。各批完整命令/结果与回执缺口见[控制台](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-verification)，不得用声明绿色替代后续行为证据。

<a id="errors-final"></a>
## 最终错误与资源责任（定稿/冻结，2026-10-16）

L7总门禁已确认；本表只收束责任与验证落点，完整WHAT/失败后状态仍只住各壳。

| 边界 | 谁处理与可观察结果 | 恢复/资源边界及后续验证 |
|---|---|---|
| 集中登记失败 | 宿主收到真实fory::Error，停止发布候选 | 不换号/回滚/缓存；普通Result受实际panic构建开关前提限制；[protocol壳](../../../src/protocol.rs)/[验证](#errors-protocol-typed)。 |
| 构造/记录器绑定/安装失败 | 宿主收到CoordinationError，按部分失败状态收尾 | 已有context拒绝保持原拥有者；已完成前项不回滚，无静态注销；[coordinator壳](../../../src/coordinator.rs)/[验证](#errors-coordinator)。 |
| 显式发送失败 | 调用者收到CoordinationSendError | 所有前置零send，进入提交恰一次；失败不重试、不据Submit推外部零效果；[client壳](../../../src/client.rs)/[验证](#errors-client-typed)。 |
| notifier普通失败 | 尝试一次原因日志，返回同一错误包装，上游累计失败 | 日志在临时终端释放后；不建新subscriber/结果关联表；[client验证](#errors-client-typed)。 |
| 宏前置解码失败 | 现有框架处理，本库不承诺日志 | 不进typed函数/记录器，不新增raw宏/codec/帧；[service壳](../../../src/service.rs)/[验证](#errors-service-typed)。 |
| typed入口后非法/关闭/无记录器/未发布 | service尝试一次validate或submit阶段拒绝日志 | 成功失败均消费输入；binding锁/临时资源释放后才日志；不猜false原因；[service验证](#errors-service-typed)/[binding验证](#errors-binding-typed)。 |
| 提交后异步错误 | 沿宿主真实通信粒度处理 | 不建逐Trace/操作关联表、不额外应答或补偿；[需求](../01-requirements/needs.md#needs)。 |
| panic/分配终止 | 不伪装普通Result，遵守依赖与宿主政策 | 已提交副作用不回滚；实现不额外捕获依赖panic；各块实际行为/独立审查见实施验收，不能把所有依赖异常都当普通Err。 |

普通错误策略：protocol返回业务/真实登记错误；binding返回语义错误或无返回终态清理；client显式返回/通知日志后返回两种；service业务入口后日志结束；coordinator返回并由宿主收尾。依赖panic不额外变为普通拒绝。

| 资源/持有方 | 当前冻结责任与验证边界 |
|---|---|
| coordinator | binding首字段；自动析构先进入binding主动关闭，再释放client/context。无额外Drop芯。 |
| binding | 唯一权威状态/记录器持有；锁内裁决、锁外调用和释放；close不等待已准入操作。 |
| client/notifier | 弱终端/弱binding，强预绑定codec；notifier可延长client/codec，不永久保活前两者。 |
| context/宏params | context弱binding；params强context/codec，关闭不注销/清它们。 |
| 宿主共享环境 | runtime、终端、线程、FlightRecorderGuard仍归宿主；本库不关闭它们。 |
| Fory/TLS/缓冲 | 实例/Arc释放与宿主线程TLS回收不同；无跨线程按实例驱逐保证，不声称硬内存上限。 |

以上弱持有/设计DAG不是整个宿主无环、无泄漏或无死锁的运行证明。必要真实验证全部留后续逐层TDD/相关测试；所有当前功能及相关验证完成后才进入集中最终回归/压力/基准，基准继续服从用户提交时机。覆盖率与CRAP始终禁止。

关联：[最终全链](call-chain.md#chain-final)、[有序切片](call-chain.md#implementation-slices)、[当前状态与实证](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-verification)。

<a id="errors-verification-ownership"></a>
## 唯一验证落点（差量确认，2026-10-16）

本表只定义验证责任；错误分类、失败后状态和资源合同仍以各壳为权威。后续组合复验不改变出口的首次验证归属。责任分配保持；各块现有证据和排除项见[实施验收](../../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-matrix)，不按旧TODO状态恢复。

| 验证范围 | 唯一负责块 | 精确归属入口 |
|---|---|---|
| protocol业务/期限/交接/错误trait | 第1块 | [逐符号表](call-chain.md#slice-protocol) |
| 固定登记、共表冲突、真实codec表示往返 | 第2块 | [登记符号](call-chain.md#slice-registration) |
| binding状态/Owner Drop/弱句柄/错误trait | 第3块 | [逐符号表](call-chain.md#slice-binding) |
| client发送/notifier/错误trait及本次资源 | 第4块 | [逐符号表](call-chain.md#slice-client) |
| 宏适配/元信息/初始化/实际poll/typed接收/params | 第5块 | [逐符号表](call-chain.md#slice-service) |
| 公开协调器组合/两安装方式/薄委托/根出口/共享设施保护 | 第6块 | [逐符号表](call-chain.md#slice-coordinator)、[无芯声明](call-chain.md#slice-declarations) |
| 过期不传播、重启不恢复、同机跨机/平台组合及集中最终门禁 | 第7块 | [最终需求收束](call-chain.md#slice-final) |

全部功能和相关验证完成后才进入第7块；集中最终验证、资源串行、用户授权及基准提交时机仍服从AGENTS.md。测试脚手架不得补全产品行为，局部分支验证不得冒充生产可达性。

<a id="errors-history"></a>
## 已替代历史入口

以下仅保留旧锚点兼容跳转，不是当前待办、源码符号存在证明或新实现要求。完整原文及收束前指纹见[历史档案](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#history-errors)。

<a id="errors-round-one"></a>
- 历史`errors-round-one`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#errors-round-one)；现行以上方当前条目为准。

<a id="errors-round-two"></a>
- 历史`errors-round-two`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#errors-round-two)；现行以上方当前条目为准。

<a id="errors-strategies"></a>
- 历史`errors-strategies`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#errors-strategies)；现行以上方当前条目为准。

<a id="errors-protocol-delta"></a>
- 历史`errors-protocol-delta`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#errors-protocol-delta)；现行以上方当前条目为准。

<a id="errors-binding-delta"></a>
- 历史`errors-binding-delta`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#errors-binding-delta)；现行以上方当前条目为准。

<a id="errors-client-delta"></a>
- 历史`errors-client-delta`：[原文](../../../docs/DESIGN_L7_SUPERSEDED_20261016.md#errors-client-delta)；现行以上方当前条目为准。

关联：[设计索引](../00-index.md)、[L7全链](call-chain.md#chain-final)、[唯一开发上下文](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-current)。
