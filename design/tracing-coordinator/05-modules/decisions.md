# tracing-coordinator · 决定与来源（L5）

**状态：定向返工后定稿/冻结，2026-10-16。** 用户选择“确认，归档并进入L7”，确认五模块、21条编译边、标准宏双内置codec适配、宿主codec预绑定持有、旧私有准备模型撤回及最小直接futures选型。具体新签名、源码/配置写入和声明检查另经L7逐批门禁。本版是结构设计，不是功能验收；历史见[日志](../index/changes.md)。

<a id="decisions"></a>
## 核心决定

| 决定 | 理由及边界 |
|---|---|
| 标准宏接收类型化SelectionMessage | 完整复用既有服务协议；宏解码成功后才执行本库业务校验和记录器交接。手写Service及新增raw宏能力方向撤回。 |
| 本库维护一份类型登记清单 | 宿主在相关表发布前调用；发送与接收使用相同编号、表示及配置。当前仅登记4300，4301–4310仅文档预留。 |
| 使用宿主稳定codec | 不在每通知构造Fory、重新登记、clone codec或切层；撤回第二套私有编解码资源及运行时固定检查。 |
| 五模块保留 | 装配、消息规则、生命周期、发送适配、静态框架接入各有独立变化原因；不新增只转发的一次性层。 |
| 保持绑定封装 | 接收只能消费已校验结果；原始状态、锁、记录器及弱引用升级能力不跨边界。 |
| 两个内置codec分别采用标准宏适配 | 宏生成具体codec类型；提供DefaultCodec、MultiLayerCodec相应静态接入，内部共用一处业务处理。 |
| 每进程只装配其中一种对应接入 | 两适配承载同一个逻辑服务、同一端口4091；不同codec/注册表不豁免进程端口唯一约束。 |
| 显式注册与inventory按宿主流程择一 | 宏产生inventory条目，同一服务不重复显式注册和自动加载；注册、初始化分别处理真实结果。 |
| client实现既有TraceNotifier | 宿主注入内部共享客户端而非整个coordinator，复用唯一发送路径，不造新trait或传播线程。 |
| close仅停止本库新准入 | 先准入可收尾，不等待在途、不撤回消息、不注销静态服务或关闭共享环境；不新增计数或队列。 |

[模块](modules.md#modules)、[依赖/调用/持有](deps.md#deps)、[影响面](change-points.md#change-points)是本表结构落点。被替代准备符号和源码门禁见[解冻清单](change-points.md#l7-revision)。

<a id="assembly"></a>
## 宿主装配及稳定性合同

1. **先登记，后发布，再预绑定。** 宿主把本库集中登记清单纳入相关类型表的冷路径准备；本库不在通知热路径修改宿主表。同表不重复登记，失败保留真实原因、不自动换号，不假定回滚。
2. **默认codec使用实际当前表。** DefaultCodec::Clone共享可替换槽；使用期由宿主保持相关配置稳定，本库不主动reset宿主槽。
3. **多层codec核对真实目标表。** switch_layer缺层可能回退default，codec_for_layer返回Ok不能独自证明正确登记；L7为实际支持的两种装配写清核对方式。
4. **预绑定后保持一致。** MultiLayerCodec已绑定current_fory快照不自动跟随后续layer替换；本库不承诺追踪热更新。发送端与宏初始化分别取得的绑定结果须协议一致。
5. **环境配对由宿主保证。** runtime与终端来自匹配通信装配，泛型相同不证明是同一环境；本库不强持runtime来代替该约束。
6. **本库逻辑服务串行安装。** 同类上下文不能并发替换；ServiceContexts重复插入会覆盖，已生成params仍持旧句柄。具体检查和失败后置在L7明确，不把只读上下文解释为容器不可替换。
7. **启动阶段不冒充事务。** 注册不init；绑定上下文失败与Service::init失败的状态不同。部分装配失败不假装已撤销前项，宿主据真实错误关闭本次协调器收尾；重复init不宣称幂等。
8. **关闭只作用本库准入。** 不等待在途、不注销静态服务、不关闭共享终端/runtime/Guard；重新使用按新的合法装配流程，不重开Closed绑定。

client强持预绑定Arc<C>，notifier可延长其持有。两个内置codec已读字段无runtime/终端反向强边；MultiLayerCodec强持整个层表，可能延长其它层Fory生命期。client消失只释放自身持有，底层仍可能被宿主或其它实例共享。binding.close不会清clientcodec、宏paramscodec或依赖TLS；首次线程/实例/读写方向的解析器深克隆、无按实例跨线程驱逐、缓冲高水位边界仍按已核验证据记录，不宣称常量总内存。

<a id="error-responsibility"></a>
## 错误责任

- 显式发送：校验、编码和提交错误返回调用者；前置失败零提交、进入提交至多一次，不重试。
- notifier：复用发送路径，普通同步失败尝试记录后返回；不额外建发送关联表。
- 接收业务入口之后：参数非法、关闭、记录器不可用和事件未发布由本库记录，非法参数不进入记录器。
- 宏前置解码失败：不调用业务函数、不进入记录器，沿既有框架处理，不承诺本库日志出口。
- 不以重试、吞错、自动换号或猜测记录器false的唯一原因掩盖失败；本地提交/发布不证明远端完成。

以上继承[用户已确认职责](../01-requirements/notes.md#typed-static-boundary)，不是新全局错误钩子或传输协议。

<a id="feasibility"></a>
## 可行性与证据等级

| 问题 | 已核验源码入口 | 结论及限制 |
|---|---|---|
| 宏支持拥有式typed输入 | pi_service_macro0.3.2；相邻src/lib.rs与解析版本指纹一致 | 最后输入不能引用；先default_decode再调用业务函数；send返回()或!。 |
| 两个具体codec标准适配 | 宏codec解析/生成与pi_service/src/lib.rs | Default/Multi均Clone且上游已collect；宏各自产inventory条目，宿主承担择一与全进程唯一，不自动全局互斥。 |
| 发送冷路径预绑定 | ServiceRuntime::codec_for_layer | clone→switch→Arc；不持runtime，目标层成功不等于登记正确，缺层可fallback。 |
| codec持有及替换 | pi_service/src/codec.rs完整400行 | Default共享替换槽，Multi共享层表但当前槽独立；无需新trait、codec或直接p2p依赖。 |
| 静态注册初始化 | ServiceRegistry::register/init、load_services_from_inventory | register不init；bind_params错误保留旧状态，init错误走Failed；无静态注销，库存顺序不保证、中途失败不回滚前项。 |
| 只读上下文 | ServiceContexts::insert_only_read/get_only_read | 返回/保存Arc；同类型插入覆盖，params保留既有快照，安装需串行约束。 |
| 单次发送 | ServiceTerminal::send及静态收敛链 | 同步本地受理，不保证远端；本库不新增队列/任务。 |
| 记录器及notifier | pi_logger TraceNotifier、通知线程、FlightRecorder::apply_remote_selection | 独立通知线程强持notifier；接收仅发布事件，后续消费不是当前栈直接回调client。 |
| 类型号 | tools集中登记、组合调用及详细档案 | 用户4300–4310/4300已确认，当前可见集合未见占用；未知部署/未来仍核实际表。 |
| TLS/资源 | 既有Fory0.14.1依赖核验与codec字段 | 释放实例不驱逐宿主线程上下文；仅源码证据，无内存或异常输入实验。 |

主上下文完整回读现有L5四册、七源码、Cargo、codec/terminal/宏及关键装配调用；复用只读自检代理，无写入/构建/测试/下级，未发现阻止提交提案的结构性阻断。自检指出的alloc来源缺口由主上下文版本化官方源码进一步核实；不是实际编译通过。String构造前前提、异常长度分配、EOF/异构schema、关闭竞争、平台及实际宿主组合仍按既有责任和验证缺口，不将源码核对当安全认证。

<a id="sources"></a>
## 来源与依赖选型

L5选型及后续L7完整源码门禁均已确认，service批已在Cargo中加入：

```toml
futures = { version = "0.3", default-features = false, features = ["alloc"] }
```

实际宏直接生成futures::future::BoxFuture，标准库等价类型无法改变宏字面引用；不能靠传递依赖。当前锁已有0.3.34；版本化源码核验futures→futures-util::future→futures-core::future::BoxFuture重导出链及alloc条件。此版本清单为Rust2018、MSRV1.71、MIT OR Apache-2.0，兼容本库指定nightly；不加入执行器或线程。pi_service特性合并可启用其它项，因此不声称全图仅alloc；实际service批前后声明检查退出码0，锁差量仅根依赖列表增加futures，无版本变动，见[当前证据](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-verification)；本段选择理由保持不变。

| 对象 | 选择及理由 | 版本/升级边界 |
|---|---|---|
| pi_service | 既有静态宏、codec、上下文和终端 | =0.6.2、path ../pi-service、default-features=false；接口或宏变化重核。 |
| pi_logger | 既有Trace/身份、notifier和记录器入口 | =0.11.2、path ../pi_logger、default-features=false；不改上游。 |
| fory / fory-core | 唯一消息及派生、集中登记 | 均=0.14.1；derive实际0.14.1，升级复核表示、登记、异常输入及TLS。 |
| anyhow | 现有notifier返回和真实解析错误 | 1.0、default-features=false、std；锁1.0.104。 |
| tracing | notifier失败/接收业务拒绝 | 0.1、default-features=false、std；锁0.1.44，不建subscriber。 |
| futures | 满足实际宏字面BoxFuture | 0.3、default-features=false、alloc；锁0.3.34；不因此次选型新增执行器。 |
| inventory | 已有宏收集机制 | 由pi_service重导出并已collect两种codec，不新增直接依赖。 |
| 标准库 | 所有权、弱引用、时间和最小同步 | 不增加通道、异步锁或运行时。 |
| 宿主通信 | 已启动终端和QUIC/IPC | pi_p2p_service0.3.9为装配参考，不增加直接依赖；不自造协议/帧/二次编码。 |
| Rust | 用户只支持1.99.0-nightly | nightly-2026-07-19 / eff8269f7，声明检查以WSL为准，不替代Windows运行证据。 |

外部来源核验2026-10-16：以下为版本化源码/清单，采用最小依赖引用和feature事实，不采用其执行器作为本库runtime，也不将本轮视为完整安全通告或全依赖审计：

- [futures0.3.34清单](https://docs.rs/crate/futures/0.3.34/source/Cargo.toml)
- [futures0.3.34模块重导出](https://docs.rs/crate/futures/0.3.34/source/src/lib.rs)
- [futures-util0.3.34 future模块](https://docs.rs/crate/futures-util/0.3.34/source/src/future/mod.rs)
- [futures-core0.3.34 BoxFuture](https://docs.rs/crate/futures-core/0.3.34/source/src/future.rs)

本地源码入口：[宏](../../../../pi-service-macro/src/lib.rs)、[codec](../../../../pi-service/src/codec.rs)、[runtime/registry](../../../../pi-service/src/service.rs)、[上下文](../../../../pi-service/src/context.rs)、[终端](../../../../pi-service/src/terminal.rs)、[上游collect](../../../../pi-service/src/lib.rs)、[通知线程](../../../../pi_logger/src/observability/trace_coordinator.rs)、[记录器](../../../../pi_logger/src/observability/flight_recorder.rs)。只读参考数据库集中登记，完整编号与指纹见[类型号档案](../../../docs/FORY_TYPE_ID_ALLOCATION_ARCHIVE_20261016.md#fory-evidence)；不照搬其业务RPC、附件、发现或运行期重建实例。

<a id="next"></a>
## 验证与逐轮推进

1. 已确认本轮四册/图源，立即归档、上下文及簿记同步；自检后机器推进L7。
2. 按protocol→binding→client逐层对齐受影响接口，再确认标准宏service、统一coordinator及全链错误/资源收束。
3. 每批另收完整源码/配置/声明检查门禁，获准后才修改壳芯；WHAT住壳中文合同，HOW住芯伪码，md留WHY/精确定位/图源/验证缺口。
4. L7全册五册＋声明骨架档位不重问；整体确认后新建无会话历史的独立L9。
5. 实现、真实TDD、相关测试、审查和验收在lite之后按AGENTS.md逐层执行；全量/压力/基准后置，覆盖率/CRAP禁止。没有执行的验证不标通过。

本次结构确认不授权新运行逻辑；后续L7已完成11源码/37TODO芯，仍不是可用能力，最终设计/声明状态见[当前索引](../00-index.md)。依赖已验收摘要不足仍为证据缺口；本轮完成设计流程的阶段约定不替代后续实现准入。

来源：[需求](../01-requirements/needs.md#needs)、[类型化责任](../01-requirements/notes.md#typed-static-boundary)、[素材](../../../Trace跨进程协调器外部库接入提示词.md)、[日志](../index/changes.md)。反向：[模块](modules.md#modules)、[依赖](deps.md#deps)、[影响面](change-points.md#change-points)、[索引](../00-index.md)、[当前状态](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-typed-static-revision)。
