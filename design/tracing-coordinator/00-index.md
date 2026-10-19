# tracing-coordinator 设计索引（design-lite）

恢复入口：[状态册](index/status.md)。当前阶段表示下一个要执行阶段；所有内容均按用户完整门禁确认后落盘。

**当前：design-lite设计已完成，实施37芯及七块工作流已完成。** 2026-10-16用户接受L9及R10复核的设计收口记录保留；2026-10-19已归并分块验收、独立审查回执及[稳定出口摘要](../../docs/IMPLEMENTATION_API.md#implementation-api)。原完整部署/压力/性能矩阵没有完整通过证据；后续按用户最新指令仅WSL构建测试，排除集成、压力、基准及Windows原生构建测试。见[实施验收](../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-scope)和[唯一当前上下文](../../docs/DEVELOPMENT_CONTEXT.md#ctx-current)；技术合同、签名、类型及可见性不变。

## 登记与来源

- [状态](index/status.md)
- [变更日志](index/changes.md)
- [词汇](index/glossary.md)：唯一上报通知；旧登记业务已撤回。
- [L0事实盘点](00-context.md)：历史地形，不把当时空库/旧版本当现状。

## 冻结设计

- L1：唯一上报通知，关联/目标/发送时机归宿主；失败观测责任已于2026-10-16定向校准。
  - [问题与范围](01-requirements/problem.md#problem)
  - [需求与验收标准](01-requirements/needs.md#needs)
  - [唯一上报流程与源表](01-requirements/flow.md#selection-flow)
  - [业务内容](01-requirements/notes.md#message-semantics)、[类型化接入边界](01-requirements/notes.md#typed-static-boundary)
- L5：2026-10-16完整返工已重新冻结，五模块/21条编译边，标准宏双内置codec、宿主预绑定资源及最小futures直依赖。
  - [模块与组织图](05-modules/modules.md#modules)
  - [类型依赖](05-modules/deps.md#deps)、[预期调用](05-modules/deps.md#calls)、[强弱持有](05-modules/deps.md#ownership)
  - [变更点](05-modules/change-points.md#change-points)、[旧L7替代面](05-modules/change-points.md#l7-revision)
  - [决定与来源](05-modules/decisions.md#decisions)、[宿主装配](05-modules/decisions.md#assembly)
- L7：全册五册＋声明骨架及L9后最小差量均完成确认归档；用户本轮总门禁已选择“确认，归档后进入L9（推荐）”。WHAT住壳、HOW住芯，md留理由/指针/图源/验证。
  - [接口索引](07-design/api.md#api-index)
    - [protocol](07-design/api.md#api-protocol-typed)、[binding](07-design/api.md#api-binding-typed)、[client](07-design/api.md#api-client-typed)、[service](07-design/api.md#api-service-typed)、[coordinator](07-design/api.md#api-coordinator)
  - [数据与不变量定位](07-design/data.md#data-index)
  - [37芯定位](07-design/units.md#units-index)
  - [最终全链/源表/读图说明](07-design/call-chain.md#chain-final)
    - [集中登记](07-design/call-chain.md#chain-protocol-registration)、[业务转换](07-design/call-chain.md#chain-protocol-typed)、[生命周期/发送准入](07-design/call-chain.md#chain-binding-lifecycle)、[接收关闭交错](07-design/call-chain.md#chain-binding-typed)
    - [协调器构造](07-design/call-chain.md#chain-coordinator)、[安装编排](07-design/call-chain.md#chain-coordinator-install)、[宏初始化](07-design/call-chain.md#chain-service-initialization)、[发送](07-design/call-chain.md#chain-client-typed)、[接收](07-design/call-chain.md#chain-service-typed)
    - [完备有序实现切片](07-design/call-chain.md#implementation-slices)、[宿主接入次序](07-design/call-chain.md#chain-next)
  - [错误与验证索引](07-design/errors.md#errors-index)、[最终错误/资源责任](07-design/errors.md#errors-final)
- 实施：[七块验收矩阵](../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-matrix)、[全库API摘要与现状图](../../docs/IMPLEMENTATION_API.md#implementation-api)；第1块[验收矩阵](../../docs/SLICE_PROTOCOL_ACCEPTANCE.md#slice-protocol-matrix)、[7出口摘要与三图](../../docs/SLICE_PROTOCOL_API.md#slice-protocol-api)；只同步实施状态，不改变冻结技术合同。
- L9：全新无对话历史代理已直接读冻结分册及壳芯，15项通过，R10条件经同一代理定向复核解除；用户已接受报告及复核并收口设计。实际设计状态为“已完成”，设计期结束时实现/测试/验收尚未开始（本块最新状态见实施条目）；[收口记录](09-review.md#l9-design-closure)。

## 源码、配置及证据边界

- 11生产源码：[根](../../src/lib.rs)及五模块壳芯，共37芯；37芯均已实现。测试包含协议/登记/绑定/发送/服务/协调器及最终组合/网络，既有文件保留；本轮只运行纯单元范围。所有权及Drop已实现并经独立审查。
- [Cargo.toml](../../Cargo.toml)当前7项生产直接依赖及3项经授权的固定开发依赖；历史Fory/futures与真实网络宿主的锁扩展见当前上下文，本轮不再改依赖。
- 最新WSL限定验证：nightly-2026-07-19，单包locked/offline；lib check、lib build、lib Clippy及21+3纯单元均退出0。本库无新增诊断，依赖既有警告保留；[实际命令与回执](../../docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-wsl-checks)。此前声明期检查只作历史，不充当前行为证据。
- 类型号：[详细事实档案](../../docs/FORY_TYPE_ID_ALLOCATION_ARCHIVE_20261016.md#fory-archive-meta)、[4300–4310/SelectionMessage=4300](../../docs/FORY_TYPE_ID_ALLOCATION_ARCHIVE_20261016.md#fory-allocation-contract)。4301–4310只文档预留，当前集中登记已实现并通过真实表/两codec分块验证。
- 服务端口：[4091档案及保证边界](../../docs/SERVICE_PORT_ALLOCATION_ARCHIVE_20261015.md#port-contract)。宿主仍核对最终实际组合，不能把分配/同表注册成功当进程全局唯一证据。

## 历史与恢复

- [L7被替代正文/收束前指纹/旧控制台](../../docs/DESIGN_L7_SUPERSEDED_20261016.md#l7-history-meta)：完整保留旧准备模型、旧codec接缝和局部切片；五册旧锚点继续跳转，不作为现行合同。
- [20261014会话档案](../../docs/DESIGN_SESSION_ARCHIVE_20261014.md#archive-meta)、[20261013早期档案](../../docs/DESIGN_SESSION_ARCHIVE_20261013.md#archive-meta)：只作历史，不恢复旧业务/编号/暂停或过期下一问。
- [唯一当前开发上下文](../../docs/DEVELOPMENT_CONTEXT.md#ctx-current)。设计冻结、声明编译、L9轻审、实现、测试和验收分别记录；全库已验收稳定出口及分块证据见实施摘要；历史过程和当前执行范围分开记录，不恢复旧TODO状态。
