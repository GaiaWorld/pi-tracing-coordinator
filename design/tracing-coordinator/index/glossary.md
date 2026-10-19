# 登记分册：词汇表

**状态：2026-10-16 L9后差量确认，定稿/冻结。** 唯一上报通知及旧登记术语撤回保持；补齐准入、一次绑定、预绑定codec、本地事件发布的既有语义，词汇行经dev-design-lite具名函数维护。来源：[澄清结论](../01-requirements/notes.md#clarifications)。

## 词汇

| 术语 | 定义 | 出处 |
|---|---|---|
| Trace | 同一业务处理链的既有追踪标识。 | L1 接入素材；[业务内容](../01-requirements/notes.md#message-semantics) |
| 参与进程实例 | 当前运行中的参与进程；重启清空本地旧内存，不要求一定更换通信身份。 | L1 重启确认；[需求](../01-requirements/needs.md#needs) |
| 相邻关系 | 同一 Trace 涉及的进程关系；跟踪、登记和缓存全部由宿主负责，本库不管理。 | 2026-10-15 用户职责纠正；[澄清](../01-requirements/notes.md#clarifications) |
| 登记通知（已撤回） | 已撤回的旧方案：本库不发送或接收登记通知，也不提供本地登记入口；历史见变更日志。 | 2026-10-15 用户明确本库只有上报通知；[澄清](../01-requirements/notes.md#clarifications) |
| 上报决定 | 指定 Trace 与绝对期限的处理决定；连同本跳来源组成本库唯一的协调通知，不表示已经导出。 | L1 接入素材；[业务内容](../01-requirements/notes.md#message-semantics) |
| 实际来源 | 宿主提供的本跳直接发送进程，供记录器排除回传；本库不承担认证。 | L1 接入素材；[业务内容](../01-requirements/notes.md#message-semantics) |
| 本地发送提交 | 本地接受一次发送尝试，不保证送达或远端完成。 | L1 素材与 send 约束；[需求](../01-requirements/needs.md#needs) |
| 绝对期限 | 统一 100 纳秒精度的过期时刻；发送前仅向下舍弃更细部分，最多提前 99 纳秒，之后传输与交接保持该值；记录器可缩短不续期，部署方负责时钟。 | L1 期限确认；[需求](../01-requirements/needs.md#needs) |
| 宿主 | 使用本库的进程；负责关联跟踪/登记/缓存、目标选择和发送决定，提供身份及通信环境，管理共享资源，并初始化注入本库通知实现。 | L1 用户明确补充；[范围](../01-requirements/problem.md#scope) |
| 准入 | binding在同一状态锁下裁决的一次操作许可。发送返回单位成功，接收内部取得临时记录器；不返回持续令牌、不跟踪在途。先准入可在close返回后收尾。 | [binding合同](../../../src/binding.rs)；消费者client/service/coordinator，见[调用源表](../05-modules/deps.md#calls) |
| 一次绑定 | 每个binding仅从未绑定进入已绑定一次，首次Some/None都消耗机会；重复不能替换，关闭后不能重开。 | [binding合同](../../../src/binding.rs)；消费者[coordinator](../../../src/coordinator.rs) |
| 预绑定codec | 冷路径从宿主runtime选层并保存供后续使用的codec。Default共享可替换槽，Multi保存已选当前表快照；两者不能笼统称不可变快照，宿主保持使用期配置稳定。 | [coordinator](../../../src/coordinator.rs)/[client](../../../src/client.rs)/[service](../../../src/service.rs)；[L5装配](../05-modules/decisions.md#assembly) |
| 本地事件发布 | apply_remote_selection的true表示本地事件发布，不证明选择、传播或导出完成；false只按未发布处理，不推断唯一原因。 | [binding](../../../src/binding.rs)/[service](../../../src/service.rs)；pi_logger接入，见[需求](../01-requirements/needs.md#needs) |

内容来源：[L1 业务内容](../01-requirements/notes.md#message-semantics)、[问题范围](../01-requirements/problem.md#scope)、[需求](../01-requirements/needs.md#needs)。恢复：[设计索引](../00-index.md)、[项目状态](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-current)。
