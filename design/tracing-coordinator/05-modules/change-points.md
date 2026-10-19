# tracing-coordinator · 变更点（L5）

**状态：定向返工后定稿/冻结，2026-10-16。** 用户确认本表、五模块/21边及调用/持有关系，并进入L7。旧私有准备和手写Service方向被替代；具体新签名、源码/配置写入和声明编译仍逐批门禁。历史见[日志](../index/changes.md)。

<a id="change-points"></a>
## 变更点

| 变更点 | 变更方向 | 隔离位置 | 直接影响面与复验入口 |
|---|---|---|---|
| 宿主统一装配 | 接受既有环境，冷路径取得预绑定codec | coordinator | 构造失败、环境配对、notifier注入、静态接入。 |
| 唯一消息及编号 | 4300；集中登记对宿主可达，消息对库内消费者可命名 | protocol | 两端登记、标准宏编译、字段保真及编号冲突。 |
| 业务转换与校验 | 与字节编解码分离，保留TraceId→source→期限校验顺序 | protocol | 显式发送、notifier、接收业务入口及错误优先级。 |
| codec持有与编码 | 客户端复用预绑定宿主codec，编码错误在发送层分类 | client | 构造参数、错误链、使用期稳定、关闭后的资源持有。 |
| 准入、绑定与关闭 | 撤回协议资源字段；保留一次绑定、弱接入、锁外交接 | binding | 构造与发送准入结果；接收、关闭竞争、中毒清理。 |
| 静态框架接入 | 标准宏自动解码，业务函数接收拥有式消息 | service | 两种具体codec适配、上下文绑定、注册与初始化。 |
| 错误观测 | 发送错误返回；notifier同步失败和接收业务拒绝记录 | client/service | 不把宏前置解码错误写成本库可观察能力。 |

<a id="l7-revision"></a>
## 本次明确解冻/撤回的旧L7面

| 旧声明或合同 | 本次方向 |
|---|---|
| PreparedProtocol、ProtocolPrepareError及其错误表示和trait芯 | 撤回私有协议资源及准备错误模型。 |
| check_protocol_candidate、固定准备样本检查及其封装 | 撤回运行时准备检查；必要编解码验证放后续真实测试。 |
| PREPARED_PROTOCOL、initialize_protocol、prepare_protocol及根重导出 | 撤回进程级私有准备缓存。 |
| BindingInner::protocol、绑定构造的协议参数 | 移除协议资源归属。 |
| admit_send返回静态协议引用 | 改为只表达准入结果，具体签名在L7确认。 |
| SelectionMessage私有可见性、SELECTION_TYPE_ID=1000、库内登记出口 | 对齐必要库内可见性、宿主登记出口及已确认4300；字段保持封装。 |
| PreparedProtocol::encode_selection/decode | 拆为业务值/消息转换；编码由client使用宿主codec，解码由宏负责。 |
| ProtocolError::Codec(ServiceError) | 编码错误归发送层；protocol只表达业务转换错误。 |
| 客户端_codec类型标记及“不持codec”合同 | 改为预绑定codec实际持有，重定构造及释放合同。 |
| L5四册、L7五册中受影响索引、图、伪码与源码注释 | 随逐批门禁同步，保留旧快照和替代原因。 |

期限共同域、100ns向下规范化、身份字节表示、已校验交接封装、一次记录器绑定及关闭不变量继续作为输入。本表保存L5时明确撤回/替代的范围。后续L7已逐批确认并替换全部受影响声明，当前11源码/37芯TODO，旧准备符号已删除；现行冻结出口见[接口索引](../07-design/api.md#api-index)，不再把本表的旧签名当当前能力。

变化先沿[21边和调用/持有源表](deps.md#deps)确定消费者，再按依赖顺序重对齐。编号已确认[4300–4310/4300](../../../docs/FORY_TYPE_ID_ALLOCATION_ARCHIVE_20261016.md#fory-allocation-contract)；其余号只作预留，不创建占位类型。静态元信息的具体id/layer/version、上下文安装检查及失败收尾在L7逐项确定；端口4091及进程唯一性约束保持。

归属：[模块](modules.md#modules)；理由与可行性：[决定](decisions.md#decisions)、[装配合同](decisions.md#assembly)；验证来源：[需求](../01-requirements/needs.md#needs)。恢复：[索引](../00-index.md)、[项目状态](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-current)。
