// 设计：tracing-coordinator
// slice-client五芯已实现；下列步骤保留冻结设计预期。
// 行为入口：tests/client/client_test.rs；独立审查已通过，验收见docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-client。
// 设计：design/tracing-coordinator/07-design/call-chain.md#slice-client。

use super::{CoordinationClient, CoordinationSendError, CoordinationSendErrorKind};
use crate::{binding::CoordinationHandle, protocol::selection_to_message};
use pi_logger::{TraceNodeId, TraceSelection};
use pi_service::{ServiceAddress, ServiceCodec, ServiceHost, ServiceTerminal};
use std::{error::Error, fmt, ops::Deref, sync::Arc};

// new
//   从借入终端建立弱引用
//   移入已预绑定codec、binding和local
//   返回客户端；不验证、登记、切层或提交
pub(super) fn new<T, C>(
    terminal: &Arc<T>,
    codec: Arc<C>,
    binding: CoordinationHandle,
    local: TraceNodeId,
) -> CoordinationClient<T, C>
where
    C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>,
    T: Deref<Target = ServiceTerminal<C>> + Send + Sync + 'static,
{
    // 只组合既有资源：不升级终端或触碰绑定锁，空local留到实际发送时校验。
    // 验证：tests/client/client_test.rs的new_is_lazy_and_respects_resource_ownership场景。
    CoordinationClient {
        terminal: Arc::downgrade(terminal),
        codec,
        binding,
        local,
    }
}

// send_selection
//   请求发送准入；失败返回Binding
//   升级终端；失败返回TerminalUnavailable
//   解析原始目标；失败保存实际解析原因
//   转换业务消息；失败保存Protocol
//   借用已保存codec编码；失败保存Encode
//   成功时恰一次提交地址/字节/from=None
//   保存本地受理结果或Submit错误
//   各退出路径锁外释放临时终端和剩余局部值
//   返回结果；无重试、缓存或补偿
pub(super) fn send_selection<T, C>(
    client: &CoordinationClient<T, C>,
    remote: &TraceNodeId,
    selection: TraceSelection,
    source: &TraceNodeId,
) -> Result<(), CoordinationSendError>
where
    C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>,
    T: Deref<Target = ServiceTerminal<C>> + Send + Sync + 'static,
{
    // 六阶段先后是首错合同；准入返回前binding已解锁并释放自身临时持有。
    // 此后不再检查Closed，已获准调用可以在close返回后完成提交。
    client
        .binding
        .admit_send()
        .map_err(|error| CoordinationSendError {
            kind: CoordinationSendErrorKind::Binding(error),
        })?;
    let terminal = client.terminal.upgrade().ok_or(CoordinationSendError {
        kind: CoordinationSendErrorKind::TerminalUnavailable,
    })?;
    // 临时Arc从解析前保活至提交结束；原始文本不trim，原始解析错误交anyhow接管。
    let peer = remote
        .as_str()
        .parse()
        .map_err(|error| CoordinationSendError {
            kind: CoordinationSendErrorKind::InvalidTarget(anyhow::Error::new(error)),
        })?;
    let host = ServiceHost::Remote(peer);
    let message =
        selection_to_message(selection, source).map_err(|error| CoordinationSendError {
            kind: CoordinationSendErrorKind::Protocol(error),
        })?;
    // 只借保存的预绑定codec，不克隆、登记或切层；依赖已字符串化的原因不另行重建。
    let bytes = client
        .codec
        .default_encode(&message)
        .map_err(|error| CoordinationSendError {
            kind: CoordinationSendErrorKind::Encode(error),
        })?;
    // 唯一真实终端调用：拥有式字节、固定逻辑端口、无from；不预探测、自拒绝或重试。
    // Submit保留终端实际返回值，Err也不能推定传输侧未接管字节或无外部效果。
    let result = terminal
        .send(
            ServiceAddress::new(host, crate::COORDINATION_SERVICE_PORT.into()),
            bytes,
            None,
        )
        .map_err(|error| CoordinationSendError {
            kind: CoordinationSendErrorKind::Submit(error),
        });
    // 所有正常/错误返回均在锁外释放局部值；上方?及依赖panic展开同样由栈回收。
    // 最后终端引用的Drop可阻塞或panic，不转换为普通Err，也不回滚已提交效果。
    drop(message);
    drop(terminal);
    result
}

// notify
//   用本次remote/selection和保存local调用一次send_selection
//   成功直接返回
//   普通失败尝试一次固定字段警告
//   用anyhow接管同一错误并返回
pub(super) fn notify<T, C>(
    client: &CoordinationClient<T, C>,
    remote: &TraceNodeId,
    selection: TraceSelection,
) -> anyhow::Result<()>
where
    C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>,
    T: Deref<Target = ServiceTerminal<C>> + Send + Sync + 'static,
{
    // 保存local只在共用发送链的protocol阶段校验，不改变六阶段首错顺序。
    match client.send_selection(remote, selection, &client.local) {
        Ok(()) => Ok(()),
        Err(error) => {
            // 发送已返回，临时终端/消息及绑定锁均已释放；只尝试一次宿主警告。
            // subscriber/格式化panic不捕获，正常返回时anyhow按值接管同一个错误。
            tracing::warn!(
                target: "pi_tracing_coordinator::client",
                remote = %remote.as_str(),
                service_port = crate::COORDINATION_SERVICE_PORT,
                error = %error,
            );
            Err(anyhow::Error::new(error))
        }
    }
}

// fmt_send_error
//   按六类原因写出真实阶段
//   有原因则显示实际原因；终端失效只描述已知事实
//   任意写出失败立即传播fmt错误
//   不修改业务或错误，不记录日志
pub(super) fn fmt_send_error(
    error: &CoordinationSendError,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    // 阶段说明独立于原因文本；各分支直接返回写出结果，首次fmt失败即传播。
    // 验证：tests/client/client_test.rs的error_display两场景，不新增日志或业务副作用。
    match &error.kind {
        CoordinationSendErrorKind::Binding(source) => write!(f, "发送绑定准入失败：{source}"),
        CoordinationSendErrorKind::TerminalUnavailable => {
            f.write_str("发送终端不可用，弱引用无法升级")
        }
        CoordinationSendErrorKind::InvalidTarget(source) => write!(f, "发送目标解析失败：{source}"),
        CoordinationSendErrorKind::Protocol(source) => write!(f, "发送协议业务转换失败：{source}"),
        CoordinationSendErrorKind::Encode(source) => write!(f, "发送编码失败：{source}"),
        CoordinationSendErrorKind::Submit(source) => write!(f, "发送提交失败：{source}"),
    }
}

// send_error_source
//   按已保存分类借用直接原因
//   InvalidTarget借用anyhow中的解析原因
//   TerminalUnavailable无原因
//   不跳层、不分配、不格式化、不改变状态
pub(super) fn send_error_source(error: &CoordinationSendError) -> Option<&(dyn Error + 'static)> {
    // 借用保存的直接原因，不调用其source跳层；anyhow.as_ref保留原始解析错误。
    // Encode/Submit只承诺真实ServiceError，不能恢复依赖已经字符串化的Fory原因。
    match &error.kind {
        CoordinationSendErrorKind::Binding(source) => Some(source),
        CoordinationSendErrorKind::TerminalUnavailable => None,
        CoordinationSendErrorKind::InvalidTarget(source) => Some(source.as_ref()),
        CoordinationSendErrorKind::Protocol(source) => Some(source),
        CoordinationSendErrorKind::Encode(source) | CoordinationSendErrorKind::Submit(source) => {
            Some(source)
        }
    }
}
