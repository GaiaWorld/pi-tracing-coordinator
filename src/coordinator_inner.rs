// 设计：tracing-coordinator
// slice-coordinator十芯已实现；下列步骤保留冻结设计预期。
// 行为入口：tests/coordinator_test.rs；独立审查已通过，验收见docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-coordinator。
// 取证：docs/SLICE_COORDINATOR_TEST_EVIDENCE.md#slice-coordinator-test-pending。

use super::{CoordinationError, CoordinationErrorKind, TracingCoordinator};
use crate::{
    binding::CoordinationBinding,
    client::{CoordinationClient, CoordinationSendError},
    service::{self, CoordinationServiceContext},
};
use pi_logger::{FlightRecorder, TraceNodeId, TraceNotifier, TraceSelection};
use pi_service::{DefaultCodec, MultiLayerCodec, ServiceCodec, ServiceRuntime, ServiceTerminal};
use std::{error::Error, fmt, ops::Deref, sync::Arc};

// 默认装配构造：
// 前提：宿主已登记、发布并核对实际默认类型表；terminal与runtime匹配，
//       使用期配置稳定。这里只组合对象，不代替宿主证明这些前提。
// 1. 从runtime按本库服务layer取得冷路径预绑定codec。
//    普通失败：将实际ServiceError保存为CoordinationError的Service原因，
//              立即返回，不继续创建binding/client/context或发布对象。
// 2. 取得DefaultCodec对应的default_service静态借用，不注册或初始化。
// 3. 创建独立CoordinationBinding，分别取得供client和context使用的两个弱接入。
// 4. 借terminal建立客户端弱终端引用，把预绑定codec、client弱接入及原local
//    移入CoordinationClient；用Arc共享该客户端。不在此检查空local或通信就绪。
// 5. 把另一弱接入移入CoordinationServiceContext，用Arc共享该上下文。
// 6. 按父模块冻结字段组合：binding所有者、共享client、共享context、对应static。
//    保持binding为首字段，返回调用方拥有的独立协调器。
// 后置：未发布context、绑定记录器、注册或初始化服务；未创建任务。
//       普通失败仅释放局部持有；没有已返回对象时不虚构调用其close。
//       依赖panic或分配终止不捕获为普通错误。
pub(super) fn new_default<T>(
    terminal: &Arc<T>,
    runtime: &ServiceRuntime<DefaultCodec>,
    local: TraceNodeId,
) -> Result<TracingCoordinator<T, DefaultCodec>, CoordinationError>
where
    T: Deref<Target = ServiceTerminal<DefaultCodec>> + Send + Sync + 'static,
{
    // 先预绑定再创建所有者；普通错误直接移入原因，不发布对象或虚构失败后的close。
    let codec = runtime
        .codec_for_layer(service::COORDINATION_SERVICE_LAYER)
        .map_err(|error| CoordinationError {
            kind: CoordinationErrorKind::Service(error),
        })?;
    let service = service::default_service();
    let binding = CoordinationBinding::new();
    let client_binding = binding.handle();
    let context_binding = binding.handle();
    // client仅弱持终端，两个接入均弱持同一binding；三处组合Arc不包含依赖预绑定成本。
    let client = Arc::new(CoordinationClient::new(
        terminal,
        codec,
        client_binding,
        local,
    ));
    let context = Arc::new(CoordinationServiceContext::new(context_binding));
    // 字段布局沿冻结壳：自动析构先由binding关闭，残留notifier/params不能保活owner。
    Ok(TracingCoordinator {
        binding,
        client,
        context,
        service,
    })
}

// 多层装配构造：
// 前提：宿主已完成明确目标层的登记、发布和真实表核对，环境配对且配置稳定。
// 1. 先通过runtime的模板codec检查本库服务layer是否真实存在。
//    缺失：立即返回CodecLayerMissing，不调用codec_for_layer，不采用默认层回退。
// 2. 目标层存在才取得该layer的冷路径预绑定codec。
//    普通失败：保存实际ServiceError为Service原因并立即返回，不继续组合对象。
// 3. 取得MultiLayerCodec对应的multi_layer_service静态借用。
// 4. 创建独立CoordinationBinding，分别取得client和context的两个弱接入。
// 5. 借terminal建立弱终端引用，把预绑定codec、client弱接入及原local
//    移入CoordinationClient，再用Arc共享；不验证空local或重新登记。
// 6. 把另一弱接入移入CoordinationServiceContext，再用Arc共享。
// 7. 按父模块冻结字段顺序组合并返回，binding仍为首字段。
// 后置：不发布context、绑定记录器或注册/init服务；已选快照不承诺跟随层替换。
//       普通失败不发布对象，局部持有正常释放；无已返回对象时不调用其close。
//       不捕获依赖panic或分配终止冒充普通错误。
pub(super) fn new_multi_layer<T>(
    terminal: &Arc<T>,
    runtime: &ServiceRuntime<MultiLayerCodec>,
    local: TraceNodeId,
) -> Result<TracingCoordinator<T, MultiLayerCodec>, CoordinationError>
where
    T: Deref<Target = ServiceTerminal<MultiLayerCodec>> + Send + Sync + 'static,
{
    // switch_layer可能回退default，必须先查明确目标；缺层时不预绑定或创建owner。
    if !runtime
        .codec()
        .has_layer(service::COORDINATION_SERVICE_LAYER)
    {
        return Err(CoordinationError {
            kind: CoordinationErrorKind::CodecLayerMissing,
        });
    }
    let codec = runtime
        .codec_for_layer(service::COORDINATION_SERVICE_LAYER)
        .map_err(|error| CoordinationError {
            kind: CoordinationErrorKind::Service(error),
        })?;
    let service = service::multi_layer_service();
    let binding = CoordinationBinding::new();
    let client_binding = binding.handle();
    let context_binding = binding.handle();
    // 使用期稳定配置由宿主保证；client和宏初始化各自取得的codec须协议一致。
    let client = Arc::new(CoordinationClient::new(
        terminal,
        codec,
        client_binding,
        local,
    ));
    let context = Arc::new(CoordinationServiceContext::new(context_binding));
    Ok(TracingCoordinator {
        binding,
        client,
        context,
        service,
    })
}

// notifier
//   复制共享client的Arc并作为既有TraceNotifier返回
pub(super) fn notifier<T, C>(coordinator: &TracingCoordinator<T, C>) -> Arc<dyn TraceNotifier>
where
    C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>,
    T: Deref<Target = ServiceTerminal<C>> + Send + Sync + 'static,
{
    // 只复制已有client的引用计数并转换为既有trait，不新建包装或保存整个协调器。
    coordinator.client.clone()
}

// bind_recorder
//   委托binding一次，失败保留实际BindingError
pub(super) fn bind_recorder<T, C>(
    coordinator: &TracingCoordinator<T, C>,
    recorder: Option<FlightRecorder>,
) -> Result<(), CoordinationError>
where
    C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>,
    T: Deref<Target = ServiceTerminal<C>> + Send + Sync + 'static,
{
    // Some/None及失败输入释放均交给唯一状态归属方；只按值保存它返回的真实原因。
    coordinator
        .binding
        .bind_recorder(recorder)
        .map_err(|error| CoordinationError {
            kind: CoordinationErrorKind::Binding(error),
        })
}

// 接入本次接收上下文：
// 前提：传入构造时同一runtime；宿主串行化同逻辑服务的安装、容器变更和关闭。
// 1. 从本次binding建立弱接入，执行一次开放准入。
//    失败：保存真实BindingError并立即返回；不得查询后写入、发布或替换context。
//    成功：确认准入调用已返回、其锁和临时强引用已释放，再进入宿主容器。
// 2. 查询CoordinationServiceContext的现有只读条目。
//    已存在：立即返回ContextAlreadyInstalled；同Arc也不按幂等成功处理，
//            保持原context、原服务及原拥有者不变，不执行插入。
// 3. 仅不存在时，复制本次已保存context的Arc并插入只读上下文容器。
// 4. 返回成功；不继续register或init。
// 后置：先查后插仅在宿主串行前提下使用，不新增并发安装锁或原子替换承诺。
//       已发布条目不自动删除；普通准入/预占错误不产生本次容器写入。
pub(super) fn bind_service_context<T, C>(
    coordinator: &TracingCoordinator<T, C>,
    runtime: &ServiceRuntime<C>,
) -> Result<(), CoordinationError>
where
    C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>,
    T: Deref<Target = ServiceTerminal<C>> + Send + Sync + 'static,
{
    // 准入返回时下层已释放状态锁及临时强引用；拒绝路径不进入宿主容器。
    coordinator
        .binding
        .handle()
        .admit_send()
        .map_err(|error| CoordinationError {
            kind: CoordinationErrorKind::Binding(error),
        })?;
    let contexts = runtime.contexts();
    if contexts
        .get_only_read::<CoordinationServiceContext>()
        .is_some()
    {
        // 同Arc也拒绝；不调用会覆盖旧值的insert，原context和原owner保持不变。
        return Err(CoordinationError {
            kind: CoordinationErrorKind::ContextAlreadyInstalled,
        });
    }
    // 先查后插依赖宿主串行安装/容器修改/关闭，不扩大为并发原子替换保证。
    contexts.insert_only_read(coordinator.context.clone());
    Ok(())
}

// 显式安装：
// 前提：使用构造时同一runtime，与inventory方式择一；安装/容器变更/关闭串行。
// 1. 调用本次对象的bind_service_context。
//    失败：原样返回该CoordinationError，不注册、不初始化。
// 2. 上下文接入成功后，向该runtime的registry注册本次保存的对应静态service。
//    失败：保存实际ServiceError并立即返回；保留已发布context和原服务，
//          不覆盖、换号、移除context或自动重试。
// 3. 注册成功才初始化本库固定端口4091。
//    失败：保存实际ServiceError并立即返回；不改写框架已形成的状态：
//          bind_params失败保持旧状态，Service::init失败才形成Failed。
// 4. 初始化成功才返回成功；这只表示该runtime本步完成，不代表业务就绪。
// 后置：任一步首错停止，不回滚已完成前项，不持binding状态锁进入registry。
//       装配失败由宿主处理并close/丢弃已返回候选；不关闭共享通信设施。
//       无静态注销或按owner删context操作，不在旧容器盲目重装。
pub(super) fn install_service<T, C>(
    coordinator: &TracingCoordinator<T, C>,
    runtime: &ServiceRuntime<C>,
) -> Result<(), CoordinationError>
where
    C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>,
    T: Deref<Target = ServiceTerminal<C>> + Send + Sync + 'static,
{
    // 共用发布入口完成准入/预占检查；返回后已无binding锁，再进入宿主registry。
    coordinator.bind_service_context(runtime)?;
    let registry = runtime.registry();
    registry
        .register(coordinator.service)
        .map_err(|error| CoordinationError {
            kind: CoordinationErrorKind::Service(error),
        })?;
    // 首错即停，不撤销已发布context或框架部分状态；普通失败由宿主收尾候选。
    registry
        .init(runtime, crate::COORDINATION_SERVICE_PORT.into())
        .map_err(|error| CoordinationError {
            kind: CoordinationErrorKind::Service(error),
        })
}

// send_selection
//   向共享client委托一次指定发送，原参数和错误不改写
pub(super) fn send_selection<T, C>(
    coordinator: &TracingCoordinator<T, C>,
    remote: &TraceNodeId,
    selection: TraceSelection,
    source: &TraceNodeId,
) -> Result<(), CoordinationSendError>
where
    C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>,
    T: Deref<Target = ServiceTerminal<C>> + Send + Sync + 'static,
{
    // 本次source由宿主指定，不用保存local覆盖；单次委托并原样返回发送结果。
    coordinator.client.send_selection(remote, selection, source)
}

// close
//   委托binding关闭
pub(super) fn close<T, C>(coordinator: &TracingCoordinator<T, C>)
where
    C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>,
    T: Deref<Target = ServiceTerminal<C>> + Send + Sync + 'static,
{
    // 只关闭本对象的新准入；不等待在途、不注销服务，也不关闭任何宿主共享设施。
    coordinator.binding.close();
}

// fmt_coordination_error
//   写真实分类和直接原因，传播fmt错误，不日志/改状态
pub(super) fn fmt_coordination_error(
    error: &CoordinationError,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    // 外层分类独立于真实原因文本；直接返回写出结果，首次fmt失败即结束。
    // 验证：coordinator_test中的四类真实错误及逐容量写失败场景。
    match &error.kind {
        CoordinationErrorKind::Binding(source) => write!(f, "协调绑定失败：{source}"),
        CoordinationErrorKind::Service(source) => write!(f, "协调服务失败：{source}"),
        CoordinationErrorKind::CodecLayerMissing => {
            write!(f, "协调编码层缺失：{}", service::COORDINATION_SERVICE_LAYER)
        }
        CoordinationErrorKind::ContextAlreadyInstalled => f.write_str("协调上下文已存在"),
    }
}

// coordination_error_source
//   借用Binding/Service直接原因，其余无源
pub(super) fn coordination_error_source(
    error: &CoordinationError,
) -> Option<&(dyn Error + 'static)> {
    // 借用实际保存的直接原因，不克隆、重建、格式化或调用其source跳过一层。
    match &error.kind {
        CoordinationErrorKind::Binding(source) => Some(source),
        CoordinationErrorKind::Service(source) => Some(source),
        CoordinationErrorKind::CodecLayerMissing
        | CoordinationErrorKind::ContextAlreadyInstalled => None,
    }
}
