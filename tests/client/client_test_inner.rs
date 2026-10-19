//! slice-client 测试实现：仅经冻结client/binding/protocol壳及依赖公开出口驱动。
//! @api:src/client.rs @impl:src/client_inner.rs
//! @test:tests/client/client_test.rs
//! @design-doc:design/tracing-coordinator/07-design/call-chain.md#slice-client
//!
//! 真实ServiceTerminal与DefaultCodec/MultiLayerCodec执行生产路由及编解码。
//! 宿主传输观察器只接管终端交来的拥有式字节、计数并提供受控受理结果，
//! 不实现准入、协议转换、发送重试或接收处理，不冒充真实QUIC/远端处理。
//! 各场景不读写client/Binding内部字段、不构造私有错误Kind、不导入生产芯。
//! from=None、精确终端调用次数、同一发送委托、所有分支锁外释放、中毒映射和
//! 无隐藏任务须结合实现后独立源码审查；传输计数不是这些内部属性的单独证明。
//! 仅new场景能在当前基线到达自身TODO；其余目标红线待前置实现后逐芯观察。

use crate::binding::{BindingError, CoordinationBinding};
use crate::client::{CoordinationClient, CoordinationSendError};
use crate::protocol::{
    register_fory_types, selection_to_message, validate_selection_message, ProtocolError,
    SelectionMessage,
};
use fory::Fory;
use pi_logger::{TraceId, TraceNodeId, TraceNotifier, TraceSelection};
use pi_service::{
    DefaultCodec, MultiLayerCodec, ServiceAddress, ServiceCodec, ServiceError, ServiceHost,
    ServiceTerminal, ServiceTransport,
};
use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    io::Read,
    ops::Deref,
    panic::{catch_unwind, AssertUnwindSafe},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tracing::{
    field::{Field, Visit},
    span::{Attributes, Id, Record},
    Event, Level, Metadata, Subscriber,
};

// 两个不同合法SHA-256 PeerId固定值；真实FromStr校准后才作为目标前置。
const REMOTE_A: &str = "QmYwAPJzv5CZsnAzt8auVZRnGi2CkEJjQd2B5Hc9EcAm7H";
const REMOTE_B: &str = "QmYwAPJzv5CZsnAzt8auVZRnGi2CkEJjQd2B5Hc9EcAm7J";
const TRACE: [u8; 16] = [
    1, 35, 69, 103, 137, 171, 205, 239, 254, 220, 186, 152, 118, 84, 50, 16,
];
const LOCAL: &str = "  保存/节点é/e\u{301}🦀\0 ";
const SOURCE: &str = "  显式/节点🦀\0 ";
const TARGET: &str = "pi_tracing_coordinator::client";
const WAIT: Duration = Duration::from_secs(3);

/// 步骤标记只区分目标与前置TODO，不能把后续场景的new失败当作发送/错误trait红线。
fn step(value: &str) {
    eprintln!("CLIENT_STEP={value}");
}

/// 固定合法期限不依赖now；本层允许已过期输入通过，不复制记录器裁剪策略。
fn notice() -> TraceSelection {
    TraceSelection {
        trace_id: TraceId::from_bytes(TRACE),
        expires_at: UNIX_EPOCH.checked_add(Duration::new(1, 100)).unwrap(),
    }
}

/// PeerId类型由真实Remote载荷推导，不为测试新增libp2p直接依赖或自造解析器。
fn host(raw: &str) -> ServiceHost {
    ServiceHost::Remote(raw.parse().expect("依赖前置：固定PeerId必须可解析"))
}

/// 候选独占登记后才发布；使用本库公开集中登记，不在测试补登记消息类型。
fn registered_fory() -> Arc<Fory> {
    assert!(
        !fory_core::error::should_panic_on_error(),
        "普通编码错误场景要求FORY_PANIC_ON_ERROR=false，环境错误不是目标RED"
    );
    let mut candidate = Fory::default();
    register_fory_types(&mut candidate).expect("前置：第2块真实登记必须成功");
    Arc::new(candidate)
}

/// 两份独立类型表可用于发送与权威解码，避免编码器写错后被同一替身算法抵消。
fn default_codec() -> Arc<DefaultCodec> {
    Arc::new(DefaultCodec::new(registered_fory()))
}

/// 由真实codec解码，再经已验收合法校验/消费出口观察业务，不读取消息私有字段。
fn assert_message<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    codec: &C,
    bytes: Vec<u8>,
    trace: [u8; 16],
    source: &str,
    deadline: SystemTime,
) {
    let message: SelectionMessage = codec.default_decode(bytes).expect("真实解码");
    let (selection, actual_source) = validate_selection_message(message)
        .expect("client产生的消息必须业务合法")
        .into_parts();
    assert_eq!(selection.trace_id.to_bytes(), trace);
    assert_eq!(actual_source.as_str(), source);
    assert_eq!(selection.expires_at, deadline);
}

/// 受控宿主受理结果；错误发生前仍保存提交字节，证明Err不能推断外部零效果。
#[derive(Clone, Copy)]
enum Acceptance {
    Ok,
    Runtime,
    Codec,
    Port,
    Disconnected,
    Closed,
}

/// 宿主出口收到的拥有式内容，生命周期独立于调用入参。
#[derive(Clone)]
struct Submission {
    address: ServiceAddress,
    bytes: Vec<u8>,
    thread: thread::ThreadId,
}

/// 只观测合法传输边界，不代替真实终端路由或client发送逻辑。
struct Transport {
    local: ServiceHost,
    result: Acceptance,
    submissions: Mutex<Vec<Submission>>,
    host_queries: AtomicUsize,
    other_calls: AtomicUsize,
}

impl Transport {
    fn new(local: ServiceHost, result: Acceptance) -> Arc<Self> {
        Arc::new(Self {
            local,
            result,
            submissions: Mutex::new(Vec::new()),
            host_queries: AtomicUsize::new(0),
            other_calls: AtomicUsize::new(0),
        })
    }

    fn saved(&self) -> Vec<Submission> {
        self.submissions.lock().unwrap().clone()
    }

    /// 前置错误不能触达终端路由或传输；不把这个观测扩大为私有方法调用计数。
    fn assert_untouched(&self) {
        assert!(self.saved().is_empty());
        assert_eq!(self.host_queries.load(Ordering::SeqCst), 0);
        assert_eq!(self.other_calls.load(Ordering::SeqCst), 0);
    }
}

impl<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>> ServiceTransport<C> for Transport {
    fn local_host(&self) -> ServiceHost {
        self.host_queries.fetch_add(1, Ordering::SeqCst);
        self.local.clone()
    }

    fn send(&self, address: ServiceAddress, input: Vec<u8>) -> Result<(), ServiceError> {
        self.submissions.lock().unwrap().push(Submission {
            address: address.clone(),
            bytes: input,
            thread: thread::current().id(),
        });
        match self.result {
            Acceptance::Ok => Ok(()),
            Acceptance::Runtime => Err(ServiceError::RuntimeError("宿主拒收/保留原因".into())),
            Acceptance::Codec => Err(ServiceError::CodecError("提交阶段同名CodecError".into())),
            Acceptance::Port => Err(ServiceError::SendPortFailed(17)),
            Acceptance::Disconnected => Err(ServiceError::RemoteHostNotConnected(address.host)),
            Acceptance::Closed => Err(ServiceError::RemoteConnectionClosed(address.host)),
        }
    }

    fn rpc(
        &self,
        _address: ServiceAddress,
        _input: Vec<u8>,
        _timeout: Option<Duration>,
    ) -> futures::future::BoxFuture<'static, Result<Vec<u8>, ServiceError>> {
        self.other_calls.fetch_add(1, Ordering::SeqCst);
        panic!("本票据不允许RPC");
    }

    fn reconnect(
        &self,
        _target: ServiceHost,
    ) -> futures::future::BoxFuture<'static, Result<(), ServiceError>> {
        self.other_calls.fetch_add(1, Ordering::SeqCst);
        panic!("本票据不允许自动重连");
    }

    fn close_connection(
        &self,
        _target: ServiceHost,
    ) -> futures::future::BoxFuture<'static, Result<(), ServiceError>> {
        self.other_calls.fetch_add(1, Ordering::SeqCst);
        panic!("本票据不关闭宿主连接");
    }

    fn is_connected(&self, _target: ServiceHost) -> bool {
        self.other_calls.fetch_add(1, Ordering::SeqCst);
        panic!("本票据不做发送前连接探测");
    }
}

/// 泛型T的合法宿主包装：Deref始终交出同一真实终端。
/// 可选动作仅控制宿主自身引用或同步观察窗口，不触达client/binding内部。
struct HostTerminal<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>> {
    terminal: ServiceTerminal<C>,
    on_deref: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    derefs: AtomicUsize,
    drops: Arc<AtomicUsize>,
    panic_on_drop: AtomicBool,
}

impl<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>> Deref for HostTerminal<C> {
    type Target = ServiceTerminal<C>;

    fn deref(&self) -> &Self::Target {
        self.derefs.fetch_add(1, Ordering::SeqCst);
        let action = self.on_deref.lock().unwrap().take();
        if let Some(action) = action {
            action();
        }
        &self.terminal
    }
}

impl<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>> Drop for HostTerminal<C> {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.panic_on_drop.load(Ordering::SeqCst),
            "宿主终端最后引用析构panic"
        );
    }
}

/// 真实终端只绑定宿主传输；本块不需要本库service/coordinator未实现芯。
fn terminal<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    transport: &Arc<Transport>,
    drops: &Arc<AtomicUsize>,
) -> Arc<HostTerminal<C>> {
    Arc::new(HostTerminal {
        terminal: ServiceTerminal::from_transport(transport.clone()),
        on_deref: Mutex::new(None),
        derefs: AtomicUsize::new(0),
        drops: drops.clone(),
        panic_on_drop: AtomicBool::new(false),
    })
}

/// 装配只使用三个模块壳；new步骤标记防止前置TODO冒充发送目标RED。
fn client<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    terminal: &Arc<HostTerminal<C>>,
    codec: Arc<C>,
    binding: &CoordinationBinding,
    local: &str,
) -> CoordinationClient<HostTerminal<C>, C> {
    step("new");
    CoordinationClient::new(terminal, codec, binding.handle(), TraceNodeId::new(local))
}

/// 单次发送只经冻结壳，不在helper补准入、转换、编码、提交或日志。
fn send<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    client: &CoordinationClient<HostTerminal<C>, C>,
    remote: &str,
    selection: TraceSelection,
    source: &str,
) -> Result<(), CoordinationSendError> {
    step("send_selection");
    client.send_selection(
        &TraceNodeId::new(remote),
        selection,
        &TraceNodeId::new(source),
    )
}

/// 只检查本库出口事件，捕获时记录终端析构计数与当前线程，避免时序推测。
#[derive(Clone)]
struct Warning {
    level: Level,
    target: String,
    fields: BTreeMap<String, String>,
    terminal_drops: usize,
    thread: thread::ThreadId,
}

struct Fields(BTreeMap<String, String>);

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().to_owned(), value.to_string());
    }
}

/// 线程局部宿主订阅者；观察本库根及全部子target的各级事件，使错误target或额外日志可见。
/// 依赖及同名前缀的其它库不计入；不初始化全局logger/subscriber，也不实现生产观测行为。
#[derive(Clone)]
struct Observer {
    enabled: bool,
    warnings: Arc<Mutex<Vec<Warning>>>,
    drops: Arc<AtomicUsize>,
    panic_on_event: bool,
}

impl Observer {
    fn new(drops: &Arc<AtomicUsize>) -> Self {
        Self {
            enabled: true,
            warnings: Arc::new(Mutex::new(Vec::new())),
            drops: drops.clone(),
            panic_on_event: false,
        }
    }

    fn saved(&self) -> Vec<Warning> {
        self.warnings.lock().unwrap().clone()
    }
}

impl Subscriber for Observer {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        self.enabled
            && (metadata.target() == "pi_tracing_coordinator"
                || metadata.target().starts_with("pi_tracing_coordinator::"))
    }

    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}
    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}
    fn enter(&self, _span: &Id) {}
    fn exit(&self, _span: &Id) {}

    fn event(&self, event: &Event<'_>) {
        // 与enabled共享同一命名空间规则，不能在记录时再次缩窄到正确警告target。
        if !self.enabled(event.metadata()) {
            return;
        }
        let mut fields = Fields(BTreeMap::new());
        event.record(&mut fields);
        self.warnings.lock().unwrap().push(Warning {
            level: *event.metadata().level(),
            target: event.metadata().target().to_owned(),
            fields: fields.0,
            terminal_drops: self.drops.load(Ordering::SeqCst),
            thread: thread::current().id(),
        });
        assert!(!self.panic_on_event, "宿主subscriber测试panic");
    }
}

/// 只断言冻结字段及级别，不冻结完整日志message或标点。
fn assert_warning(event: &Warning, remote: &str, error: &CoordinationSendError) {
    assert_eq!(event.level, Level::WARN);
    assert_eq!(event.target, TARGET);
    assert_eq!(event.fields.get("remote").unwrap(), remote);
    assert_eq!(event.fields.get("service_port").unwrap(), "4091");
    assert_eq!(event.fields.get("error").unwrap(), &error.to_string());
    assert!(!event.fields.contains_key("trace_id"));
    assert!(!event.fields.contains_key("operation_id"));
    assert_eq!(event.thread, thread::current().id());
}

/// 原始错误类型由泛型推导；没有直接依赖libp2p，仍能证明非字符串替代及下层链保真。
fn assert_original_cause<E: Error + 'static>(actual: &(dyn Error + 'static), expected: &E) {
    let typed = actual
        .downcast_ref::<E>()
        .expect("必须保留原始依赖错误类型");
    assert_eq!(typed.to_string(), expected.to_string());
    let mut left = actual.source();
    let mut right = expected.source();
    for _ in 0..8 {
        match (left, right) {
            (Some(a), Some(b)) => {
                assert_eq!(a.to_string(), b.to_string());
                left = a.source();
                right = b.source();
            }
            (None, None) => return,
            _ => panic!("原始原因链长度不符"),
        }
    }
    panic!("测试错误链不得无界或成环");
}

/// 危险析构及有界交错使用独立进程；20秒硬截止，子进程必须恰好命中当前场景。
fn isolated(action: impl FnOnce()) {
    const ENV: &str = "PI_SLICE_CLIENT_TEST_CHILD";
    let current = thread::current();
    let name = current.name().expect("libtest场景名").to_owned();
    if std::env::var(ENV).ok().as_deref() == Some(name.as_str()) {
        action();
        return;
    }
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &name, "--nocapture", "--test-threads=1"])
        .env(ENV, &name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("隔离进程启动失败不是目标RED");
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let output = thread::spawn(move || {
        let mut text = String::new();
        stdout.read_to_string(&mut text).unwrap();
        text
    });
    let errors = thread::spawn(move || {
        let mut text = String::new();
        stderr.read_to_string(&mut text).unwrap();
        text
    });
    let started = Instant::now();
    let mut timed_out = false;
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if started.elapsed() >= Duration::from_secs(20) {
            timed_out = true;
            child.kill().unwrap();
            break child.wait().unwrap();
        }
        thread::sleep(Duration::from_millis(5));
    };
    let output = output.join().unwrap();
    let errors = errors.join().unwrap();
    assert!(
        !timed_out,
        "隔离测试超过20秒未完成，不是有效RED：{output}\n{errors}"
    );
    assert!(
        output.contains("running 1 test"),
        "未命中唯一测试：{output}\n{errors}"
    );
    assert!(
        status.success(),
        "场景失败：{name}；按CLIENT_STEP区分目标与前置TODO。\n{output}\n{errors}"
    );
    assert!(output.contains("1 passed; 0 failed; 0 ignored"));
}

/// 测试侧只为场景组织命名阶段，不读取生产私有Kind或依赖Debug字符串分类。
#[derive(Clone, Copy, Debug)]
enum Stage {
    Binding,
    Terminal,
    Target,
    Protocol,
    Encode,
    Submit,
}

/// 先移除完整原因的所有原样出现，再检查剩余阶段说明；原因不能替错误阶段提供关键词。
/// 不约束原因与阶段的先后、分隔符或完整句式；此断言也由真实依赖原因的互换负例校准。
fn assert_stage_text(text: &str, stage: Stage, reason: Option<&str>) {
    let stage_text = match reason {
        Some(reason) => {
            assert!(!reason.is_empty(), "原因前置不能为空");
            assert!(text.contains(reason), "未显示完整真实原因：{text}");
            text.replace(reason, "")
        }
        None => {
            assert!(matches!(stage, Stage::Terminal), "非终端失效必须有原因");
            text.to_owned()
        }
    }
    .to_lowercase();
    let words: &[&str] = match stage {
        Stage::Binding => &["绑定", "准入", "binding"],
        Stage::Terminal => &["终端", "terminal"],
        Stage::Target => &["目标", "解析", "target"],
        Stage::Protocol => &["协议", "业务转换", "protocol"],
        Stage::Encode => &["编码", "encode"],
        Stage::Submit => &["提交", "submit"],
    };
    assert!(
        words.iter().any(|word| stage_text.contains(word)),
        "阶段{stage:?}没有准确显示：{stage_text}；完整文本：{text}"
    );
}

/// 原因显式由独立预期或已核验的直接原因传入，Display场景不依赖生产source实现来划分文本。
fn assert_stage(error: &CoordinationSendError, stage: Stage, reason: Option<&str>) {
    step("fmt_send_error");
    assert_stage_text(&error.to_string(), stage, reason);
}

/// 真实解析失败经目标Remote载荷推导原始错误类型，并比对完整原因链。
fn assert_parse_error(error: &CoordinationSendError, raw: &str) -> String {
    let expected = match raw.parse() {
        Ok(peer) => {
            let _unexpected = ServiceHost::Remote(peer);
            panic!("负面对照必须解析失败");
        }
        Err(error) => error,
    };
    step("send_error_source");
    assert_original_cause(error.source().expect("解析错误必须保留原始原因"), &expected);
    expected.to_string()
}

/// 通过合法实际调用生成六类错误，记录传输出口以证明trait本身无提交副作用。
struct Failure {
    stage: Stage,
    error: CoordinationSendError,
    expected_reason: Option<String>,
    transport: Arc<Transport>,
}

fn failures() -> Vec<Failure> {
    let mut failures = Vec::new();
    for stage in [
        Stage::Binding,
        Stage::Terminal,
        Stage::Target,
        Stage::Protocol,
        Stage::Encode,
        Stage::Submit,
    ] {
        let binding = CoordinationBinding::new();
        let transport = Transport::new(ServiceHost::Local(0), Acceptance::Runtime);
        let drops = Arc::new(AtomicUsize::new(0));
        let terminal = terminal(&transport, &drops);
        let codec = if matches!(stage, Stage::Encode) {
            Arc::new(DefaultCodec::default())
        } else {
            default_codec()
        };
        let expected_reason = match stage {
            Stage::Binding => Some(BindingError::Closed.to_string()),
            Stage::Terminal => None,
            Stage::Target => {
                let expected = match "0".parse() {
                    Ok(peer) => {
                        let _host = ServiceHost::Remote(peer);
                        panic!("0必须非法")
                    }
                    Err(error) => error,
                };
                Some(expected.to_string())
            }
            Stage::Protocol => Some(ProtocolError::InvalidTraceId.to_string()),
            Stage::Encode => {
                let message = selection_to_message(notice(), &TraceNodeId::new(SOURCE)).unwrap();
                Some(codec.default_encode(&message).unwrap_err().to_string())
            }
            Stage::Submit => {
                Some(ServiceError::RuntimeError("宿主拒收/保留原因".into()).to_string())
            }
        };
        let client = client(&terminal, codec, &binding, LOCAL);
        if matches!(stage, Stage::Binding) {
            binding.close();
        }
        if matches!(stage, Stage::Terminal) {
            drop(terminal);
        }
        let mut selection = notice();
        if matches!(stage, Stage::Protocol) {
            selection.trace_id = TraceId::INVALID;
        }
        let remote = if matches!(stage, Stage::Target) {
            "0"
        } else {
            REMOTE_A
        };
        let error = send(&client, remote, selection, SOURCE).unwrap_err();
        failures.push(Failure {
            stage,
            error,
            expected_reason,
            transport,
        });
    }
    failures
}

/// 校准真实编码、终端路由和拥有式观察，绿色只计依赖前提，不计client行为。
pub(super) fn test_client_fixture_real_codec_terminal_controls() {
    assert_ne!(host(REMOTE_A), host(REMOTE_B));
    let transport = Transport::new(ServiceHost::Local(0), Acceptance::Ok);
    let terminal = ServiceTerminal::<DefaultCodec>::from_transport(transport.clone());
    let message = selection_to_message(notice(), &TraceNodeId::new(SOURCE)).unwrap();
    let bytes = default_codec().default_encode(&message).unwrap();
    terminal
        .send(
            ServiceAddress::new(host(REMOTE_A), 4091u16.into()),
            bytes,
            None,
        )
        .unwrap();
    let submitted = transport.saved();
    assert_eq!(submitted.len(), 1);
    assert_message(
        default_codec().as_ref(),
        submitted[0].bytes.clone(),
        TRACE,
        SOURCE,
        notice().expires_at,
    );
    assert_eq!(submitted[0].address.host, host(REMOTE_A));
    assert_eq!(u16::from(submitted[0].address.port), 4091);
    assert_eq!(submitted[0].thread, thread::current().id());
    assert_eq!(transport.other_calls.load(Ordering::SeqCst), 0);
    let error = DefaultCodec::default()
        .default_encode(&message)
        .unwrap_err();
    assert!(
        matches!(&error, ServiceError::CodecError(reason) if reason.contains("not found in type_id_index"))
    );
    let encode_reason = error.to_string();
    let transport = Transport::new(ServiceHost::Local(0), Acceptance::Port);
    let terminal = ServiceTerminal::<DefaultCodec>::from_transport(transport.clone());
    assert!(matches!(
        terminal.send(ServiceAddress::new(host(REMOTE_A), 4091u16.into()), vec![1], None),
        Err(ServiceError::RemoteServiceNotFound(target, port)) if target == host(REMOTE_A) && u16::from(port) == 4091
    ));
    assert_eq!(transport.saved().len(), 1);

    // 两种原因均来自真实公开依赖出口；只替换显示标签校准测试断言，不构造生产私有错误。
    let transport = Transport::new(ServiceHost::Local(0), Acceptance::Codec);
    let terminal = ServiceTerminal::<DefaultCodec>::from_transport(transport.clone());
    let error = terminal
        .send(
            ServiceAddress::new(host(REMOTE_A), 4091u16.into()),
            vec![1],
            None,
        )
        .unwrap_err();
    assert!(
        matches!(&error, ServiceError::CodecError(reason) if reason == "提交阶段同名CodecError")
    );
    assert_eq!(transport.saved().len(), 1);
    let submit_reason = error.to_string();
    assert!(encode_reason.contains("encode"));
    assert!(submit_reason.contains("提交"));
    for (stage, correct, swapped, reason) in [
        (Stage::Encode, "编码失败", "提交失败", encode_reason),
        (Stage::Submit, "提交失败", "编码失败", submit_reason),
    ] {
        for text in [
            format!("{correct}: {reason}"),
            format!("{reason}；{correct}"),
        ] {
            assert_stage_text(&text, stage, Some(&reason));
        }
        let text = format!("{swapped}: {reason}");
        let panic = catch_unwind(|| assert_stage_text(&text, stage, Some(&reason)))
            .expect_err("阶段互换必须被产品场景共用的断言拒绝，不能借原因关键词通过");
        let message = panic.downcast_ref::<String>().expect("阶段断言的具体诊断");
        assert!(message.starts_with(&format!("阶段{stage:?}没有准确显示：")));
    }
}

/// 校准正确警告、错误子target及根target事件均可观察，并排除依赖和同名前缀噪声。
/// 额外事件必须进入零/唯一事件断言的同一saved集合；校准不能替代notify产品证据。
pub(super) fn test_client_fixture_warning_observer_controls() {
    let drops = Arc::new(AtomicUsize::new(2));
    let observer = Observer::new(&drops);
    tracing::subscriber::with_default(observer.clone(), || {
        tracing::warn!(target: "pi_tracing_coordinator::client", remote = %REMOTE_A, service_port = 4091u16, error = %"校准原因");
    });
    let events = observer.saved();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].level, Level::WARN);
    assert_eq!(events[0].target, TARGET);
    assert_eq!(events[0].thread, thread::current().id());
    assert_eq!(events[0].fields.get("remote").unwrap(), REMOTE_A);
    assert_eq!(events[0].fields.get("error").unwrap(), "校准原因");
    assert_eq!(events[0].fields.get("service_port").unwrap(), "4091");
    assert_eq!(events[0].terminal_drops, 2);
    tracing::subscriber::with_default(observer.clone(), || {
        tracing::warn!(target: "pi_tracing_coordinator::client_inner", error = "错误target也必须计入");
        tracing::info!(target: "pi_tracing_coordinator", "本库根target的额外事件");
        tracing::warn!(target: "pi_service::client", "依赖噪声不计入");
        tracing::warn!(target: "pi_tracing_coordinator_other::client", "相似库名前缀不计入");
    });
    let events = observer.saved();
    assert_eq!(
        events.len(),
        3,
        "错误target和根target都必须进入同一事件计数"
    );
    assert_eq!(events[1].target, "pi_tracing_coordinator::client_inner");
    assert_eq!(events[1].level, Level::WARN);
    assert_eq!(
        events[1].fields.get("error").unwrap(),
        "错误target也必须计入"
    );
    assert_eq!(events[2].target, "pi_tracing_coordinator");
    assert_eq!(events[2].level, Level::INFO);
    for event in &events {
        assert_eq!(event.terminal_drops, 2);
        assert_eq!(event.thread, thread::current().id());
    }
    let mut disabled = observer.clone();
    disabled.enabled = false;
    tracing::subscriber::with_default(disabled, || {
        tracing::warn!(target: "pi_tracing_coordinator::client", error = "不应记录");
        tracing::warn!(target: "pi_tracing_coordinator::client_inner", error = "关闭后其它本库target也不记录");
    });
    assert_eq!(observer.saved().len(), 3);
}

/// 真实弱生命周期保护构造组合；不读取client字段、不将引用计数赋值当行为测试。
/// 这是可独立完成new红绿的场景，不调用其余四个尚未实现的client芯。
pub(super) fn test_client_new_is_lazy_and_respects_resource_ownership() {
    let binding = CoordinationBinding::new();
    binding.close();
    let transport = Transport::new(ServiceHost::Local(0), Acceptance::Runtime);
    let drops = Arc::new(AtomicUsize::new(0));
    let terminal = terminal(&transport, &drops);
    let terminal_weak = Arc::downgrade(&terminal);
    let codec = Arc::new(DefaultCodec::default());
    let codec_weak = Arc::downgrade(&codec);
    let client = client(&terminal, codec, &binding, "");
    assert_eq!(terminal.derefs.load(Ordering::SeqCst), 0);
    transport.assert_untouched();
    drop(terminal);
    assert!(terminal_weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(codec_weak.upgrade().is_some());
    drop(client);
    assert!(codec_weak.upgrade().is_none());
    transport.assert_untouched();
}

/// 独立解码器核验提交后的拥有式数据，不以client保存字段或引用计数赋值作为正确性证据。
pub(super) fn test_client_explicit_send_preserves_content_once() {
    let binding = CoordinationBinding::new();
    let transport = Transport::new(ServiceHost::Local(0), Acceptance::Ok);
    let drops = Arc::new(AtomicUsize::new(0));
    let terminal = terminal(&transport, &drops);
    let client = client(&terminal, default_codec(), &binding, LOCAL);
    let observer = Observer::new(&drops);
    let remote = TraceNodeId::new(REMOTE_A);
    let source = TraceNodeId::new(SOURCE);
    step("send_selection");
    tracing::subscriber::with_default(observer.clone(), || {
        client.send_selection(&remote, notice(), &source)
    })
    .unwrap();
    drop(source);
    drop(remote);
    drop(client);
    let submitted = transport.saved();
    assert_eq!(submitted.len(), 1);
    assert_eq!(submitted[0].address.host, host(REMOTE_A));
    assert_eq!(u16::from(submitted[0].address.port), 4091);
    assert_eq!(crate::COORDINATION_SERVICE_PORT, 4091);
    assert_eq!(submitted[0].thread, thread::current().id());
    assert_message(
        default_codec().as_ref(),
        submitted[0].bytes.clone(),
        TRACE,
        SOURCE,
        notice().expires_at,
    );
    assert_eq!(transport.other_calls.load(Ordering::SeqCst), 0);
    assert!(observer.saved().is_empty());
    // 传输只受理保存，没有模拟接收；Ok不可被测试扩大为远端完成。
    assert_eq!(drops.load(Ordering::SeqCst), 0);
}

/// 默认层故意未登记，只有冷路径明确选择的层可成功；client不得重选或回退。
pub(super) fn test_client_multi_layer_uses_saved_prebound_codec() {
    let mut codec = MultiLayerCodec::default();
    codec.register_layer_fory("tracing-coordinator", registered_fory());
    assert!(codec.has_layer("tracing-coordinator"));
    codec.switch_layer("tracing-coordinator").unwrap();
    let codec = Arc::new(codec);
    let binding = CoordinationBinding::new();
    let transport = Transport::new(ServiceHost::Local(0), Acceptance::Ok);
    let drops = Arc::new(AtomicUsize::new(0));
    let terminal = terminal(&transport, &drops);
    let client = client(&terminal, codec.clone(), &binding, LOCAL);
    send(&client, REMOTE_B, notice(), SOURCE).unwrap();
    assert_eq!(codec.current_layer().as_str(), "tracing-coordinator");
    let submitted = transport.saved();
    assert_eq!(submitted.len(), 1);
    assert_eq!(submitted[0].address.host, host(REMOTE_B));
    assert_eq!(u16::from(submitted[0].address.port), 4091);
    assert_message(
        default_codec().as_ref(),
        submitted[0].bytes.clone(),
        TRACE,
        SOURCE,
        notice().expires_at,
    );
}

/// 两次相同及一次不同目标分别受理，保护无跨调用去重/缓存且每次采用本次目标与来源。
pub(super) fn test_client_repeated_calls_are_independent_attempts() {
    let binding = CoordinationBinding::new();
    binding.bind_recorder(None).unwrap();
    let transport = Transport::new(ServiceHost::Local(0), Acceptance::Ok);
    let drops = Arc::new(AtomicUsize::new(0));
    let terminal = terminal(&transport, &drops);
    let client = client(&terminal, default_codec(), &binding, LOCAL);
    for (remote, source) in [(REMOTE_A, "A"), (REMOTE_A, "A"), (REMOTE_B, "B")] {
        send(&client, remote, notice(), source).unwrap();
    }
    let submitted = transport.saved();
    assert_eq!(submitted.len(), 3);
    for (entry, (remote, source)) in
        submitted
            .iter()
            .zip([(REMOTE_A, "A"), (REMOTE_A, "A"), (REMOTE_B, "B")])
    {
        assert_eq!(entry.address.host, host(remote));
        assert_message(
            default_codec().as_ref(),
            entry.bytes.clone(),
            TRACE,
            source,
            notice().expires_at,
        );
    }
    assert_eq!(transport.other_calls.load(Ordering::SeqCst), 0);
}

/// 空local不影响显式source；notify到protocol阶段才拒绝，且不增加一次提交。
pub(super) fn test_client_empty_local_is_deferred_and_explicit_source_wins() {
    let binding = CoordinationBinding::new();
    let transport = Transport::new(ServiceHost::Local(0), Acceptance::Ok);
    let drops = Arc::new(AtomicUsize::new(0));
    let terminal = terminal(&transport, &drops);
    let client = client(&terminal, default_codec(), &binding, "");
    send(&client, REMOTE_A, notice(), SOURCE).unwrap();
    let observer = Observer::new(&drops);
    step("notify");
    let error = tracing::subscriber::with_default(observer.clone(), || {
        client.notify(&TraceNodeId::new(REMOTE_A), notice())
    })
    .unwrap_err();
    let error = error
        .downcast_ref::<CoordinationSendError>()
        .expect("anyhow必须接管发送错误");
    assert!(matches!(
        error.source().unwrap().downcast_ref::<ProtocolError>(),
        Some(ProtocolError::EmptyIdentity { field: "source" })
    ));
    let submitted = transport.saved();
    assert_eq!(submitted.len(), 1);
    assert_message(
        default_codec().as_ref(),
        submitted[0].bytes.clone(),
        TRACE,
        SOURCE,
        notice().expires_at,
    );
    assert_eq!(observer.saved().len(), 1);
    assert_warning(&observer.saved()[0], REMOTE_A, error);
}

/// 成功通知只提交一次，真实消息source是保存local，且不产生成功或失败日志。
pub(super) fn test_client_notifier_uses_local_and_has_no_success_warning() {
    let binding = CoordinationBinding::new();
    let transport = Transport::new(ServiceHost::Local(0), Acceptance::Ok);
    let drops = Arc::new(AtomicUsize::new(0));
    let terminal = terminal(&transport, &drops);
    let client = client(&terminal, default_codec(), &binding, LOCAL);
    let observer = Observer::new(&drops);
    step("notify");
    tracing::subscriber::with_default(observer.clone(), || {
        client.notify(&TraceNodeId::new(REMOTE_B), notice())
    })
    .unwrap();
    let submitted = transport.saved();
    assert_eq!(submitted.len(), 1);
    assert_eq!(submitted[0].address.host, host(REMOTE_B));
    assert_message(
        default_codec().as_ref(),
        submitted[0].bytes.clone(),
        TRACE,
        LOCAL,
        notice().expires_at,
    );
    assert!(observer.saved().is_empty());
    assert_eq!(transport.other_calls.load(Ordering::SeqCst), 0);
}

/// 关闭/Owner消失均优先于死终端、坏目标和坏业务；不将合成StatePoisoned当真实中毒。
pub(super) fn test_client_binding_rejection_precedes_all_later_errors() {
    for drop_owner in [false, true] {
        let binding = CoordinationBinding::new();
        let transport = Transport::new(ServiceHost::Local(0), Acceptance::Runtime);
        let drops = Arc::new(AtomicUsize::new(0));
        let terminal = terminal(&transport, &drops);
        let client = client(&terminal, Arc::new(DefaultCodec::default()), &binding, "");
        if drop_owner {
            drop(binding);
        } else {
            binding.close();
        }
        drop(terminal);
        let error = send(
            &client,
            "0",
            TraceSelection {
                trace_id: TraceId::INVALID,
                ..notice()
            },
            "",
        )
        .unwrap_err();
        step("send_error_source");
        assert!(matches!(
            error.source().unwrap().downcast_ref::<BindingError>(),
            Some(BindingError::Closed)
        ));
        assert_stage(
            &error,
            Stage::Binding,
            Some(&BindingError::Closed.to_string()),
        );
        transport.assert_untouched();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

/// client/notifier存活不能保活宿主终端；失效先于解析或业务验证，且无伪造原因链。
pub(super) fn test_client_terminal_unavailable_precedes_parse_and_protocol() {
    let binding = CoordinationBinding::new();
    let transport = Transport::new(ServiceHost::Local(0), Acceptance::Runtime);
    let drops = Arc::new(AtomicUsize::new(0));
    let terminal = terminal(&transport, &drops);
    let weak = Arc::downgrade(&terminal);
    let client = client(&terminal, default_codec(), &binding, LOCAL);
    drop(terminal);
    assert!(weak.upgrade().is_none());
    let error = send(
        &client,
        "0",
        TraceSelection {
            trace_id: TraceId::INVALID,
            ..notice()
        },
        "",
    )
    .unwrap_err();
    step("send_error_source");
    assert!(error.source().is_none());
    assert_stage(&error, Stage::Terminal, None);
    transport.assert_untouched();
}

/// 不trim目标；Base58错误和合法Base58但非法multihash均保留原始解析类型及下层原因。
pub(super) fn test_client_target_parse_precedes_protocol_and_preserves_causes() {
    let binding = CoordinationBinding::new();
    let transport = Transport::new(ServiceHost::Local(0), Acceptance::Runtime);
    let drops = Arc::new(AtomicUsize::new(0));
    let terminal = terminal(&transport, &drops);
    let client = client(&terminal, default_codec(), &binding, LOCAL);
    for raw in ["", "0", "1", " 目标🦀 ", &format!(" {REMOTE_A}")] {
        let error = send(
            &client,
            raw,
            TraceSelection {
                trace_id: TraceId::INVALID,
                ..notice()
            },
            "",
        )
        .unwrap_err();
        let reason = assert_parse_error(&error, raw);
        assert_stage(&error, Stage::Target, Some(&reason));
        transport.assert_untouched();
    }
}

/// 多项坏业务与真实未登记编码条件同时存在，先Trace，再空source，均零次终端提交。
pub(super) fn test_client_protocol_priority_precedes_real_encoding_failure() {
    let binding = CoordinationBinding::new();
    let transport = Transport::new(ServiceHost::Local(0), Acceptance::Runtime);
    let drops = Arc::new(AtomicUsize::new(0));
    let terminal = terminal(&transport, &drops);
    let client = client(
        &terminal,
        Arc::new(DefaultCodec::default()),
        &binding,
        LOCAL,
    );
    #[cfg(target_os = "linux")]
    let deadlines = vec![
        notice().expires_at,
        UNIX_EPOCH
            .checked_sub(Duration::from_secs(11_644_473_601))
            .unwrap(),
    ];
    #[cfg(not(target_os = "linux"))]
    let deadlines = vec![notice().expires_at];
    for expires_at in deadlines {
        let error = send(
            &client,
            REMOTE_A,
            TraceSelection {
                trace_id: TraceId::INVALID,
                expires_at,
            },
            "",
        )
        .unwrap_err();
        assert!(matches!(
            error.source().unwrap().downcast_ref::<ProtocolError>(),
            Some(ProtocolError::InvalidTraceId)
        ));
        let error = send(
            &client,
            REMOTE_A,
            TraceSelection {
                trace_id: TraceId::from_bytes(TRACE),
                expires_at,
            },
            "",
        )
        .unwrap_err();
        assert!(matches!(
            error.source().unwrap().downcast_ref::<ProtocolError>(),
            Some(ProtocolError::EmptyIdentity { field: "source" })
        ));
        transport.assert_untouched();
    }
}

/// Linux可构造共同域之外SystemTime；不能将Windows不存在的对象伪造为业务输入。
#[cfg(target_os = "linux")]
pub(super) fn test_client_protocol_deadline_error_precedes_encoding() {
    let binding = CoordinationBinding::new();
    let transport = Transport::new(ServiceHost::Local(0), Acceptance::Runtime);
    let drops = Arc::new(AtomicUsize::new(0));
    let terminal = terminal(&transport, &drops);
    let client = client(
        &terminal,
        Arc::new(DefaultCodec::default()),
        &binding,
        LOCAL,
    );
    let selection = TraceSelection {
        expires_at: UNIX_EPOCH
            .checked_sub(Duration::from_secs(11_644_473_601))
            .unwrap(),
        ..notice()
    };
    let error = send(&client, REMOTE_A, selection, SOURCE).unwrap_err();
    assert!(matches!(
        error.source().unwrap().downcast_ref::<ProtocolError>(),
        Some(ProtocolError::DeadlineOutOfRange)
    ));
    transport.assert_untouched();
}

/// 真实缺少消息登记触发Encode，ServiceError完整原因保留而原Fory类型链已经不存在。
pub(super) fn test_client_real_encode_failure_has_no_submission() {
    assert!(!fory_core::error::should_panic_on_error());
    let binding = CoordinationBinding::new();
    let codec = Arc::new(DefaultCodec::default());
    let message = selection_to_message(notice(), &TraceNodeId::new(SOURCE)).unwrap();
    let expected = codec.default_encode(&message).unwrap_err();
    let transport = Transport::new(ServiceHost::Local(0), Acceptance::Runtime);
    let drops = Arc::new(AtomicUsize::new(0));
    let terminal = terminal(&transport, &drops);
    let client = client(&terminal, codec, &binding, LOCAL);
    let observer = Observer::new(&drops);
    let error = tracing::subscriber::with_default(observer.clone(), || {
        send(&client, REMOTE_A, notice(), SOURCE)
    })
    .unwrap_err();
    assert_stage(&error, Stage::Encode, Some(&expected.to_string()));
    let cause = error.source().unwrap();
    assert!(
        matches!(cause.downcast_ref::<ServiceError>(), Some(ServiceError::CodecError(reason)) if reason.contains("not found in type_id_index"))
    );
    assert_eq!(cause.to_string(), expected.to_string());
    assert!(cause.source().is_none());
    transport.assert_untouched();
    assert!(observer.saved().is_empty());
}

/// 所有错误来自真实终端返回；受控传输先保留字节再Err，不将拒收误解为外部零效果。
pub(super) fn test_client_submit_preserves_terminal_errors_and_never_retries() {
    for result in [
        Acceptance::Runtime,
        Acceptance::Codec,
        Acceptance::Port,
        Acceptance::Disconnected,
        Acceptance::Closed,
    ] {
        let binding = CoordinationBinding::new();
        let transport = Transport::new(ServiceHost::Local(0), result);
        let drops = Arc::new(AtomicUsize::new(0));
        let terminal = terminal(&transport, &drops);
        let client = client(&terminal, default_codec(), &binding, LOCAL);
        let observer = Observer::new(&drops);
        let error = tracing::subscriber::with_default(observer.clone(), || {
            send(&client, REMOTE_A, notice(), SOURCE)
        })
        .unwrap_err();
        let cause = error
            .source()
            .unwrap()
            .downcast_ref::<ServiceError>()
            .unwrap();
        match result {
            Acceptance::Runtime => assert!(
                matches!(cause, ServiceError::RuntimeError(reason) if reason == "宿主拒收/保留原因")
            ),
            Acceptance::Codec => assert!(
                matches!(cause, ServiceError::CodecError(reason) if reason == "提交阶段同名CodecError")
            ),
            Acceptance::Port => assert!(
                matches!(cause, ServiceError::RemoteServiceNotFound(target, port) if *target == host(REMOTE_A) && u16::from(*port) == 4091)
            ),
            Acceptance::Disconnected => assert!(
                matches!(cause, ServiceError::RemoteHostNotConnected(target) if *target == host(REMOTE_A))
            ),
            Acceptance::Closed => assert!(
                matches!(cause, ServiceError::RemoteConnectionClosed(target) if *target == host(REMOTE_A))
            ),
            Acceptance::Ok => panic!("本例必须失败"),
        }
        assert_stage(&error, Stage::Submit, Some(&cause.to_string()));
        assert!(cause.source().is_none());
        let submitted = transport.saved();
        assert_eq!(submitted.len(), 1);
        assert_message(
            default_codec().as_ref(),
            submitted[0].bytes.clone(),
            TRACE,
            SOURCE,
            notice().expires_at,
        );
        assert_eq!(transport.other_calls.load(Ordering::SeqCst), 0);
        assert!(observer.saved().is_empty());
    }
}

/// 目标等于宿主身份仍进入真实本地路由；无runtime的真实Submit错误不应被自发送检查替代。
pub(super) fn test_client_self_target_uses_terminal_local_routing() {
    let binding = CoordinationBinding::new();
    let transport = Transport::new(host(REMOTE_A), Acceptance::Ok);
    let drops = Arc::new(AtomicUsize::new(0));
    let terminal = terminal(&transport, &drops);
    let client = client(&terminal, default_codec(), &binding, LOCAL);
    let error = send(&client, REMOTE_A, notice(), SOURCE).unwrap_err();
    assert!(
        matches!(error.source().unwrap().downcast_ref::<ServiceError>(), Some(ServiceError::LocalRuntimeMissing(target)) if *target == host(REMOTE_A))
    );
    let reason = ServiceError::LocalRuntimeMissing(host(REMOTE_A)).to_string();
    assert_stage(&error, Stage::Submit, Some(&reason));
    assert!(transport.saved().is_empty());
    assert_eq!(transport.host_queries.load(Ordering::SeqCst), 1);
    assert_eq!(transport.other_calls.load(Ordering::SeqCst), 0);
}

/// 负余数用独立预期时刻观察发送组合，不能只测protocol转换后假设client使用它。
#[cfg(target_os = "linux")]
pub(super) fn test_client_normalized_deadlines_survive_real_encoding() {
    let binding = CoordinationBinding::new();
    let transport = Transport::new(ServiceHost::Local(0), Acceptance::Ok);
    let drops = Arc::new(AtomicUsize::new(0));
    let terminal = terminal(&transport, &drops);
    let client = client(&terminal, default_codec(), &binding, LOCAL);
    let examples = [
        (
            UNIX_EPOCH.checked_add(Duration::from_nanos(99)).unwrap(),
            UNIX_EPOCH,
        ),
        (
            UNIX_EPOCH.checked_sub(Duration::from_nanos(1)).unwrap(),
            UNIX_EPOCH.checked_sub(Duration::from_nanos(100)).unwrap(),
        ),
        (
            UNIX_EPOCH.checked_sub(Duration::from_nanos(101)).unwrap(),
            UNIX_EPOCH.checked_sub(Duration::from_nanos(200)).unwrap(),
        ),
    ];
    for (expires_at, _) in examples {
        send(
            &client,
            REMOTE_A,
            TraceSelection {
                expires_at,
                ..notice()
            },
            SOURCE,
        )
        .unwrap();
    }
    let submitted = transport.saved();
    assert_eq!(submitted.len(), 3);
    for (entry, (_, expected)) in submitted.iter().zip(examples) {
        assert_message(
            default_codec().as_ref(),
            entry.bytes.clone(),
            TRACE,
            SOURCE,
            expected,
        );
    }
}

/// 六类真实错误本身不携带终端/client/codec引用，fmt不增加传输或日志。
pub(super) fn test_client_error_display_reports_six_stages_and_causes() {
    let failures = failures();
    let observer = Observer::new(&Arc::new(AtomicUsize::new(0)));
    tracing::subscriber::with_default(observer.clone(), || {
        let mut texts = Vec::new();
        for failure in &failures {
            let before = failure.transport.saved().len();
            assert_stage(
                &failure.error,
                failure.stage,
                failure.expected_reason.as_deref(),
            );
            let text = failure.error.to_string();
            if let Some(reason) = &failure.expected_reason {
                assert!(text.contains(reason), "未显示真实原因：{text}");
            }
            assert!(!text.contains(&TraceId::from_bytes(TRACE).to_string()));
            assert!(!text.contains("operation_id"));
            texts.push(text);
            assert_eq!(failure.transport.saved().len(), before);
        }
        for left in 0..texts.len() {
            for right in (left + 1)..texts.len() {
                assert_ne!(texts[left], texts[right]);
            }
        }
    });
    assert!(observer.saved().is_empty());
}

/// 任意写片段首次失败后不得继续；无载荷fmt::Error以精确Err断言原样传播。
struct LimitedWriter {
    remaining: usize,
    failed: bool,
}
impl fmt::Write for LimitedWriter {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        assert!(!self.failed, "写失败后仍继续格式化");
        if value.len() > self.remaining {
            self.failed = true;
            Err(fmt::Error)
        } else {
            self.remaining -= value.len();
            Ok(())
        }
    }
}

/// 六类实际错误的写入边界故障及足够容量对照，不改变业务或提交。
pub(super) fn test_client_error_display_propagates_all_writer_failures() {
    let observer = Observer::new(&Arc::new(AtomicUsize::new(0)));
    tracing::subscriber::with_default(observer.clone(), || {
        for failure in failures() {
            let text = failure.error.to_string();
            let before = failure.transport.saved().len();
            for capacity in 0..text.len() {
                let mut writer = LimitedWriter {
                    remaining: capacity,
                    failed: false,
                };
                step("fmt_send_error");
                assert_eq!(
                    fmt::write(&mut writer, format_args!("{}", failure.error)),
                    Err(fmt::Error)
                );
                assert!(writer.failed);
            }
            let mut writer = LimitedWriter {
                remaining: text.len(),
                failed: false,
            };
            assert_eq!(
                fmt::write(&mut writer, format_args!("{}", failure.error)),
                Ok(())
            );
            assert!(!writer.failed);
            assert_eq!(failure.transport.saved().len(), before);
        }
    });
    assert!(observer.saved().is_empty());
}

/// 多次source返回同一个借用；类型和值证明直接原因，内部保存地址仍由实现审查补足。
pub(super) fn test_client_error_source_keeps_direct_typed_causes() {
    let failures = failures();
    let observer = Observer::new(&Arc::new(AtomicUsize::new(0)));
    tracing::subscriber::with_default(observer.clone(), || {
        for failure in failures {
            let before = failure.transport.saved().len();
            step("send_error_source");
            let first = failure.error.source();
            let second = failure.error.source();
            // 完整核验两次Option，不能让Some/None或None/Some绕过地址比较。
            match (failure.stage, first, second) {
                (Stage::Terminal, None, None) => {}
                (Stage::Terminal, _, _) => panic!("终端失效两次source都必须为None"),
                (_, Some(a), Some(b)) => assert!(std::ptr::eq(a, b), "直接原因借用地址改变"),
                _ => panic!("有原因阶段两次source都必须为Some"),
            }
            match failure.stage {
                Stage::Binding => assert!(matches!(
                    first.unwrap().downcast_ref::<BindingError>(),
                    Some(BindingError::Closed)
                )),
                Stage::Terminal => assert!(first.is_none()),
                Stage::Target => {
                    assert_parse_error(&failure.error, "0");
                }
                Stage::Protocol => assert!(matches!(
                    first.unwrap().downcast_ref::<ProtocolError>(),
                    Some(ProtocolError::InvalidTraceId)
                )),
                Stage::Encode | Stage::Submit => {
                    assert!(first.unwrap().is::<ServiceError>());
                    assert_eq!(first.unwrap().to_string(), failure.expected_reason.unwrap());
                    assert!(first.unwrap().source().is_none());
                }
            }
            assert_eq!(failure.transport.saved().len(), before);
        }
    });
    assert!(observer.saved().is_empty());
}

/// 六阶段通知失败均返回可下转型的同一发送错误，日志字段不替换错误类型/原因。
pub(super) fn test_client_notifier_failure_warns_once_and_keeps_error() {
    for stage in [
        Stage::Binding,
        Stage::Terminal,
        Stage::Target,
        Stage::Protocol,
        Stage::Encode,
        Stage::Submit,
    ] {
        let binding = CoordinationBinding::new();
        let transport = Transport::new(ServiceHost::Local(0), Acceptance::Runtime);
        let drops = Arc::new(AtomicUsize::new(0));
        let terminal = terminal(&transport, &drops);
        let codec = if matches!(stage, Stage::Encode) {
            Arc::new(DefaultCodec::default())
        } else {
            default_codec()
        };
        let client = client(&terminal, codec, &binding, LOCAL);
        if matches!(stage, Stage::Binding) {
            binding.close();
        }
        if matches!(stage, Stage::Terminal) {
            drop(terminal);
        }
        let remote = if matches!(stage, Stage::Target) {
            "0"
        } else {
            REMOTE_A
        };
        let selection = if matches!(stage, Stage::Protocol) {
            TraceSelection {
                trace_id: TraceId::INVALID,
                ..notice()
            }
        } else {
            notice()
        };
        let observer = Observer::new(&drops);
        step("notify");
        let wrapped = tracing::subscriber::with_default(observer.clone(), || {
            client.notify(&TraceNodeId::new(remote), selection)
        })
        .unwrap_err();
        let error = wrapped
            .downcast_ref::<CoordinationSendError>()
            .expect("不能字符串化替代发送错误");
        match stage {
            Stage::Binding => assert!(matches!(
                error.source().unwrap().downcast_ref::<BindingError>(),
                Some(BindingError::Closed)
            )),
            Stage::Terminal => assert!(error.source().is_none()),
            Stage::Target => {
                assert_parse_error(error, remote);
            }
            Stage::Protocol => assert!(matches!(
                error.source().unwrap().downcast_ref::<ProtocolError>(),
                Some(ProtocolError::InvalidTraceId)
            )),
            Stage::Encode => assert!(
                matches!(error.source().unwrap().downcast_ref::<ServiceError>(), Some(ServiceError::CodecError(reason)) if reason.contains("not found in type_id_index"))
            ),
            Stage::Submit => assert!(
                matches!(error.source().unwrap().downcast_ref::<ServiceError>(), Some(ServiceError::RuntimeError(reason)) if reason == "宿主拒收/保留原因")
            ),
        }
        let reason = error.source().map(ToString::to_string);
        assert_stage(error, stage, reason.as_deref());
        let events = observer.saved();
        assert_eq!(events.len(), 1);
        assert_warning(&events[0], remote, error);
        assert_eq!(
            transport.saved().len(),
            usize::from(matches!(stage, Stage::Submit))
        );
        assert_eq!(transport.other_calls.load(Ordering::SeqCst), 0);
    }
}

/// 空local不能在准入/终端/目标之前抢先失败；notify不能拥有独立的提前校验路径。
pub(super) fn test_client_notifier_empty_local_preserves_stage_precedence() {
    for stage in [Stage::Binding, Stage::Terminal, Stage::Target] {
        let binding = CoordinationBinding::new();
        let transport = Transport::new(ServiceHost::Local(0), Acceptance::Runtime);
        let drops = Arc::new(AtomicUsize::new(0));
        let terminal = terminal(&transport, &drops);
        let client = client(&terminal, default_codec(), &binding, "");
        if matches!(stage, Stage::Binding) {
            binding.close();
        }
        if matches!(stage, Stage::Terminal) {
            drop(terminal);
        }
        let observer = Observer::new(&drops);
        step("notify");
        let wrapped = tracing::subscriber::with_default(observer.clone(), || {
            client.notify(&TraceNodeId::new("0"), notice())
        })
        .unwrap_err();
        let error = wrapped.downcast_ref::<CoordinationSendError>().unwrap();
        let reason = error.source().map(ToString::to_string);
        assert_stage(error, stage, reason.as_deref());
        transport.assert_untouched();
        assert_eq!(observer.saved().len(), 1);
        assert_warning(&observer.saved()[0], "0", error);
    }
}

/// 过滤事件不应吞错、改变来源、重试或伪报成功；只保证尝试，记录权在宿主。
pub(super) fn test_client_notifier_filtered_warning_still_returns_typed_error() {
    let binding = CoordinationBinding::new();
    let transport = Transport::new(ServiceHost::Local(0), Acceptance::Runtime);
    let drops = Arc::new(AtomicUsize::new(0));
    let terminal = terminal(&transport, &drops);
    let client = client(&terminal, default_codec(), &binding, LOCAL);
    let mut observer = Observer::new(&drops);
    observer.enabled = false;
    step("notify");
    let wrapped = tracing::subscriber::with_default(observer.clone(), || {
        client.notify(&TraceNodeId::new(REMOTE_A), notice())
    })
    .unwrap_err();
    let error = wrapped.downcast_ref::<CoordinationSendError>().unwrap();
    assert!(
        matches!(error.source().unwrap().downcast_ref::<ServiceError>(), Some(ServiceError::RuntimeError(reason)) if reason == "宿主拒收/保留原因")
    );
    assert!(observer.saved().is_empty());
    let submitted = transport.saved();
    assert_eq!(submitted.len(), 1);
    assert_message(
        default_codec().as_ref(),
        submitted[0].bytes.clone(),
        TRACE,
        LOCAL,
        notice().expires_at,
    );
}

/// 外部notifier延长client/codec，不能保活binding Owner或终端；最后释放不声称驱逐TLS。
pub(super) fn test_client_notifier_holds_codec_but_not_terminal_or_binding() {
    let binding = CoordinationBinding::new();
    let transport = Transport::new(ServiceHost::Local(0), Acceptance::Ok);
    let drops = Arc::new(AtomicUsize::new(0));
    let terminal = terminal(&transport, &drops);
    let terminal_weak = Arc::downgrade(&terminal);
    let codec = default_codec();
    let codec_weak = Arc::downgrade(&codec);
    let client = Arc::new(client(&terminal, codec, &binding, LOCAL));
    let notifier: Arc<dyn TraceNotifier> = client.clone();
    drop(client);
    assert!(codec_weak.upgrade().is_some());
    drop(binding);
    drop(terminal);
    assert!(terminal_weak.upgrade().is_none());
    let observer = Observer::new(&drops);
    step("notify");
    let wrapped = tracing::subscriber::with_default(observer.clone(), || {
        notifier.notify(&TraceNodeId::new(REMOTE_A), notice())
    })
    .unwrap_err();
    let error = wrapped.downcast_ref::<CoordinationSendError>().unwrap();
    assert!(matches!(
        error.source().unwrap().downcast_ref::<BindingError>(),
        Some(BindingError::Closed)
    ));
    transport.assert_untouched();
    drop(notifier);
    assert!(codec_weak.upgrade().is_none());
    // 错误及日志仍存活，但没有延长资源持有。
    assert_eq!(observer.saved().len(), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// 每个可触达阶段结果保留时宿主终端仍能析构；成功和错误返回均不遗留临时强持有。
pub(super) fn test_client_all_returns_release_temporary_terminal() {
    for stage in [
        Stage::Binding,
        Stage::Terminal,
        Stage::Target,
        Stage::Protocol,
        Stage::Encode,
        Stage::Submit,
    ] {
        let binding = CoordinationBinding::new();
        let transport = Transport::new(ServiceHost::Local(0), Acceptance::Runtime);
        let drops = Arc::new(AtomicUsize::new(0));
        let terminal = terminal(&transport, &drops);
        let weak = Arc::downgrade(&terminal);
        let codec = if matches!(stage, Stage::Encode) {
            Arc::new(DefaultCodec::default())
        } else {
            default_codec()
        };
        let client = client(&terminal, codec, &binding, LOCAL);
        if matches!(stage, Stage::Binding) {
            binding.close();
        }
        // 本场景保留宿主强引用；终端失效由专门场景证明，不用字段或布局伪造Weak。
        let error = if matches!(stage, Stage::Terminal) {
            drop(terminal);
            send(&client, REMOTE_A, notice(), SOURCE).unwrap_err()
        } else {
            let remote = if matches!(stage, Stage::Target) {
                "0"
            } else {
                REMOTE_A
            };
            let selection = if matches!(stage, Stage::Protocol) {
                TraceSelection {
                    trace_id: TraceId::INVALID,
                    ..notice()
                }
            } else {
                notice()
            };
            let error = send(&client, remote, selection, SOURCE).unwrap_err();
            assert_eq!(Arc::strong_count(&terminal), 1, "结果返回后只应剩宿主持有");
            drop(terminal);
            error
        };
        assert!(weak.upgrade().is_none());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let reason = error.source().map(ToString::to_string);
        assert_stage(&error, stage, reason.as_deref());
    }
    let binding = CoordinationBinding::new();
    let transport = Transport::new(ServiceHost::Local(0), Acceptance::Ok);
    let drops = Arc::new(AtomicUsize::new(0));
    let terminal = terminal(&transport, &drops);
    let weak = Arc::downgrade(&terminal);
    let client = client(&terminal, default_codec(), &binding, LOCAL);
    send(&client, REMOTE_A, notice(), SOURCE).unwrap();
    assert_eq!(Arc::strong_count(&terminal), 1);
    drop(terminal);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// 在真实Deref边界释放宿主最后常驻引用；临时Arc必须保活实际send，退出后先析构再日志。
pub(super) fn test_client_notifier_releases_last_terminal_before_warning() {
    isolated(|| {
        let binding = CoordinationBinding::new();
        let transport = Transport::new(ServiceHost::Local(0), Acceptance::Runtime);
        let drops = Arc::new(AtomicUsize::new(0));
        let terminal = terminal(&transport, &drops);
        let weak = Arc::downgrade(&terminal);
        let client = client(&terminal, default_codec(), &binding, LOCAL);
        let holder = Arc::new(Mutex::new(Some(terminal.clone())));
        let holder_weak = Arc::downgrade(&holder);
        *terminal.on_deref.lock().unwrap() = Some(Box::new(move || {
            let holder = holder_weak.upgrade().unwrap();
            drop(holder.lock().unwrap().take());
        }));
        drop(terminal);
        let observer = Observer::new(&drops);
        step("notify");
        let wrapped = tracing::subscriber::with_default(observer.clone(), || {
            client.notify(&TraceNodeId::new(REMOTE_A), notice())
        })
        .unwrap_err();
        let error = wrapped.downcast_ref::<CoordinationSendError>().unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(weak.upgrade().is_none());
        assert!(holder.lock().unwrap().is_none());
        let events = observer.saved();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].terminal_drops, 1);
        assert_warning(&events[0], REMOTE_A, error);
        assert_eq!(transport.saved().len(), 1);
    });
}

/// 真实发送同步暂停于已准入后的终端出口，close须先返回；本次仍提交一次，后续拒绝。
pub(super) fn test_client_admitted_send_finishes_after_close() {
    isolated(|| {
        let binding = CoordinationBinding::new();
        let transport = Transport::new(ServiceHost::Local(0), Acceptance::Ok);
        let drops = Arc::new(AtomicUsize::new(0));
        let terminal = terminal(&transport, &drops);
        let client = Arc::new(client(&terminal, default_codec(), &binding, LOCAL));
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        *terminal.on_deref.lock().unwrap() = Some(Box::new(move || {
            entered_tx.send(()).unwrap();
            release_rx
                .recv_timeout(WAIT)
                .expect("宿主暂停窗口必须有截止");
        }));
        let sending = client.clone();
        let worker = thread::spawn(move || send(&sending, REMOTE_A, notice(), SOURCE));
        entered_rx
            .recv_timeout(WAIT)
            .expect("未到达已准入发送窗口；new/TODO不是竞争证据");
        let (closed_tx, closed_rx) = mpsc::sync_channel(1);
        let closer = thread::spawn(move || {
            binding.close();
            closed_tx.send(()).unwrap();
            binding
        });
        closed_rx
            .recv_timeout(WAIT)
            .expect("close不应等待已准入发送或持有其锁");
        assert!(transport.saved().is_empty());
        release_tx.send(()).unwrap();
        worker.join().unwrap().unwrap();
        let binding = closer.join().unwrap();
        assert_eq!(transport.saved().len(), 1);
        let error = send(&client, REMOTE_B, notice(), SOURCE).unwrap_err();
        assert!(matches!(
            error.source().unwrap().downcast_ref::<BindingError>(),
            Some(BindingError::Closed)
        ));
        assert_eq!(transport.saved().len(), 1);
        drop(binding);
    });
}

/// 宿主Deref panic不转普通Err；展开后终端临时强持有与状态锁不遗留。
pub(super) fn test_client_dependency_panic_unwinds_without_retaining_terminal() {
    isolated(|| {
        let binding = CoordinationBinding::new();
        let transport = Transport::new(ServiceHost::Local(0), Acceptance::Ok);
        let drops = Arc::new(AtomicUsize::new(0));
        let terminal = terminal(&transport, &drops);
        let weak = Arc::downgrade(&terminal);
        let client = client(&terminal, default_codec(), &binding, LOCAL);
        *terminal.on_deref.lock().unwrap() = Some(Box::new(|| panic!("宿主Deref测试panic")));
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            send(&client, REMOTE_A, notice(), SOURCE)
        }));
        let panic = outcome.expect_err("依赖panic不能变成普通Err");
        let text = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("");
        assert_eq!(text, "宿主Deref测试panic", "TODO/new panic不是目标展开证据");
        assert_eq!(Arc::strong_count(&terminal), 1);
        binding.close();
        drop(terminal);
        assert!(weak.upgrade().is_none());
        assert!(transport.saved().is_empty());
    });
}

/// 日志panic由宿主负责；测试只在边界捕获以观察已提交及释放，不要求notify继续包装。
pub(super) fn test_client_subscriber_panic_is_not_converted_or_retried() {
    isolated(|| {
        let binding = CoordinationBinding::new();
        let transport = Transport::new(ServiceHost::Local(0), Acceptance::Runtime);
        let drops = Arc::new(AtomicUsize::new(0));
        let terminal = terminal(&transport, &drops);
        let client = client(&terminal, default_codec(), &binding, LOCAL);
        let mut observer = Observer::new(&drops);
        observer.panic_on_event = true;
        step("notify");
        let panic = catch_unwind(AssertUnwindSafe(|| {
            tracing::subscriber::with_default(observer.clone(), || {
                client.notify(&TraceNodeId::new(REMOTE_A), notice())
            })
        }))
        .expect_err("subscriber panic不能变普通Err");
        let text = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("");
        assert_eq!(text, "宿主subscriber测试panic", "前置TODO不是日志panic证据");
        assert_eq!(observer.saved().len(), 1);
        assert_eq!(transport.saved().len(), 1);
        assert_eq!(Arc::strong_count(&terminal), 1);
        assert_eq!(transport.other_calls.load(Ordering::SeqCst), 0);
    });
}

/// 最后引用析构panic可发生在成功提交之后；本库不回滚外部效果，不伪装普通结果。
pub(super) fn test_client_last_terminal_drop_panic_keeps_submitted_effect() {
    isolated(|| {
        let binding = CoordinationBinding::new();
        let transport = Transport::new(ServiceHost::Local(0), Acceptance::Ok);
        let drops = Arc::new(AtomicUsize::new(0));
        let terminal = terminal(&transport, &drops);
        let weak = Arc::downgrade(&terminal);
        let client = client(&terminal, default_codec(), &binding, LOCAL);
        let holder = Arc::new(Mutex::new(Some(terminal.clone())));
        let holder_weak = Arc::downgrade(&holder);
        let terminal_weak = weak.clone();
        *terminal.on_deref.lock().unwrap() = Some(Box::new(move || {
            // 只有实际到达Deref才启用故障；send TODO不会引发清理时二次panic。
            terminal_weak
                .upgrade()
                .unwrap()
                .panic_on_drop
                .store(true, Ordering::SeqCst);
            let holder = holder_weak.upgrade().unwrap();
            drop(holder.lock().unwrap().take());
        }));
        drop(terminal);
        let panic = catch_unwind(AssertUnwindSafe(|| {
            send(&client, REMOTE_A, notice(), SOURCE)
        }))
        .expect_err("最后引用析构panic不能变成Ok/Err");
        let text = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("");
        assert_eq!(text, "宿主终端最后引用析构panic", "前置TODO不是析构证据");
        assert!(weak.upgrade().is_none());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(transport.saved().len(), 1);
        assert_eq!(transport.other_calls.load(Ordering::SeqCst), 0);
    });
}
