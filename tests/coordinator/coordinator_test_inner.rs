//! slice-coordinator：真正的crate外集成测试，只消费根公开面与真实宿主依赖。
//! @api:src/coordinator.rs @impl:src/coordinator_inner.rs
//! @test:tests/coordinator_test.rs
//! @design-doc:design/tracing-coordinator/07-design/call-chain.md#slice-coordinator
//! 私有context、binding、消息及Kind不可导入。宿主传输只观测字节，手动提交通道只拥有并poll生产Future。
//! 每场景独立进程及硬截止；panic探针防止静态框架吞掉TODO后假绿。内部精确调用/锁外释放须独立审查。

use fory::{Fory, Reader};
use opentelemetry_sdk::{
    error::OTelSdkResult,
    trace::{SpanData, SpanExporter},
    Resource,
};
use pi_logger::{
    FlightRecorder, FlightRecorderGuard, TraceId, TraceNodeId, TraceNotifier, TraceSelection,
};
use pi_service::{
    DefaultCodec, MultiLayerCodec, Service, ServiceAccessMode, ServiceAddress, ServiceCodec,
    ServiceContexts, ServiceError, ServiceHost, ServiceId, ServicePort, ServiceRuntime,
    ServiceStatus, ServiceStatusEvent, ServiceStatusListener, ServiceTerminal, ServiceTransport,
    ServiceVersion, TaskSubmitter,
};
use pi_tracing_coordinator::{
    register_fory_types, CoordinationError, CoordinationSendError, TracingCoordinator,
    COORDINATION_SERVICE_LAYER, COORDINATION_SERVICE_PORT,
};
use std::{
    any::Any,
    collections::{BTreeMap, VecDeque},
    error::Error,
    fmt,
    future::Future,
    io::Read,
    marker::PhantomData,
    ops::Deref,
    panic::catch_unwind,
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, Mutex,
    },
    task::{Context, Poll, Waker},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tracing::{
    field::{Field, Visit},
    span::{Attributes, Id, Record},
    Event, Level, Metadata, Subscriber,
};

const LOCAL: &str = "宿主/节点-A";
const SOURCE: &str = "  本跳é/e\u{301}🦀\0 ";
const REMOTE: &str = "QmYwAPJzv5CZsnAzt8auVZRnGi2CkEJjQd2B5Hc9EcAm7H";
const SELF: &str = "QmYwAPJzv5CZsnAzt8auVZRnGi2CkEJjQd2B5Hc9EcAm7J";
const TRACE: [u8; 16] = [
    1, 35, 69, 103, 137, 171, 205, 239, 254, 220, 186, 152, 118, 84, 50, 16,
];
const WAIT: Duration = Duration::from_secs(3);
const HOST_PORT: u16 = 4090;
const LAYER: &str = "tracing-coordinator";
type Task = futures::future::BoxFuture<'static, ()>;
type Coordinator<C> = TracingCoordinator<Host<C>, C>;

/// 每场景只启动自身子进程，隔离全局logger、inventory适配和panic hook；超时不是目标RED。
fn isolated(action: impl FnOnce(&PanicProbe)) {
    const ENV: &str = "PI_SLICE_COORDINATOR_CHILD";
    let name = thread::current().name().expect("具名集成测试").to_owned();
    if std::env::var(ENV).ok().as_deref() == Some(name.as_str()) {
        let panics = PanicProbe::install();
        action(&panics);
        panics.assert_clean();
        return;
    }
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &name, "--nocapture", "--test-threads=1"])
        .env(ENV, &name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("夹具：启动独立场景");
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let out = thread::spawn(move || {
        let mut text = String::new();
        stdout.read_to_string(&mut text).unwrap();
        text
    });
    let err = thread::spawn(move || {
        let mut text = String::new();
        stderr.read_to_string(&mut text).unwrap();
        text
    });
    let start = Instant::now();
    let mut timed_out = false;
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if start.elapsed() > Duration::from_secs(25) {
            timed_out = true;
            child.kill().unwrap();
            break child.wait().unwrap();
        }
        thread::sleep(Duration::from_millis(5));
    };
    let output = out.join().unwrap();
    let errors = err.join().unwrap();
    assert!(
        output.contains("running 1 test"),
        "零命中不能作证据：{output}\n{errors}"
    );
    assert!(!timed_out, "环境/场景超时，非RED：{output}\n{errors}");
    assert!(
        status.success(),
        "按COORDINATOR_STEP及具体TODO定位：{status}\n{output}\n{errors}"
    );
    assert!(
        output.contains("1 passed; 0 failed; 0 ignored"),
        "未完成场景：{output}\n{errors}"
    );
}

/// 只观测，不改变框架catch_unwind；任意线程意外panic均保留到场景出口。
struct PanicProbe(Arc<Mutex<Vec<String>>>);
impl PanicProbe {
    fn install() -> Self {
        let saved = Arc::new(Mutex::new(Vec::new()));
        let captured = saved.clone();
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let message = info
                .payload()
                .downcast_ref::<&str>()
                .map(|v| (*v).to_owned())
                .or_else(|| info.payload().downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "非字符串panic".to_owned());
            captured
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(message);
            previous(info);
        }));
        Self(saved)
    }
    fn assert_clean(&self) {
        assert!(
            self.0.lock().unwrap().is_empty(),
            "意外panic不能因框架吞掉而通过"
        );
    }
    fn calibration(&self) {
        let effects = AtomicUsize::new(0);
        assert!(catch_unwind(|| {
            effects.fetch_add(1, Ordering::SeqCst);
            panic!("COORDINATOR_PANIC_CONTROL");
        })
        .is_err());
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        assert_eq!(
            std::mem::take(&mut *self.0.lock().unwrap()),
            ["COORDINATOR_PANIC_CONTROL"]
        );
    }
}
fn step(value: &str) {
    eprintln!("COORDINATOR_STEP={value}");
}
fn port() -> ServicePort {
    COORDINATION_SERVICE_PORT.into()
}
fn host(value: &str) -> ServiceHost {
    ServiceHost::Remote(value.parse().expect("固定有效PeerId"))
}

/// 仅poll同步静态服务/宿主已接受任务；Pending不伪装完成，不引入自制业务状态推进。
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("此最小真实同步链未完成，不把Pending当成功"),
    }
}
fn wait_for(mut predicate: impl FnMut() -> bool, label: &str) {
    let start = Instant::now();
    while !predicate() {
        assert!(start.elapsed() < WAIT, "有界等待失败：{label}");
        thread::sleep(Duration::from_millis(2));
    }
}
fn registered() -> Arc<Fory> {
    assert!(
        !fory_core::error::should_panic_on_error(),
        "环境：普通Err矩阵要求实际Fory构建开关false"
    );
    let mut table = Fory::default();
    register_fory_types(&mut table).expect("第2块根公开集中登记");
    Arc::new(table)
}
fn notice(tag: u8) -> TraceSelection {
    TraceSelection {
        trace_id: TraceId::from_bytes([tag; 16]),
        expires_at: future_deadline(),
    }
}
/// 真实记录器依赖当前时间；使用一次采样的整秒后30秒，期望直接保存，断言不重新取now。
fn future_deadline() -> SystemTime {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    UNIX_EPOCH + Duration::from_secs(seconds + 30)
}

/// 独立宿主对端表示，真实Fory登记4300；不导入生产私有消息或复制其转换实现。
#[derive(fory::ForyObject)]
struct PeerMessage {
    trace_id: [u8; 16],
    expires_at_100ns: i64,
    source: Vec<u8>,
}
fn peer_codec() -> DefaultCodec {
    let mut table = Fory::default();
    table.register::<PeerMessage>(4300).unwrap();
    DefaultCodec::new(Arc::new(table))
}
fn peer_bytes(trace: [u8; 16], source: &str, ticks: i64) -> Vec<u8> {
    peer_codec()
        .default_encode(&PeerMessage {
            trace_id: trace,
            expires_at_100ns: ticks,
            source: source.as_bytes().to_vec(),
        })
        .unwrap()
}
/// 下列输入均为整秒；无需重写生产100ns舍入算法，边界矩阵仍归第1块。
fn ticks(value: SystemTime) -> i64 {
    i64::try_from(value.duration_since(UNIX_EPOCH).unwrap().as_secs()).unwrap() * 10_000_000
}
fn received_bytes(selection: TraceSelection, source: &str) -> Vec<u8> {
    peer_bytes(
        selection.trace_id.to_bytes(),
        source,
        ticks(selection.expires_at),
    )
}
fn assert_packet(bytes: &[u8], selection: TraceSelection, source: &str) {
    let expected = received_bytes(selection, source);
    assert_eq!(bytes, expected, "独立对端编码与实际生产字节应一致");
    let decoded: PeerMessage = peer_codec().default_decode(bytes.to_vec()).unwrap();
    assert_eq!(decoded.trace_id, selection.trace_id.to_bytes());
    assert_eq!(decoded.source, source.as_bytes());
    assert_eq!(decoded.expires_at_100ns, ticks(selection.expires_at));
}

/// 宿主任务接管器：只保存生产Future，调用返回后才由宿主显式poll；没有同步等待提交完成。
#[derive(Default)]
struct Tasks {
    queue: Mutex<VecDeque<Task>>,
    accepted: AtomicUsize,
}
impl Tasks {
    fn submitter(self: &Arc<Self>) -> TaskSubmitter {
        let tasks = self.clone();
        TaskSubmitter::from_fn(move |task| {
            tasks.queue.lock().unwrap().push_back(task);
            tasks.accepted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
    fn drain(&self) {
        loop {
            let task = self.queue.lock().unwrap().pop_front();
            match task {
                Some(task) => ready(task),
                None => break,
            }
        }
    }
}
#[derive(Clone)]
struct Submission {
    address: ServiceAddress,
    bytes: Vec<u8>,
}
/// 传输边界只观测真实ServiceTerminal路由的结果；不替代消息转换、准入、codec或接收。
struct Wire {
    local: ServiceHost,
    saved: Mutex<Vec<Submission>>,
    failure: Mutex<Option<ServiceError>>,
    other_calls: AtomicUsize,
}
impl Wire {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            local: host(SELF),
            saved: Mutex::new(Vec::new()),
            failure: Mutex::new(None),
            other_calls: AtomicUsize::new(0),
        })
    }
    fn packets(&self) -> Vec<Submission> {
        self.saved.lock().unwrap().clone()
    }
}
impl<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>> ServiceTransport<C> for Wire {
    fn local_host(&self) -> ServiceHost {
        self.local.clone()
    }
    fn send(&self, address: ServiceAddress, input: Vec<u8>) -> Result<(), ServiceError> {
        self.saved.lock().unwrap().push(Submission {
            address,
            bytes: input,
        });
        match &*self.failure.lock().unwrap() {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }
    fn rpc(
        &self,
        _address: ServiceAddress,
        _input: Vec<u8>,
        _timeout: Option<Duration>,
    ) -> futures::future::BoxFuture<'static, Result<Vec<u8>, ServiceError>> {
        self.other_calls.fetch_add(1, Ordering::SeqCst);
        panic!("不允许RPC");
    }
    fn reconnect(
        &self,
        _target: ServiceHost,
    ) -> futures::future::BoxFuture<'static, Result<(), ServiceError>> {
        self.other_calls.fetch_add(1, Ordering::SeqCst);
        panic!("不允许重连");
    }
    fn close_connection(
        &self,
        _target: ServiceHost,
    ) -> futures::future::BoxFuture<'static, Result<(), ServiceError>> {
        self.other_calls.fetch_add(1, Ordering::SeqCst);
        panic!("不能关闭共享设施");
    }
    fn is_connected(&self, _target: ServiceHost) -> bool {
        self.other_calls.fetch_add(1, Ordering::SeqCst);
        panic!("不允许预探测");
    }
}
/// 合法T包装，Deref始终返回与runtime匹配的同一终端；可选暂停只观测宿主出口。
struct Host<C: ServiceCodec> {
    terminal: ServiceTerminal<C>,
    on_deref: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    drops: Arc<AtomicUsize>,
}
impl<C: ServiceCodec> Deref for Host<C> {
    type Target = ServiceTerminal<C>;
    fn deref(&self) -> &Self::Target {
        let action = self.on_deref.lock().unwrap().take();
        if let Some(action) = action {
            action();
        }
        &self.terminal
    }
}
impl<C: ServiceCodec> Drop for Host<C> {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
/// 监听器仅保存事实，不重入注册、初始化或关闭；其同步调用时点也用于验证context先发布。
#[derive(Default)]
struct Events(Mutex<Vec<(ServiceStatusEvent, thread::ThreadId)>>);
impl ServiceStatusListener for Events {
    fn on_status_event(&self, event: ServiceStatusEvent) {
        self.0.lock().unwrap().push((event, thread::current().id()));
    }
}
struct Rig<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>> {
    runtime: ServiceRuntime<C>,
    terminal: Arc<Host<C>>,
    wire: Arc<Wire>,
    events: Arc<Events>,
    tasks: Arc<Tasks>,
}
impl<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>> Rig<C> {
    fn new(codec: C) -> Self {
        let wire = Wire::new();
        let events = Arc::new(Events::default());
        let tasks = Arc::new(Tasks::default());
        let runtime = ServiceRuntime::new(
            ServiceContexts::new(),
            codec,
            Some(tasks.submitter()),
            Default::default(),
            host(SELF),
            Some(events.clone()),
        );
        let terminal = Arc::new(Host {
            terminal: runtime.terminal(wire.clone()),
            on_deref: Mutex::new(None),
            drops: Arc::new(AtomicUsize::new(0)),
        });
        Self {
            runtime,
            terminal,
            wire,
            events,
            tasks,
        }
    }
    fn deliver(&self, value: TraceSelection, source: &str) {
        ready(
            self.runtime
                .on_send(port(), received_bytes(value, source), None),
        )
        .expect("真实框架仅证明按send规则收尾");
    }
}
fn default_rig() -> Rig<DefaultCodec> {
    let table = registered();
    let codec = DefaultCodec::new(table.clone());
    assert!(Arc::ptr_eq(&*codec.default_codec(), &table));
    Rig::new(codec)
}
fn multi_rig() -> Rig<MultiLayerCodec> {
    let table = registered();
    let codec = MultiLayerCodec::new(Arc::new(Fory::default()));
    codec.register_layer_fory(LAYER, table.clone());
    assert!(codec.has_layer(LAYER));
    assert!(Arc::ptr_eq(&*codec.layer_fory(LAYER).unwrap(), &table));
    Rig::new(codec)
}
fn default_coordinator(rig: &Rig<DefaultCodec>, local: &str) -> Coordinator<DefaultCodec> {
    step("new_default：前置或本场景构造目标");
    TracingCoordinator::new_default(&rig.terminal, &rig.runtime, TraceNodeId::new(local))
        .expect("公开默认构造")
}
fn multi_coordinator(rig: &Rig<MultiLayerCodec>) -> Coordinator<MultiLayerCodec> {
    step("new_multi_layer：前置或本场景构造目标");
    TracingCoordinator::new_multi_layer(&rig.terminal, &rig.runtime, TraceNodeId::new(LOCAL))
        .expect("公开明确层构造")
}

/// 这是独立宿主服务，只证明共享设施继续工作或真实4091冲突；不承担协调器功能。
struct HostCalls(AtomicUsize);
struct HostService<C, const PORT: u16>(PhantomData<fn() -> C>);
impl<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>, const PORT: u16> Service
    for HostService<C, PORT>
{
    type Codec = C;
    fn id(&self) -> ServiceId {
        format!("svc://local/slice-coordinator/host-{PORT}").into()
    }
    fn port(&self) -> ServicePort {
        PORT.into()
    }
    fn layer(&self) -> &'static str {
        "slice-coordinator-host"
    }
    fn mode(&self) -> ServiceAccessMode {
        ServiceAccessMode::Send
    }
    fn version(&self) -> ServiceVersion {
        "0.1.0".into()
    }
    fn bind_params(
        &self,
        contexts: &ServiceContexts,
    ) -> Result<Arc<dyn Any + Send + Sync>, ServiceError> {
        contexts
            .get_only_read::<HostCalls>()
            .map(|calls| calls as Arc<dyn Any + Send + Sync>)
            .ok_or_else(|| {
                ServiceError::BindOnlyReadNotFound(PORT, "calls".into(), "HostCalls".into())
            })
    }
    fn init(
        &self,
        _runtime: &ServiceRuntime<C>,
        _params: Arc<dyn Any + Send + Sync>,
    ) -> Result<(), ServiceError> {
        Ok(())
    }
    fn handle_send(
        &self,
        _runtime: ServiceRuntime<C>,
        params: Arc<dyn Any + Send + Sync>,
        _input: Vec<u8>,
    ) -> futures::future::BoxFuture<'static, Result<(), ServiceError>> {
        Box::pin(async move {
            params
                .downcast::<HostCalls>()
                .map_err(|_| ServiceError::DowncastParamsFailed(PORT))?
                .0
                .fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
    fn handle_rpc(
        &self,
        _runtime: ServiceRuntime<C>,
        _params: Arc<dyn Any + Send + Sync>,
        _input: Vec<u8>,
    ) -> futures::future::BoxFuture<'static, Result<Vec<u8>, ServiceError>> {
        Box::pin(async { panic!("宿主观察服务只有send") })
    }
}
static HOST_DEFAULT: HostService<DefaultCodec, HOST_PORT> = HostService(PhantomData);
static HOST_MULTI: HostService<MultiLayerCodec, HOST_PORT> = HostService(PhantomData);
static OCCUPANT_DEFAULT: HostService<DefaultCodec, 4091> = HostService(PhantomData);
static OCCUPANT_MULTI: HostService<MultiLayerCodec, 4091> = HostService(PhantomData);

fn install_host<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    rig: &Rig<C>,
    service: &'static dyn Service<Codec = C>,
) -> Arc<HostCalls> {
    let calls = Arc::new(HostCalls(AtomicUsize::new(0)));
    rig.runtime.contexts().insert_only_read(calls.clone());
    rig.runtime.registry().register(service).unwrap();
    rig.runtime
        .registry()
        .init(&rig.runtime, service.port())
        .unwrap();
    calls
}
fn assert_host<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    rig: &Rig<C>,
    calls: &HostCalls,
    expected: usize,
) {
    assert!(rig.tasks.queue.lock().unwrap().is_empty());
    rig.terminal
        .terminal
        .send(
            ServiceAddress::new(host(SELF), HOST_PORT.into()),
            vec![9, 8, 7],
            None,
        )
        .unwrap();
    assert_eq!(
        calls.0.load(Ordering::SeqCst),
        expected - 1,
        "同步返回仅表示接管，不是业务完成"
    );
    assert_eq!(rig.tasks.queue.lock().unwrap().len(), 1);
    rig.tasks.drain();
    assert_eq!(calls.0.load(Ordering::SeqCst), expected);
    assert!(matches!(
        rig.runtime.get_status(HOST_PORT.into()),
        Some(ServiceStatus::Running)
    ));
    assert_eq!(rig.wire.other_calls.load(Ordering::SeqCst), 0);
}
fn assert_meta<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(rig: &Rig<C>) {
    assert_eq!(COORDINATION_SERVICE_PORT, 4091);
    assert_eq!(COORDINATION_SERVICE_LAYER, LAYER);
    let meta = rig.runtime.get_meta(port()).expect("实际安装元信息");
    assert_eq!(
        meta.id.to_string(),
        "svc://local/observability/tracing/coordinator"
    );
    assert_eq!(u16::from(meta.port), 4091);
    assert_eq!(meta.layer, LAYER);
    assert_eq!(meta.mode, ServiceAccessMode::Send);
    assert_eq!(meta.version, ServiceVersion::from("0.1.0"));
}

#[derive(Clone, Debug)]
struct Warning {
    target: String,
    level: Level,
    fields: BTreeMap<String, String>,
}
struct Fields(BTreeMap<String, String>);
impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().into(), value.to_string());
    }
}
/// 纳入本库根及所有子target，不能通过窄过滤隐藏意外额外日志。
#[derive(Clone, Default)]
struct Observer(Arc<Mutex<Vec<Warning>>>);
impl Subscriber for Observer {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target() == "pi_tracing_coordinator"
            || metadata.target().starts_with("pi_tracing_coordinator::")
    }
    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _span: &Id, _values: &Record<'_>) {}
    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}
    fn enter(&self, _span: &Id) {}
    fn exit(&self, _span: &Id) {}
    fn event(&self, event: &Event<'_>) {
        if self.enabled(event.metadata()) {
            let mut fields = Fields(BTreeMap::new());
            event.record(&mut fields);
            self.0.lock().unwrap().push(Warning {
                target: event.metadata().target().into(),
                level: *event.metadata().level(),
                fields: fields.0,
            });
        }
    }
}
fn assert_service_warning(saved: &[Warning], reason: &str) {
    assert_eq!(saved.len(), 1, "只有一次本库拒绝日志");
    let event = &saved[0];
    assert_eq!(event.target, "pi_tracing_coordinator::service");
    assert_eq!(event.level, Level::WARN);
    assert_eq!(
        event.fields.get("service_port").map(String::as_str),
        Some("4091")
    );
    assert_eq!(
        event.fields.get("stage").map(String::as_str),
        Some("submit")
    );
    assert_eq!(event.fields.get("error").map(String::as_str), Some(reason));
}
fn assert_receive_rejected<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    rig: &Rig<C>,
    reason: &str,
) {
    let observer = Observer::default();
    tracing::subscriber::with_default(observer.clone(), || rig.deliver(notice(0x31), "远端/来源"));
    assert_service_warning(&observer.0.lock().unwrap(), reason);
}

/// 只保存真实委托返回后的观测事实，不持有CoordinationError/终端/runtime，也不改写原始返回值。
#[derive(Debug)]
struct CallbackFailure {
    text: String,
    is_send_error: bool,
    cause: Option<String>,
}
#[derive(Debug)]
struct CallbackReturn {
    remote: TraceNodeId,
    selection: TraceSelection,
    outcome: Result<(), CallbackFailure>,
    thread: thread::ThreadId,
}
/// 透明回调观测器：delegate恰调用一次，返回后才发带remote/Trace/期限的报告，再原样返回同一Result。
/// 容量4的观测通道使用try_send不阻塞回调，接收3秒截止；满/断连使夹具失败，不改写生产结果。
struct ObservedNotifier {
    delegate: Arc<dyn TraceNotifier>,
    completed: mpsc::SyncSender<CallbackReturn>,
}
impl TraceNotifier for ObservedNotifier {
    fn notify(&self, remote: &TraceNodeId, selection: TraceSelection) -> anyhow::Result<()> {
        let result = self.delegate.notify(remote, selection);
        let outcome = match &result {
            Ok(()) => Ok(()),
            Err(error) => Err(CallbackFailure {
                text: error.to_string(),
                is_send_error: error.downcast_ref::<CoordinationSendError>().is_some(),
                cause: error
                    .downcast_ref::<CoordinationSendError>()
                    .and_then(Error::source)
                    .map(ToString::to_string),
            }),
        };
        self.completed
            .try_send(CallbackReturn {
                remote: remote.clone(),
                selection,
                outcome,
                thread: thread::current().id(),
            })
            .expect("回调观测接收器必须活到目标回调返回");
        result
    }
}
fn observe_notifier(
    delegate: Arc<dyn TraceNotifier>,
) -> (Arc<dyn TraceNotifier>, mpsc::Receiver<CallbackReturn>) {
    let (completed, receiver) = mpsc::sync_channel(4);
    (
        Arc::new(ObservedNotifier {
            delegate,
            completed,
        }),
        receiver,
    )
}
/// 每个返回须同时匹配身份/Trace/期限及成功失败；累计开始数/失败快照不能替代此判定。
fn callback_return_matches(
    report: &CallbackReturn,
    remote: &TraceNodeId,
    selection: TraceSelection,
    success: bool,
) -> bool {
    report.remote == *remote
        && report.selection.trace_id == selection.trace_id
        && report.selection.expires_at == selection.expires_at
        && report.outcome.is_ok() == success
}
fn expect_callback_return(
    receiver: &mpsc::Receiver<CallbackReturn>,
    selection: TraceSelection,
    success: bool,
) -> CallbackReturn {
    let report = receiver
        .recv_timeout(WAIT)
        .expect("3秒内须取得该次真实委托的返回结果，开始通知不算完成");
    assert!(
        callback_return_matches(&report, &TraceNodeId::new(REMOTE), selection, success),
        "回调结果/身份/Trace/期限不符：{report:?}"
    );
    assert_ne!(
        report.thread,
        thread::current().id(),
        "必须由真实通知线程调用"
    );
    report
}
/// 仅用于观测器校准的受控边界；不用于coordinator目标场景，不替代生产notifier。
struct ControlledNotifier {
    entered: mpsc::Sender<TraceSelection>,
    release: Mutex<mpsc::Receiver<bool>>,
}
impl TraceNotifier for ControlledNotifier {
    fn notify(&self, remote: &TraceNodeId, selection: TraceSelection) -> anyhow::Result<()> {
        assert_eq!(remote.as_str(), REMOTE);
        self.entered.send(selection).unwrap();
        if self
            .release
            .lock()
            .unwrap()
            .recv_timeout(WAIT)
            .expect("受控回调必须有界释放")
        {
            Ok(())
        } else {
            Err(anyhow::anyhow!("R2受控真实回调返回失败"))
        }
    }
}
/// 真记录器通知线程的正/负对照：开始但未返回时无报告，且两次最终均Err绝不能误判成首Ok。
fn check_callback_return_controls() {
    let (entered, started) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    let (notifier, returned) = observe_notifier(Arc::new(ControlledNotifier {
        entered,
        release: Mutex::new(blocked),
    }));
    let mut recorder = Recorder::with_notifier("R2真实通知线程校准", notifier);
    let mut reports = Vec::new();
    for (index, success) in [true, false, false, false].into_iter().enumerate() {
        let value = notice(0x81 + u8::try_from(index).unwrap());
        assert!(recorder
            .recorder
            .record_peer(value.trace_id, TraceNodeId::new(REMOTE)));
        assert!(recorder
            .recorder
            .apply_remote_selection(value, TraceNodeId::new("上游")));
        recorder.recorder.seal_current_thread();
        let actual = started
            .recv_timeout(WAIT)
            .expect("真实线程必须进入校准回调");
        assert_eq!(actual.trace_id, value.trace_id);
        assert_eq!(actual.expires_at, value.expires_at);
        assert_eq!(
            recorder.recorder.stats().notification_attempts,
            u64::try_from(index + 1).unwrap()
        );
        assert!(
            matches!(returned.try_recv(), Err(mpsc::TryRecvError::Empty)),
            "开始计数已增加，但delegate尚未返回时不能发布结果"
        );
        release.send(success).unwrap();
        let report = expect_callback_return(&returned, value, success);
        assert!(!callback_return_matches(
            &report,
            &TraceNodeId::new(REMOTE),
            value,
            !success
        ));
        let different_trace = TraceSelection {
            trace_id: TraceId::from_bytes([0xee; 16]),
            ..value
        };
        let different_deadline = TraceSelection {
            expires_at: value.expires_at + Duration::from_secs(1),
            ..value
        };
        assert!(!callback_return_matches(
            &report,
            &TraceNodeId::new(SELF),
            value,
            success
        ));
        assert!(!callback_return_matches(
            &report,
            &TraceNodeId::new(REMOTE),
            different_trace,
            success
        ));
        assert!(!callback_return_matches(
            &report,
            &TraceNodeId::new(REMOTE),
            different_deadline,
            success
        ));
        reports.push(report);
    }
    // 后两次回调实际均失败；不论累计快照怎样交错，任何一次都不得被判为成功。
    for report in &reports[2..] {
        assert!(report.outcome.is_err());
        assert!(!callback_return_matches(
            report,
            &TraceNodeId::new(REMOTE),
            report.selection,
            true
        ));
    }
    wait_for(
        || recorder.recorder.stats().failed_notifications == 3,
        "校准的三次Err已回到logger统计",
    );
    assert!(matches!(
        returned.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    recorder.stop();
}

/// 真实记录器出口；无Span所以export调用必须为零，关闭责任归此宿主夹具。
#[derive(Debug)]
struct Exporter(Arc<AtomicUsize>);
impl SpanExporter for Exporter {
    async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct Silent;
impl TraceNotifier for Silent {
    fn notify(&self, _remote: &TraceNodeId, _selection: TraceSelection) -> anyhow::Result<()> {
        panic!("未登记邻居，不得发通知")
    }
}
struct Recorder {
    recorder: FlightRecorder,
    guard: Option<FlightRecorderGuard>,
    exports: Arc<AtomicUsize>,
}
impl Recorder {
    fn new(local: &str) -> Self {
        Self::with_notifier(local, Arc::new(Silent))
    }
    fn with_notifier(local: &str, notifier: Arc<dyn TraceNotifier>) -> Self {
        let exports = Arc::new(AtomicUsize::new(0));
        let guard = FlightRecorder::start(
            pi_logger::observability::FlightRecorderConfig {
                retention_secs: 60,
                followup_secs: 0,
                worker_poll_ms: 5,
                export_retries: 0,
                ..Default::default()
            },
            Exporter(exports.clone()),
            Resource::builder_empty().build(),
        )
        .unwrap();
        let recorder = guard.recorder();
        recorder
            .enable_coordination(TraceNodeId::new(local), notifier)
            .unwrap();
        Self {
            recorder,
            guard: Some(guard),
            exports,
        }
    }
    fn selected(&self, count: u64) {
        wait_for(
            || self.recorder.stats().selected_traces == count,
            "真实记录器选择",
        );
    }
    fn stop(&mut self) {
        if let Some(guard) = self.guard.take() {
            guard.shutdown();
        }
        assert_eq!(self.exports.load(Ordering::SeqCst), 0);
    }
}
impl Drop for Recorder {
    fn drop(&mut self) {
        self.stop();
    }
}
fn binding_cause(error: &CoordinationError, expected: &str) {
    let first = error.source().expect("Binding须有直接原因");
    let second = error.source().expect("重复查询不得丢原因");
    assert!(std::ptr::eq(first, second));
    assert_eq!(first.to_string(), expected);
    assert!(first.source().is_none());
    assert!(!first.is::<ServiceError>(), "Binding不能改写为Service");
}
fn send_closed(error: &CoordinationSendError) {
    let cause = error.source().expect("关闭发送必须保留原因");
    assert_eq!(cause.to_string(), "协调绑定已关闭");
    assert!(cause.source().is_none());
}

/// 校准不调用任何coordinator芯：真实表示、配对终端、宿主提交及panic/日志观察有正负对照。
pub(super) fn test_coordinator_fixture_wire_route_and_observers() {
    isolated(|panics| {
        panics.calibration();
        check_error_text_controls();
        assert_eq!(COORDINATION_SERVICE_PORT, 4091);
        assert_eq!(COORDINATION_SERVICE_LAYER, LAYER);
        let rig = default_rig();
        let expected = TraceSelection {
            trace_id: TraceId::from_bytes(TRACE),
            expires_at: UNIX_EPOCH + Duration::from_secs(2),
        };
        let bytes = received_bytes(expected, SOURCE);
        assert_packet(&bytes, expected, SOURCE);
        let mut reader = Reader::new(&bytes);
        assert_eq!(reader.read_u8().unwrap(), 2);
        assert_eq!(reader.read_i8().unwrap(), -1);
        assert_eq!(reader.read_varuint32().unwrap(), 1_100_815);
        let mut peer_multi = MultiLayerCodec::new(Arc::new(Fory::default()));
        peer_multi.register_layer_fory(LAYER, peer_codec().default_codec().clone());
        peer_multi.switch_layer(LAYER).unwrap();
        let codec = &peer_multi;
        let decoded: PeerMessage = codec.default_decode(bytes.clone()).unwrap();
        assert_eq!(decoded.trace_id, TRACE);
        assert_eq!(decoded.source, SOURCE.as_bytes());
        assert_eq!(decoded.expires_at_100ns, 20_000_000);
        let calls = install_host(&rig, &HOST_DEFAULT);
        assert_host(&rig, &calls, 1);
        rig.terminal
            .terminal
            .send(
                ServiceAddress::new(host(REMOTE), port()),
                bytes.clone(),
                None,
            )
            .unwrap();
        let packets = rig.wire.packets();
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].address.host, host(REMOTE));
        assert_eq!(u16::from(packets[0].address.port), 4091);
        assert_eq!(packets[0].bytes, bytes);
        let observer = Observer::default();
        tracing::subscriber::with_default(observer.clone(), || {
            tracing::warn!(target:"pi_tracing_coordinator::control", value=7);
            tracing::warn!(target:"pi_tracing_coordinator", value=8);
            tracing::warn!(target:"pi_tracing_coordinator_extra", value=9);
        });
        let saved = observer.0.lock().unwrap();
        assert_eq!(saved.len(), 2);
        assert_eq!(saved[0].fields.get("value").map(String::as_str), Some("7"));
    });
}
/// 校准真实start及两个不同记录器，统计只验证选择效果，绝不冒充精确API调用数。
pub(super) fn test_coordinator_fixture_real_recorder_controls() {
    isolated(|_| {
        check_callback_return_controls();
        let mut first = Recorder::new("校准-A");
        let mut second = Recorder::new("校准-B");
        assert!(first
            .recorder
            .apply_remote_selection(notice(0x41), TraceNodeId::new("外部")));
        first.recorder.seal_current_thread();
        first.selected(1);
        assert_eq!(second.recorder.stats().selected_traces, 0);
        assert!(second
            .recorder
            .apply_remote_selection(notice(0x42), TraceNodeId::new("外部")));
        second.recorder.seal_current_thread();
        second.selected(1);
        first.stop();
        second.stop();
        assert_eq!(first.recorder.stats().selected_traces, 1);
        assert_eq!(second.recorder.stats().selected_traces, 1);
    });
}

fn check_lazy<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    rig: Rig<C>,
    coordinator: Coordinator<C>,
) where
    pi_service::CollectedService<C>: pi_service::inventory::Collect,
{
    assert!(rig.runtime.get_status(port()).is_none());
    assert!(rig.events.0.lock().unwrap().is_empty());
    assert!(rig.wire.packets().is_empty());
    assert_eq!(rig.tasks.accepted.load(Ordering::SeqCst), 0);
    assert!(rig.tasks.queue.lock().unwrap().is_empty());
    let observer = Observer::default();
    step("目标：构造后第一次绑定context，不得因构造提前发布而拒绝");
    tracing::subscriber::with_default(observer.clone(), || {
        coordinator.bind_service_context(&rig.runtime)
    })
    .unwrap();
    assert!(
        rig.runtime.get_status(port()).is_none(),
        "发布context不注册服务"
    );
    rig.runtime
        .load_services_from_inventory(&[COORDINATION_SERVICE_LAYER])
        .unwrap();
    rig.runtime.registry().init(&rig.runtime, port()).unwrap();
    assert_meta(&rig);
    assert_receive_rejected(&rig, "记录器不可用");
    assert!(
        observer.0.lock().unwrap().is_empty(),
        "构造和绑定不额外记录日志"
    );
}
pub(super) fn test_coordinator_default_constructor_is_lazy() {
    isolated(|_| {
        let observer = Observer::default();
        tracing::subscriber::with_default(observer.clone(), || {
            let rig = default_rig();
            let coordinator = default_coordinator(&rig, "");
            check_lazy(rig, coordinator);
        });
        assert!(observer.0.lock().unwrap().is_empty(), "构造不额外记录日志");
    });
}
pub(super) fn test_coordinator_multi_constructor_is_lazy() {
    isolated(|_| {
        let observer = Observer::default();
        tracing::subscriber::with_default(observer.clone(), || {
            let rig = multi_rig();
            let coordinator = multi_coordinator(&rig);
            check_lazy(rig, coordinator);
        });
        assert!(observer.0.lock().unwrap().is_empty(), "构造不额外记录日志");
    });
}

/// default存在且有真实登记也不能作为明确目标层；两种缺失装配均由new_multi_layer自身拒绝。
pub(super) fn test_coordinator_multi_missing_layer_rejects_without_fallback() {
    isolated(|_| {
        for codec in [
            MultiLayerCodec::new(registered()),
            MultiLayerCodec::with_default_layer("unrelated", registered()),
        ] {
            let rig = Rig::new(codec);
            assert!(!rig.runtime.codec().has_layer(LAYER));
            step("目标：new_multi_layer缺明确目标，不能采用default回退");
            let error = match TracingCoordinator::new_multi_layer(
                &rig.terminal,
                &rig.runtime,
                TraceNodeId::new(LOCAL),
            ) {
                Err(error) => error,
                Ok(_) => panic!("缺层必须拒绝"),
            };
            assert!(error.source().is_none());
            assert_error_text(&error, 2);
            assert!(rig.runtime.get_status(port()).is_none());
            assert!(rig.events.0.lock().unwrap().is_empty());
            assert!(rig.wire.packets().is_empty());
        }
    });
}
fn check_notifier<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    rig: Rig<C>,
    coordinator: Coordinator<C>,
) {
    step("目标：notifier共享既有client");
    let first = coordinator.notifier();
    let second = coordinator.notifier();
    assert!(Arc::ptr_eq(&first, &second));
    let value = TraceSelection {
        trace_id: TraceId::from_bytes(TRACE),
        expires_at: UNIX_EPOCH + Duration::from_secs(2),
    };
    first.notify(&TraceNodeId::new(REMOTE), value).unwrap();
    let packets = rig.wire.packets();
    assert_eq!(packets.len(), 1);
    assert_eq!(u16::from(packets[0].address.port), 4091);
    assert_packet(&packets[0].bytes, value, LOCAL);
    let drops = rig.terminal.drops.clone();
    let weak = Arc::downgrade(&rig.terminal);
    drop(coordinator);
    drop(rig.terminal);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let observer = Observer::default();
    let error = tracing::subscriber::with_default(observer.clone(), || {
        second.notify(&TraceNodeId::new(REMOTE), value)
    })
    .unwrap_err();
    send_closed(
        error
            .downcast_ref::<CoordinationSendError>()
            .expect("同一发送错误包装"),
    );
    assert_eq!(rig.wire.packets().len(), 1);
    assert_eq!(observer.0.lock().unwrap().len(), 1);
}
pub(super) fn test_coordinator_default_notifier_shares_client_and_is_weak() {
    isolated(|_| {
        let rig = default_rig();
        let coordinator = default_coordinator(&rig, LOCAL);
        check_notifier(rig, coordinator);
    });
}
pub(super) fn test_coordinator_multi_notifier_shares_client_and_is_weak() {
    isolated(|_| {
        let rig = multi_rig();
        let coordinator = multi_coordinator(&rig);
        check_notifier(rig, coordinator);
    });
}

/// 真实logger配置不写文件、不启用日志/指标出口、不创建Span；构造OTLP exporter但无实际导出请求。
fn logger_config(enabled: bool) -> pi_logger::ObservabilityConfig {
    let mut config =
        pi_logger::observability::observability_config_from_rust_log(Some("off")).unwrap();
    config.log.local.enabled = Some(false);
    config.log.console = None;
    config.log.remote.enabled = Some(false);
    config.metrics = None;
    config.trace.enabled = enabled;
    config.trace.protocol = pi_logger::observability::OtlpProtocol::HttpBinary;
    config.trace.endpoint = "http://127.0.0.1:4318".into();
    config.trace.flight_recorder = Some(pi_logger::observability::FlightRecorderConfig {
        retention_secs: 60,
        followup_secs: 0,
        worker_poll_ms: 5,
        export_retries: 0,
        ..Default::default()
    });
    config
}
pub(super) fn test_coordinator_notifier_is_injected_into_real_logger() {
    isolated(|_| {
        let rig = default_rig();
        let coordinator = default_coordinator(&rig, LOCAL);
        let notifier = coordinator.notifier();
        let (observed, returned) = observe_notifier(notifier.clone());
        step("目标：透明观测的共享notifier注入真实logger初始化");
        let (_reload, guards) = pi_logger::init_observability_with_coordination(
            logger_config(true),
            TraceNodeId::new(LOCAL),
            observed,
        )
        .unwrap();
        let recorder = guards.flight_recorder.as_ref().unwrap().recorder();
        assert!(rig.wire.packets().is_empty(), "初始化不能发送");
        coordinator.bind_recorder(Some(recorder.clone())).unwrap();
        let value = notice(0x51);
        assert!(recorder.record_peer(value.trace_id, TraceNodeId::new(REMOTE)));
        assert!(recorder.apply_remote_selection(value, TraceNodeId::new("上游")));
        recorder.seal_current_thread();
        // 必须先亲见0x51的生产notify返回Ok，再close；传输已有字节或attempted=1都不够。
        let first_return = expect_callback_return(&returned, value, true);
        assert!(first_return.outcome.is_ok());
        assert!(matches!(
            returned.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        let packets = rig.wire.packets();
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].address.host, host(REMOTE));
        assert_eq!(u16::from(packets[0].address.port), 4091);
        assert_packet(&packets[0].bytes, value, LOCAL);
        assert_eq!(recorder.stats().notification_attempts, 1);
        assert_eq!(recorder.stats().failed_notifications, 0);
        coordinator.close();
        let later = notice(0x52);
        assert!(recorder.record_peer(later.trace_id, TraceNodeId::new(REMOTE)));
        assert!(recorder.apply_remote_selection(later, TraceNodeId::new("上游")));
        recorder.seal_current_thread();
        let second_return = expect_callback_return(&returned, later, false);
        let failure = second_return.outcome.as_ref().unwrap_err();
        assert!(
            failure.is_send_error,
            "必须是生产CoordinationSendError：{}",
            failure.text
        );
        assert_eq!(failure.cause.as_deref(), Some("协调绑定已关闭"));
        wait_for(
            || recorder.stats().failed_notifications == 1,
            "已确认0x51成功及0x52 Closed错误后的上游统计",
        );
        assert_eq!(recorder.stats().notification_attempts, 2);
        assert_eq!(rig.wire.packets().len(), 1);
        assert!(matches!(
            returned.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        drop(coordinator);
        let drops = rig.terminal.drops.clone();
        drop(rig.terminal);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let error = notifier
            .notify(&TraceNodeId::new(REMOTE), notice(0x53))
            .unwrap_err();
        send_closed(error.downcast_ref::<CoordinationSendError>().unwrap());
        guards.shutdown();
    });
}
pub(super) fn test_coordinator_logger_without_recorder_binds_none() {
    isolated(|_| {
        let rig = default_rig();
        let coordinator = default_coordinator(&rig, LOCAL);
        let (_reload, guards) = pi_logger::init_observability_with_coordination(
            logger_config(false),
            TraceNodeId::new(LOCAL),
            coordinator.notifier(),
        )
        .unwrap();
        assert!(guards.flight_recorder.is_none());
        step("目标：宿主按logger不可用结果一次绑定None");
        coordinator.bind_recorder(None).unwrap();
        coordinator.install_service(&rig.runtime).unwrap();
        assert_receive_rejected(&rig, "记录器不可用");
        let error = coordinator.bind_recorder(None).unwrap_err();
        binding_cause(&error, "记录器已绑定，不能重复绑定");
        coordinator
            .send_selection(
                &TraceNodeId::new(REMOTE),
                notice(0x54),
                &TraceNodeId::new(SOURCE),
            )
            .unwrap();
        assert_eq!(rig.wire.packets().len(), 1);
        guards.shutdown();
    });
}
pub(super) fn test_coordinator_logger_error_remains_host_responsibility() {
    isolated(|_| {
        let rig = default_rig();
        let coordinator = default_coordinator(&rig, LOCAL);
        let error = pi_logger::init_observability_with_coordination(
            logger_config(true),
            TraceNodeId::new(""),
            coordinator.notifier(),
        )
        .err()
        .expect("真实logger空身份失败");
        assert!(error.to_string().contains("identity"));
        step("目标：logger失败不消费coordinator一次绑定机会");
        coordinator.bind_recorder(None).unwrap();
        coordinator
            .send_selection(
                &TraceNodeId::new(REMOTE),
                notice(0x55),
                &TraceNodeId::new(SOURCE),
            )
            .unwrap();
        assert_eq!(rig.wire.packets().len(), 1);
    });
}
fn check_binding<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    rig: Rig<C>,
    coordinator: Coordinator<C>,
    with_recorder: bool,
) {
    let mut first = Recorder::new("记录器-A");
    let mut second = Recorder::new("记录器-B");
    step("目标：Some/None一次绑定及重复保持原拥有者");
    coordinator
        .bind_recorder(if with_recorder {
            Some(first.recorder.clone())
        } else {
            None
        })
        .unwrap();
    for replacement in [Some(second.recorder.clone()), None] {
        let error = coordinator.bind_recorder(replacement).unwrap_err();
        binding_cause(&error, "记录器已绑定，不能重复绑定");
    }
    coordinator.install_service(&rig.runtime).unwrap();
    let value = notice(0x61);
    if with_recorder {
        rig.deliver(value, "远端");
        first.recorder.seal_current_thread();
        first.selected(1);
    } else {
        assert_receive_rejected(&rig, "记录器不可用");
    }
    assert!(second
        .recorder
        .apply_remote_selection(notice(0x62), TraceNodeId::new("远端")));
    second.recorder.seal_current_thread();
    second.selected(1);
    coordinator.close();
    let closed = coordinator.bind_recorder(None).unwrap_err();
    binding_cause(&closed, "协调绑定已关闭");
    assert!(
        first
            .recorder
            .apply_remote_selection(notice(0x63), TraceNodeId::new("远端")),
        "close不shutdown宿主记录器"
    );
    first.recorder.seal_current_thread();
    first.selected(if with_recorder { 2 } else { 1 });
    first.stop();
    second.stop();
    assert_eq!(second.recorder.stats().selected_traces, 1);
}
pub(super) fn test_coordinator_default_some_binding_keeps_original_recorder() {
    isolated(|_| {
        let rig = default_rig();
        let c = default_coordinator(&rig, LOCAL);
        check_binding(rig, c, true);
    });
}
pub(super) fn test_coordinator_multi_some_binding_keeps_original_recorder() {
    isolated(|_| {
        let rig = multi_rig();
        let c = multi_coordinator(&rig);
        check_binding(rig, c, true);
    });
}
pub(super) fn test_coordinator_none_consumes_binding_opportunity() {
    isolated(|_| {
        let rig = default_rig();
        let c = default_coordinator(&rig, LOCAL);
        check_binding(rig, c, false);
    });
}

fn check_context<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    rig: Rig<C>,
    original: Coordinator<C>,
    candidate: Coordinator<C>,
) where
    pi_service::CollectedService<C>: pi_service::inventory::Collect,
{
    let mut recorder = Recorder::new("原context记录器");
    original
        .bind_recorder(Some(recorder.recorder.clone()))
        .unwrap();
    step("目标：首次context发布及同Arc/不同候选预占拒绝");
    original.bind_service_context(&rig.runtime).unwrap();
    assert!(rig.runtime.get_status(port()).is_none());
    assert!(rig.events.0.lock().unwrap().is_empty());
    for error in [
        original.bind_service_context(&rig.runtime).unwrap_err(),
        candidate.bind_service_context(&rig.runtime).unwrap_err(),
        candidate.install_service(&rig.runtime).unwrap_err(),
    ] {
        context_error(&error);
    }
    candidate.close();
    drop(candidate);
    rig.runtime
        .load_services_from_inventory(&[COORDINATION_SERVICE_LAYER])
        .unwrap();
    rig.runtime.registry().init(&rig.runtime, port()).unwrap();
    let value = notice(0x71);
    rig.deliver(value, "远端");
    recorder.recorder.seal_current_thread();
    recorder.selected(1);
    assert_meta(&rig);
    original.close();
    assert_receive_rejected(&rig, "协调绑定已关闭");
    recorder.stop();
    assert_eq!(recorder.recorder.stats().selected_traces, 1);
}
pub(super) fn test_coordinator_default_context_preoccupation_keeps_original_owner() {
    isolated(|_| {
        let rig = default_rig();
        let a = default_coordinator(&rig, LOCAL);
        let b = default_coordinator(&rig, LOCAL);
        check_context(rig, a, b);
    });
}
pub(super) fn test_coordinator_multi_context_preoccupation_keeps_original_owner() {
    isolated(|_| {
        let rig = multi_rig();
        let a = multi_coordinator(&rig);
        let b = multi_coordinator(&rig);
        check_context(rig, a, b);
    });
}
fn check_closed_context<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    rig: Rig<C>,
    closed: Coordinator<C>,
    open: Coordinator<C>,
) where
    pi_service::CollectedService<C>: pi_service::inventory::Collect,
{
    closed.close();
    step("目标：关闭准入必须先于容器发布/注册");
    for error in [
        closed.bind_service_context(&rig.runtime).unwrap_err(),
        closed.install_service(&rig.runtime).unwrap_err(),
    ] {
        binding_cause(&error, "协调绑定已关闭");
    }
    assert!(rig.events.0.lock().unwrap().is_empty());
    assert!(rig.runtime.get_status(port()).is_none());
    open.bind_service_context(&rig.runtime).unwrap();
    let occupied = closed.bind_service_context(&rig.runtime).unwrap_err();
    binding_cause(&occupied, "协调绑定已关闭");
    // 开放候选首次成功证明此前拒绝未插入本次context；内部零查询/写入及旧Arc身份另审。
    rig.runtime
        .load_services_from_inventory(&[COORDINATION_SERVICE_LAYER])
        .unwrap();
    rig.runtime.registry().init(&rig.runtime, port()).unwrap();
    assert_receive_rejected(&rig, "记录器不可用");
}
pub(super) fn test_coordinator_default_closed_context_has_zero_publication() {
    isolated(|_| {
        let rig = default_rig();
        let a = default_coordinator(&rig, LOCAL);
        let b = default_coordinator(&rig, LOCAL);
        check_closed_context(rig, a, b);
    });
}
pub(super) fn test_coordinator_multi_closed_context_has_zero_publication() {
    isolated(|_| {
        let rig = multi_rig();
        let a = multi_coordinator(&rig);
        let b = multi_coordinator(&rig);
        check_closed_context(rig, a, b);
    });
}

fn check_install<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    rig: Rig<C>,
    coordinator: Coordinator<C>,
    inventory: bool,
) where
    pi_service::CollectedService<C>: pi_service::inventory::Collect,
{
    assert!(rig.runtime.get_status(port()).is_none());
    step("目标：对应具体codec的真实安装及4091路由");
    if inventory {
        coordinator.bind_service_context(&rig.runtime).unwrap();
        rig.runtime
            .load_services_from_inventory(&["not-the-coordinator-layer"])
            .unwrap();
        assert!(rig.runtime.get_status(port()).is_none());
        rig.runtime
            .load_services_from_inventory(&[COORDINATION_SERVICE_LAYER])
            .unwrap();
        assert!(matches!(
            rig.runtime.get_status(port()),
            Some(ServiceStatus::Registered)
        ));
        rig.runtime.registry().init(&rig.runtime, port()).unwrap();
    } else {
        coordinator.install_service(&rig.runtime).unwrap();
    }
    assert_meta(&rig);
    assert!(matches!(
        rig.runtime.get_status(port()),
        Some(ServiceStatus::Running)
    ));
    let events = rig.events.0.lock().unwrap();
    assert_eq!(events.len(), 2);
    assert!(events[0].0.prev_status.is_none());
    assert!(matches!(
        events[0].0.next_status,
        Some(ServiceStatus::Registered)
    ));
    assert!(matches!(
        events[1].0.prev_status,
        Some(ServiceStatus::Registered)
    ));
    assert!(matches!(
        events[1].0.next_status,
        Some(ServiceStatus::Running)
    ));
    assert!(events.iter().all(|(_, id)| *id == thread::current().id()));
    drop(events);
    assert_receive_rejected(&rig, "记录器不可用");
    let mut recorder = Recorder::new("接收宿主");
    coordinator
        .bind_recorder(Some(recorder.recorder.clone()))
        .unwrap();
    let value = notice(0x72);
    rig.deliver(value, "实际来源");
    recorder.recorder.seal_current_thread();
    recorder.selected(1);
    let explicit = notice(0x73);
    coordinator
        .send_selection(
            &TraceNodeId::new(REMOTE),
            explicit,
            &TraceNodeId::new(SOURCE),
        )
        .unwrap();
    let packets = rig.wire.packets();
    assert_eq!(packets.len(), 1);
    assert_eq!(packets[0].address.host, host(REMOTE));
    assert_eq!(u16::from(packets[0].address.port), 4091);
    assert_packet(&packets[0].bytes, explicit, SOURCE);
    let error = coordinator.install_service(&rig.runtime).unwrap_err();
    context_error(&error);
    assert_eq!(rig.events.0.lock().unwrap().len(), 2);
    recorder.stop();
    assert_eq!(recorder.recorder.stats().selected_traces, 1);
}
pub(super) fn test_coordinator_default_explicit_install_and_route() {
    isolated(|_| {
        let rig = default_rig();
        let c = default_coordinator(&rig, LOCAL);
        check_install(rig, c, false);
    });
}
pub(super) fn test_coordinator_multi_explicit_install_and_route() {
    isolated(|_| {
        let rig = multi_rig();
        let c = multi_coordinator(&rig);
        check_install(rig, c, false);
    });
}
pub(super) fn test_coordinator_default_inventory_install_and_route() {
    isolated(|_| {
        let rig = default_rig();
        let c = default_coordinator(&rig, LOCAL);
        check_install(rig, c, true);
    });
}
pub(super) fn test_coordinator_multi_inventory_install_and_route() {
    isolated(|_| {
        let rig = multi_rig();
        let c = multi_coordinator(&rig);
        check_install(rig, c, true);
    });
}

fn check_conflict<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    rig: Rig<C>,
    coordinator: Coordinator<C>,
    occupant: &'static dyn Service<Codec = C>,
) {
    let calls = install_host(&rig, occupant);
    let old = rig.runtime.get_meta(port()).unwrap();
    let before = rig.events.0.lock().unwrap().len();
    step("目标：context发布后真实4091注册冲突，保留前项和旧服务");
    let error = coordinator.install_service(&rig.runtime).unwrap_err();
    assert!(matches!(
        error.source().unwrap().downcast_ref::<ServiceError>(),
        Some(ServiceError::RegisterPortConflict(4091))
    ));
    assert_eq!(rig.runtime.get_meta(port()).unwrap().id, old.id);
    assert_eq!(rig.events.0.lock().unwrap().len(), before);
    assert!(matches!(
        rig.runtime.get_status(port()),
        Some(ServiceStatus::Running)
    ));
    context_error(&coordinator.bind_service_context(&rig.runtime).unwrap_err());
    coordinator.close();
    drop(coordinator);
    ready(rig.runtime.on_send(port(), vec![1], None)).unwrap();
    assert_eq!(calls.0.load(Ordering::SeqCst), 1);
    assert_eq!(rig.wire.other_calls.load(Ordering::SeqCst), 0);
}
pub(super) fn test_coordinator_default_conflict_keeps_context_and_old_service() {
    isolated(|_| {
        let rig = default_rig();
        let c = default_coordinator(&rig, LOCAL);
        check_conflict(rig, c, &OCCUPANT_DEFAULT);
    });
}
pub(super) fn test_coordinator_multi_conflict_keeps_context_and_old_service() {
    isolated(|_| {
        let rig = multi_rig();
        let c = multi_coordinator(&rig);
        check_conflict(rig, c, &OCCUPANT_MULTI);
    });
}

// 合法宿主inventory条目使用单独layer和独立端口；普通coordinator加载只按自己的layer，不受影响。
static EXTRA_DEFAULT: HostService<DefaultCodec, 4089> = HostService(PhantomData);
static EXTRA_MULTI: HostService<MultiLayerCodec, 4089> = HostService(PhantomData);
pi_service::inventory::submit! {pi_service::CollectedService::<DefaultCodec>{svc:&HOST_DEFAULT,layer:"slice-coordinator-host"}}
pi_service::inventory::submit! {pi_service::CollectedService::<DefaultCodec>{svc:&EXTRA_DEFAULT,layer:"slice-coordinator-host"}}
pi_service::inventory::submit! {pi_service::CollectedService::<MultiLayerCodec>{svc:&HOST_MULTI,layer:"slice-coordinator-host"}}
pi_service::inventory::submit! {pi_service::CollectedService::<MultiLayerCodec>{svc:&EXTRA_MULTI,layer:"slice-coordinator-host"}}
fn check_inventory_failure<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    rig: Rig<C>,
    coordinator: Coordinator<C>,
) where
    pi_service::CollectedService<C>: pi_service::inventory::Collect,
{
    coordinator.bind_service_context(&rig.runtime).unwrap();
    let entries: Vec<_> = pi_service::inventory::iter::<pi_service::CollectedService<C>>()
        .filter(|item| item.layer == LAYER || item.layer == "slice-coordinator-host")
        .collect();
    assert_eq!(
        entries.len(),
        3,
        "不假定inventory顺序，但精确核对本场景集合"
    );
    let mut ports: Vec<u16> = entries
        .iter()
        .map(|item| u16::from(item.svc.port()))
        .collect();
    ports.sort_unstable();
    assert_eq!(ports, [4089, 4090, 4091], "核对真实库存的完整端口集合");
    let pivot = entries[1].svc;
    rig.runtime.registry().register(pivot).unwrap();
    step("目标：inventory按实际顺序首错停止、前项保留、后项不注册且不init");
    let loaded = rig
        .runtime
        .load_services_from_inventory(&[LAYER, "slice-coordinator-host"]);
    assert!(
        matches!(loaded,Err(ServiceError::RegisterPortConflict(actual)) if actual==u16::from(pivot.port()))
    );
    assert!(matches!(
        rig.runtime.get_status(entries[0].svc.port()),
        Some(ServiceStatus::Registered)
    ));
    assert!(matches!(
        rig.runtime.get_status(entries[1].svc.port()),
        Some(ServiceStatus::Registered)
    ));
    assert!(rig.runtime.get_status(entries[2].svc.port()).is_none());
    assert_eq!(
        rig.events.0.lock().unwrap().len(),
        2,
        "没有Running/Failed初始化事件"
    );
    context_error(&coordinator.bind_service_context(&rig.runtime).unwrap_err());
    coordinator.close();
}
pub(super) fn test_coordinator_default_inventory_first_error_preserves_prefix() {
    isolated(|_| {
        let rig = default_rig();
        let c = default_coordinator(&rig, LOCAL);
        check_inventory_failure(rig, c);
    });
}
pub(super) fn test_coordinator_multi_inventory_first_error_preserves_prefix() {
    isolated(|_| {
        let rig = multi_rig();
        let c = multi_coordinator(&rig);
        check_inventory_failure(rig, c);
    });
}

/// 宿主漏绑定context的真实框架失败：这是装配负面对照，不冒充合法install_service可达失败。
pub(super) fn test_coordinator_inventory_missing_context_preserves_registered() {
    isolated(|_| {
        let rig = default_rig();
        rig.runtime
            .load_services_from_inventory(&[COORDINATION_SERVICE_LAYER])
            .unwrap();
        assert!(
            matches!(rig.runtime.registry().init(&rig.runtime,port()),Err(ServiceError::BindOnlyReadNotFound(4091,field,ty)) if field=="context"&&ty=="CoordinationServiceContext")
        );
        assert!(matches!(
            rig.runtime.get_status(port()),
            Some(ServiceStatus::Registered)
        ));
        assert_eq!(rig.events.0.lock().unwrap().len(), 1);
        let coordinator = default_coordinator(&rig, LOCAL);
        coordinator.bind_service_context(&rig.runtime).unwrap();
        rig.runtime.registry().init(&rig.runtime, port()).unwrap();
        assert!(matches!(
            rig.runtime.get_status(port()),
            Some(ServiceStatus::Running)
        ));
    });
}

fn check_explicit_source<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    rig: Rig<C>,
    coordinator: Coordinator<C>,
) {
    let value = TraceSelection {
        trace_id: TraceId::from_bytes(TRACE),
        expires_at: UNIX_EPOCH + Duration::from_secs(2),
    };
    step("目标：公开send_selection按本次source而非local，单次提交");
    let observer = Observer::default();
    tracing::subscriber::with_default(observer.clone(), || {
        coordinator.send_selection(&TraceNodeId::new(REMOTE), value, &TraceNodeId::new(SOURCE))
    })
    .unwrap();
    let packets = rig.wire.packets();
    assert_eq!(packets.len(), 1);
    assert_eq!(packets[0].address.host, host(REMOTE));
    assert_eq!(u16::from(packets[0].address.port), 4091);
    assert_packet(&packets[0].bytes, value, SOURCE);
    assert!(observer.0.lock().unwrap().is_empty());
    assert_eq!(rig.wire.other_calls.load(Ordering::SeqCst), 0);
}
pub(super) fn test_coordinator_default_explicit_source_wins_over_empty_local() {
    isolated(|_| {
        let rig = default_rig();
        let c = default_coordinator(&rig, "");
        check_explicit_source(rig, c);
    });
}
pub(super) fn test_coordinator_multi_explicit_source_is_preserved() {
    isolated(|_| {
        let rig = multi_rig();
        let c = multi_coordinator(&rig);
        check_explicit_source(rig, c);
    });
}
pub(super) fn test_coordinator_send_preserves_actual_submit_error() {
    isolated(|_| {
        let rig = default_rig();
        let c = default_coordinator(&rig, LOCAL);
        let value = notice(0x74);
        *rig.wire.failure.lock().unwrap() =
            Some(ServiceError::CodecError("提交阶段/真实宿主原因".into()));
        let observer = Observer::default();
        step("目标：send返回真实终端Submit原因不重试不额外日志");
        let error: CoordinationSendError =
            tracing::subscriber::with_default(observer.clone(), || {
                c.send_selection(&TraceNodeId::new(REMOTE), value, &TraceNodeId::new(SOURCE))
            })
            .unwrap_err();
        assert!(
            matches!(error.source().unwrap().downcast_ref::<ServiceError>(),Some(ServiceError::CodecError(reason)) if reason=="提交阶段/真实宿主原因")
        );
        assert_eq!(rig.wire.packets().len(), 1);
        assert_packet(&rig.wire.packets()[0].bytes, value, SOURCE);
        assert!(observer.0.lock().unwrap().is_empty());
    });
}
pub(super) fn test_coordinator_self_target_uses_real_local_route() {
    isolated(|_| {
        let rig = default_rig();
        let c = default_coordinator(&rig, LOCAL);
        let mut recorder = Recorder::new("本地接收");
        c.bind_recorder(Some(recorder.recorder.clone())).unwrap();
        c.install_service(&rig.runtime).unwrap();
        step("目标：显式remote等于实际终端身份，经真实本地提交路由");
        c.send_selection(
            &TraceNodeId::new(SELF),
            notice(0x75),
            &TraceNodeId::new(SOURCE),
        )
        .unwrap();
        assert!(rig.wire.packets().is_empty());
        assert_eq!(rig.tasks.queue.lock().unwrap().len(), 1);
        assert_eq!(recorder.recorder.stats().selected_traces, 0);
        rig.tasks.drain();
        recorder.recorder.seal_current_thread();
        recorder.selected(1);
        recorder.stop();
        assert_eq!(recorder.recorder.stats().selected_traces, 1);
    });
}
fn check_close<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    rig: Rig<C>,
    coordinator: Coordinator<C>,
    host_service: &'static dyn Service<Codec = C>,
    drop_owner: bool,
    make_candidate: impl FnOnce(&Rig<C>) -> Coordinator<C>,
) {
    let calls = install_host(&rig, host_service);
    assert_host(&rig, &calls, 1);
    let mut recorder = Recorder::new("接收宿主");
    coordinator
        .bind_recorder(Some(recorder.recorder.clone()))
        .unwrap();
    coordinator.install_service(&rig.runtime).unwrap();
    let notifier = coordinator.notifier();
    rig.deliver(notice(0x76), "远端");
    recorder.recorder.seal_current_thread();
    recorder.selected(1);
    step("目标：公开close或自动析构只停止本库新准入");
    if drop_owner {
        drop(coordinator);
    } else {
        coordinator.close();
        coordinator.close();
        assert!(
            Arc::ptr_eq(&notifier, &coordinator.notifier()),
            "关闭后仍取得既有client"
        );
        send_closed(
            &coordinator
                .send_selection(
                    &TraceNodeId::new(REMOTE),
                    notice(0x77),
                    &TraceNodeId::new(SOURCE),
                )
                .unwrap_err(),
        );
        binding_cause(
            &coordinator.bind_recorder(None).unwrap_err(),
            "协调绑定已关闭",
        );
        binding_cause(
            &coordinator.bind_service_context(&rig.runtime).unwrap_err(),
            "协调绑定已关闭",
        );
        binding_cause(
            &coordinator.install_service(&rig.runtime).unwrap_err(),
            "协调绑定已关闭",
        );
    }
    let error = notifier
        .notify(&TraceNodeId::new(REMOTE), notice(0x78))
        .unwrap_err();
    send_closed(error.downcast_ref::<CoordinationSendError>().unwrap());
    assert_receive_rejected(&rig, "协调绑定已关闭");
    assert!(matches!(
        rig.runtime.get_status(port()),
        Some(ServiceStatus::Running)
    ));
    assert_meta(&rig);
    assert_host(&rig, &calls, 2);
    assert!(recorder
        .recorder
        .apply_remote_selection(notice(0x79), TraceNodeId::new("宿主直接入口")));
    recorder.recorder.seal_current_thread();
    recorder.selected(2);
    recorder.stop();
    assert_eq!(recorder.recorder.stats().selected_traces, 2);
    assert!(rig.wire.packets().is_empty());
    // 不能仅凭registry仍Running推断context保留；新候选必须实际观察到原条目预占。
    let candidate = make_candidate(&rig);
    context_error(&candidate.bind_service_context(&rig.runtime).unwrap_err());
}
pub(super) fn test_coordinator_default_close_preserves_shared_host() {
    isolated(|_| {
        let rig = default_rig();
        let c = default_coordinator(&rig, LOCAL);
        check_close(rig, c, &HOST_DEFAULT, false, |rig| {
            default_coordinator(rig, LOCAL)
        });
    });
}
pub(super) fn test_coordinator_multi_close_preserves_shared_host() {
    isolated(|_| {
        let rig = multi_rig();
        let c = multi_coordinator(&rig);
        check_close(rig, c, &HOST_MULTI, false, multi_coordinator);
    });
}
pub(super) fn test_coordinator_default_drop_preserves_shared_host() {
    isolated(|_| {
        let rig = default_rig();
        let c = default_coordinator(&rig, LOCAL);
        check_close(rig, c, &HOST_DEFAULT, true, |rig| {
            default_coordinator(rig, LOCAL)
        });
    });
}
pub(super) fn test_coordinator_multi_drop_preserves_shared_host() {
    isolated(|_| {
        let rig = multi_rig();
        let c = multi_coordinator(&rig);
        check_close(rig, c, &HOST_MULTI, true, multi_coordinator);
    });
}

/// 在真实Deref出口暂停，准入已返回；关闭须先返回，本次恢复后仍允许一次提交，不是压力测试。
fn check_admitted<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    rig: Rig<C>,
    coordinator: Coordinator<C>,
    drop_owner: bool,
) {
    let notifier = coordinator.notifier();
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    *rig.terminal.on_deref.lock().unwrap() = Some(Box::new(move || {
        entered_tx.send(()).unwrap();
        release_rx.recv_timeout(WAIT).expect("暂停必须有截止");
    }));
    let running = notifier.clone();
    let value = notice(0x7a);
    let worker = thread::spawn(move || running.notify(&TraceNodeId::new(REMOTE), value));
    entered_rx
        .recv_timeout(WAIT)
        .expect("必须实际到达准入后的宿主Deref");
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let closer = thread::spawn(move || {
        let retained = if drop_owner {
            drop(coordinator);
            None
        } else {
            coordinator.close();
            Some(coordinator)
        };
        done_tx.send(()).unwrap();
        retained
    });
    done_rx.recv_timeout(WAIT).expect("关闭不得等待在途");
    assert!(rig.wire.packets().is_empty());
    release_tx.send(()).unwrap();
    worker.join().unwrap().unwrap();
    let retained = closer.join().unwrap();
    assert_eq!(rig.wire.packets().len(), 1);
    assert_eq!(rig.wire.packets()[0].address.host, host(REMOTE));
    assert_eq!(u16::from(rig.wire.packets()[0].address.port), 4091);
    assert_packet(&rig.wire.packets()[0].bytes, value, LOCAL);
    let error = notifier
        .notify(&TraceNodeId::new(REMOTE), notice(0x7b))
        .unwrap_err();
    send_closed(error.downcast_ref::<CoordinationSendError>().unwrap());
    assert_eq!(rig.wire.packets().len(), 1);
    drop(retained);
}
pub(super) fn test_coordinator_default_admitted_send_finishes_after_close() {
    isolated(|_| {
        let rig = default_rig();
        let c = default_coordinator(&rig, LOCAL);
        check_admitted(rig, c, false);
    });
}
pub(super) fn test_coordinator_multi_admitted_send_finishes_after_close() {
    isolated(|_| {
        let rig = multi_rig();
        let c = multi_coordinator(&rig);
        check_admitted(rig, c, false);
    });
}
pub(super) fn test_coordinator_default_admitted_send_finishes_after_drop() {
    isolated(|_| {
        let rig = default_rig();
        let c = default_coordinator(&rig, LOCAL);
        check_admitted(rig, c, true);
    });
}

/// 不读取私有binding/context/Kind；四种错误均由真实根公开操作取得。
fn four_errors() -> [CoordinationError; 4] {
    let rig = default_rig();
    let c = default_coordinator(&rig, LOCAL);
    c.bind_recorder(None).unwrap();
    let binding = c.bind_recorder(None).unwrap_err();
    c.bind_service_context(&rig.runtime).unwrap();
    let occupied = c.bind_service_context(&rig.runtime).unwrap_err();
    let conflict_rig = default_rig();
    let conflict = default_coordinator(&conflict_rig, LOCAL);
    install_host(&conflict_rig, &OCCUPANT_DEFAULT);
    let service = conflict.install_service(&conflict_rig.runtime).unwrap_err();
    let missing = Rig::new(MultiLayerCodec::new(registered()));
    let layer = match TracingCoordinator::new_multi_layer(
        &missing.terminal,
        &missing.runtime,
        TraceNodeId::new(LOCAL),
    ) {
        Err(error) => error,
        Ok(_) => panic!("缺层"),
    };
    [binding, service, layer, occupied]
}
/// 与真实错误共用判定器：移除恰一次完整原因后，整个外层表述必须精确匹配同义分类。
/// 仅归一化大小写/空白和两端说明性分隔符；保留“不/未/not/!”等语义字符，绝不查找分类子串。
/// 未列入的其它文案一律拒绝并交人工复核，不猜测任意中文/英文句子的语义。
fn error_text_matches(text: &str, reason: Option<&str>, index: usize) -> bool {
    if index >= 4 || (index < 2) != reason.is_some() {
        return false;
    }
    let stage = if let Some(reason) = reason {
        if reason.is_empty() {
            return false;
        }
        let Some((prefix, suffix)) = text.split_once(reason) else {
            return false;
        };
        if suffix.contains(reason) {
            return false;
        }
        format!("{prefix}{suffix}")
    } else {
        text.to_owned()
    };
    let stage: String = stage
        .chars()
        .filter(|c| !c.is_whitespace())
        .flat_map(char::to_lowercase)
        .collect();
    let stage = stage.trim_matches([':', '：', '(', ')', '[', ']', '（', '）', '【', '】']);
    // 缺层允许附带真实固定layer名；除此之外不接受任意前后缀或解释语句。
    let layer_suffix = format!("：{LAYER}");
    let ascii_layer_suffix = format!(":{LAYER}");
    let stage = if index == 2 {
        stage
            .strip_suffix(layer_suffix.as_str())
            .or_else(|| stage.strip_suffix(ascii_layer_suffix.as_str()))
            .unwrap_or(stage)
    } else {
        stage
    };
    let stage = stage.strip_prefix("协调").unwrap_or(stage);
    let equivalent_labels: &[&str] = match index {
        0 => &[
            "绑定失败",
            "绑定错误",
            "绑定准入失败",
            "binding",
            "bindingerror",
            "bindingfailed",
            "bindingfailure",
        ],
        1 => &[
            "服务失败",
            "服务错误",
            "service",
            "serviceerror",
            "servicefailed",
            "servicefailure",
        ],
        2 => &[
            "编码层缺失",
            "编解码层缺失",
            "编码层不存在",
            "编解码层不存在",
            "codeclayermissing",
            "codeclayernotfound",
            "missingcodeclayer",
        ],
        3 => &[
            "上下文已存在",
            "上下文已安装",
            "上下文已预占",
            "上下文已被预占",
            "上下文已占用",
            "上下文已被占用",
            "contextalreadyinstalled",
            "contextalreadyexists",
            "contextoccupied",
        ],
        _ => unreachable!(),
    };
    equivalent_labels.contains(&stage)
}
/// R1校准：所有正对照仅匹配对应一类；错误状态、原因污染和相同混合外层均拒绝。
/// 这是测试判定器的负面对照，不构造私有CoordinationError，不调用生产TODO。
fn check_error_text_controls() {
    let accepted_negations: Vec<_> = [("编码层并不缺失", 2), ("上下文未被预占", 3)]
        .into_iter()
        .filter(|(text, index)| error_text_matches(text, None, *index))
        .collect();
    assert!(
        accepted_negations.is_empty(),
        "反向语义被错误接收：{accepted_negations:?}"
    );
    let binding = "记录器已绑定，不能重复绑定";
    let service = ServiceError::RegisterPortConflict(4091).to_string();
    for (text, reason, expected) in [
        (format!("协调绑定失败：{binding}"), Some(binding), 0),
        (format!("Binding error ({binding})"), Some(binding), 0),
        (
            format!("协调服务失败：{service}"),
            Some(service.as_str()),
            1,
        ),
        (
            format!("Service error: {service}"),
            Some(service.as_str()),
            1,
        ),
        ("协调编码层缺失：tracing-coordinator".to_owned(), None, 2),
        ("Codec layer not found".to_owned(), None, 2),
        ("协调上下文已存在".to_owned(), None, 3),
        ("ContextAlreadyInstalled".to_owned(), None, 3),
    ] {
        for index in 0..4 {
            assert_eq!(
                error_text_matches(&text, reason, index),
                index == expected,
                "每种正确表述必须且只能匹配自身分类：{text}，index={index}"
            );
        }
    }
    for (text, reason, index) in [
        ("编码层并不缺失".to_owned(), None, 2),
        ("上下文未被预占".to_owned(), None, 3),
        ("编码层不缺失".to_owned(), None, 2),
        ("编码层并非缺失".to_owned(), None, 2),
        ("编码层不再缺失".to_owned(), None, 2),
        ("编码层缺失并非事实".to_owned(), None, 2),
        ("编码层缺失：并非如此".to_owned(), None, 2),
        ("Codec layer not missing".to_owned(), None, 2),
        ("Codec layer is not missing".to_owned(), None, 2),
        ("Not CodecLayerMissing".to_owned(), None, 2),
        ("!CodecLayerMissing".to_owned(), None, 2),
        ("Codec layer exists".to_owned(), None, 2),
        ("上下文并未被预占".to_owned(), None, 3),
        ("上下文没有被预占".to_owned(), None, 3),
        ("上下文未被占用".to_owned(), None, 3),
        ("上下文并非已存在".to_owned(), None, 3),
        ("上下文已存在并非事实".to_owned(), None, 3),
        ("上下文已存在：并非如此".to_owned(), None, 3),
        ("Context is not occupied".to_owned(), None, 3),
        ("Context unoccupied".to_owned(), None, 3),
        ("Context not already installed".to_owned(), None, 3),
        ("!ContextAlreadyInstalled".to_owned(), None, 3),
        (format!("绑定并未失败：{binding}"), Some(binding), 0),
        (format!("并非Binding error：{binding}"), Some(binding), 0),
        (
            format!("服务没有错误：{service}"),
            Some(service.as_str()),
            1,
        ),
        (
            format!("Service did not fail：{service}"),
            Some(service.as_str()),
            1,
        ),
        ("编码层未初始化".to_owned(), None, 2),
        ("Codec layer uninitialized".to_owned(), None, 2),
        ("上下文已删除".to_owned(), None, 3),
        ("Context already removed".to_owned(), None, 3),
        ("编码层缺失但尚未初始化".to_owned(), None, 2),
        ("上下文已存在后已删除".to_owned(), None, 3),
        (format!("绑定或服务失败：{binding}"), Some(binding), 0),
        (
            format!("绑定或服务失败：{service}"),
            Some(service.as_str()),
            1,
        ),
        (format!("服务失败：{binding}"), Some(binding), 0),
        (format!("绑定失败：{service}"), Some(service.as_str()), 1),
        (binding.to_owned(), Some(binding), 0),
        (service.clone(), Some(service.as_str()), 1),
        (format!("协调绑定成功：{binding}"), Some(binding), 0),
        ("协调服务失败".to_owned(), Some(service.as_str()), 1),
    ] {
        assert!(
            !error_text_matches(&text, reason, index),
            "错误语义必须拒绝：{text}"
        );
    }
}
/// 去除真实原因后核对唯一外层语义；原因中的关键词不能提供分类证据。
fn assert_error_text(error: &CoordinationError, index: usize) {
    let reason = error.source().map(ToString::to_string);
    assert!(
        error_text_matches(&error.to_string(), reason.as_deref(), index),
        "错误分类语义不符"
    );
}
pub(super) fn test_coordinator_error_display_classifies_four_actual_failures() {
    isolated(|_| {
        let errors = four_errors();
        step("目标：CoordinationError Display四分类及直接原因");
        let observer = Observer::default();
        tracing::subscriber::with_default(observer.clone(), || {
            for (index, error) in errors.iter().enumerate() {
                assert_error_text(error, index);
            }
        });
        assert!(observer.0.lock().unwrap().is_empty());
        for i in 0..4 {
            for j in i + 1..4 {
                assert_ne!(errors[i].to_string(), errors[j].to_string());
            }
        }
    });
}
struct LimitedWriter {
    remaining: usize,
    failed: bool,
    output: String,
}
impl fmt::Write for LimitedWriter {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        assert!(!self.failed, "首次写失败后不得继续写");
        if text.len() > self.remaining {
            self.failed = true;
            return Err(fmt::Error);
        }
        self.remaining -= text.len();
        self.output.push_str(text);
        Ok(())
    }
}
pub(super) fn test_coordinator_error_display_propagates_every_writer_failure() {
    isolated(|_| {
        step("前置：公开操作构造四种真实错误");
        for error in four_errors() {
            let expected = error.to_string();
            step("目标：Display任意容量写失败原样传播");
            for capacity in 0..expected.len() {
                let mut writer = LimitedWriter {
                    remaining: capacity,
                    failed: false,
                    output: String::new(),
                };
                assert_eq!(
                    fmt::write(&mut writer, format_args!("{error}")),
                    Err(fmt::Error)
                );
                assert!(writer.failed);
            }
            let mut writer = LimitedWriter {
                remaining: expected.len(),
                failed: false,
                output: String::new(),
            };
            assert_eq!(fmt::write(&mut writer, format_args!("{error}")), Ok(()));
            assert_eq!(writer.output, expected);
            assert!(!writer.failed);
        }
    });
}
pub(super) fn test_coordinator_error_source_borrows_direct_causes() {
    isolated(|_| {
        let errors = four_errors();
        step("目标：source保留Binding/Service直接原因及None分类");
        for (index, error) in errors.iter().enumerate() {
            match (error.source(), error.source()) {
                (Some(first), Some(second)) if index < 2 => {
                    assert!(std::ptr::eq(first, second));
                    assert!(first.source().is_none());
                    if index == 0 {
                        assert_eq!(first.to_string(), "记录器已绑定，不能重复绑定");
                        assert!(!first.is::<ServiceError>());
                    } else {
                        assert!(matches!(
                            first.downcast_ref::<ServiceError>(),
                            Some(ServiceError::RegisterPortConflict(4091))
                        ));
                    }
                }
                (None, None) if index >= 2 => {}
                _ => panic!("四类source不符"),
            }
        }
    });
}

/// 用容器中的真正不可变标记判断runtime持有，不读取runtime内部Arc；此场景不编码，排除Fory TLS影响。
struct RuntimeMarker;
fn check_ownership<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    rig: Rig<C>,
    coordinator: Coordinator<C>,
) {
    // Multi模板当前为default；其整张层表由预绑定codec共享。这里只验证资源持有，不把它当选层正确性。
    let table = rig.runtime.codec().default_codec().clone();
    let weak_table = Arc::downgrade(&table);
    drop(table);
    let marker = Arc::new(RuntimeMarker);
    let weak_marker = Arc::downgrade(&marker);
    rig.runtime.contexts().insert_only_read(marker);
    let weak_terminal = Arc::downgrade(&rig.terminal);
    let drops = rig.terminal.drops.clone();
    let notifier = coordinator.notifier();
    step("目标：构造不强持终端/runtime，notifier仅允许延长codec持有");
    drop(rig);
    assert!(weak_terminal.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(
        weak_marker.upgrade().is_none(),
        "runtime及其contexts应随宿主持有释放"
    );
    assert!(weak_table.upgrade().is_some(), "client仍持预绑定codec");
    drop(coordinator);
    assert!(weak_table.upgrade().is_some(), "残留notifier可延长codec");
    drop(notifier);
    assert!(
        weak_table.upgrade().is_none(),
        "未编解码，无TLS/其它宿主持有时最后codec释放"
    );
}
pub(super) fn test_coordinator_default_does_not_retain_host_runtime_or_terminal() {
    isolated(|_| {
        let rig = default_rig();
        let c = default_coordinator(&rig, LOCAL);
        check_ownership(rig, c);
    });
}
pub(super) fn test_coordinator_multi_does_not_retain_host_runtime_or_terminal() {
    isolated(|_| {
        let rig = multi_rig();
        let c = multi_coordinator(&rig);
        check_ownership(rig, c);
    });
}

/// 预占必须区别于另一个无源缺层错误，不仅检查source为空。
fn context_error(error: &CoordinationError) {
    assert!(error.source().is_none());
    assert_error_text(error, 3);
}
