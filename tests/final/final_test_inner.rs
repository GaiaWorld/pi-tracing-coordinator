//! slice-final：仅消费crate根公开API的最终组合测试实现面。
//! @api:src/coordinator.rs @impl:src/coordinator_inner.rs @test:tests/final_test.rs
//! @design-doc:design/tracing-coordinator/07-design/call-chain.md#slice-final
//! 私有宿主传输仅接管/观测真实终端字节，宿主执行器只poll生产Future；业务转换、
//! 宏解码、准入、记录器Worker及通知线程全部由真实生产实现推进。
//! 每子进程仅安装一个4091接收入口，不在一个进程同时安装两种codec适配。
//! 本地宿主边界组合不冒充QUIC/IPC、跨主机部署矩阵或任意异常解码安全证明。

use fory::Fory;
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
    ServiceStatus, ServiceTerminal, ServiceTransport, ServiceVersion, TaskSubmitter,
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

const A: &str = "QmYwAPJzv5CZsnAzt8auVZRnGi2CkEJjQd2B5Hc9EcAm7H";
const B: &str = "QmYwAPJzv5CZsnAzt8auVZRnGi2CkEJjQd2B5Hc9EcAm7J";
const C: &str = "QmYwAPJzv5CZsnAzt8auVZRnGi2CkEJjQd2B5Hc9EcAm7K";
const D: &str = "QmYwAPJzv5CZsnAzt8auVZRnGi2CkEJjQd2B5Hc9EcAm7L";
const E: &str = "QmYwAPJzv5CZsnAzt8auVZRnGi2CkEJjQd2B5Hc9EcAm7M";
const WAIT: Duration = Duration::from_secs(3);
const QUIET: Duration = Duration::from_millis(150);
const CHILD: &str = "PI_SLICE_FINAL_CHILD";
const PHASE: &str = "PI_SLICE_FINAL_PHASE";
type Task = futures::future::BoxFuture<'static, ()>;
type Coordinator<C> = TracingCoordinator<Host<C>, C>;
type Constructor<C> =
    fn(&Arc<Host<C>>, &ServiceRuntime<C>, TraceNodeId) -> Result<Coordinator<C>, CoordinationError>;

/// 每个具名测试重执行自身，隔离全局宏static、logger及panic hook；不是远端部署。
fn child(name: &str, phase: &str) -> String {
    let mut process = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture", "--test-threads=1"])
        .env(CHILD, name)
        .env(PHASE, phase)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = process.stdout.take().unwrap();
    let mut stderr = process.stderr.take().unwrap();
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
        if let Some(status) = process.try_wait().unwrap() {
            break status;
        }
        if start.elapsed() > Duration::from_secs(25) {
            timed_out = true;
            process.kill().unwrap();
            break process.wait().unwrap();
        }
        thread::sleep(Duration::from_millis(5));
    };
    let output = out.join().unwrap();
    let errors = err.join().unwrap();
    assert!(!timed_out, "场景超时，不是产品RED：{output}\n{errors}");
    assert!(status.success(), "子进程失败：{status}\n{output}\n{errors}");
    assert!(
        output.contains("running 1 test") && output.contains("1 passed; 0 failed; 0 ignored"),
        "零命中/未完成不能作证据：{output}\n{errors}"
    );
    println!("FINAL_CHILD phase={phase} name={name}\n{output}\n{errors}");
    output
}

/// 真实框架可吞panic，因此保存所有线程的意外panic，成功出口必须为空。
fn panic_probe() -> Arc<Mutex<Vec<String>>> {
    let saved = Arc::new(Mutex::new(Vec::new()));
    let captured = saved.clone();
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        captured
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(info.to_string());
        previous(info);
    }));
    saved
}
/// R3：同步收尾完成后再采样，包括被真实Guard.join消费的后台panic。
/// 只覆盖已完成的同步收尾；不把Guard返回解释为任意通知线程flush。
fn panics_after_teardown(panics: &Arc<Mutex<Vec<String>>>, teardown: impl FnOnce()) -> Vec<String> {
    teardown();
    panics.lock().unwrap().clone()
}

/// 仅观察器校准使用：从真实导出线程的shutdown触发精确panic，绝不装进目标Pair。
#[derive(Debug)]
struct ShutdownPanicExporter(Arc<AtomicUsize>);
impl SpanExporter for ShutdownPanicExporter {
    async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
        Ok(())
    }
    fn shutdown(&self) -> OTelSdkResult {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("slice-final-R3-export-shutdown-control");
    }
}

/// 同一收尾观察器必须在真实Guard返回后看见精确关闭panic；只移除该预期校准事件。
fn check_shutdown_panic_observer(panics: &Arc<Mutex<Vec<String>>>) {
    assert!(panics.lock().unwrap().is_empty());
    let shutdowns = Arc::new(AtomicUsize::new(0));
    let guard = FlightRecorder::start(
        pi_logger::observability::FlightRecorderConfig {
            worker_poll_ms: 5,
            export_retries: 0,
            ..Default::default()
        },
        ShutdownPanicExporter(shutdowns.clone()),
        Resource::builder_empty().build(),
    )
    .unwrap();
    let observed = panics_after_teardown(panics, || guard.shutdown());
    assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(observed.len(), 1, "R3反例：不能在同步收尾前采样panic");
    assert!(observed[0].contains("slice-final-R3-export-shutdown-control"));
    let mut saved = panics.lock().unwrap();
    assert_eq!(saved.len(), 1, "仅允许这个观察器校准panic");
    assert_eq!(saved.remove(0), observed[0]);
    println!("FINAL_R3_SHUTDOWN_PANIC_CONTROL");
}

fn isolated(action: impl FnOnce()) {
    let name = thread::current().name().expect("具名测试").to_owned();
    if std::env::var(CHILD).ok().as_deref() != Some(name.as_str()) {
        child(&name, "case");
        return;
    }
    let panics = panic_probe();
    action();
    assert!(panics.lock().unwrap().is_empty(), "框架吞panic不能冒充通过");
}
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("此同步静态链出现Pending，不能以未完成冒充成功"),
    }
}
fn wait_for(mut predicate: impl FnMut() -> bool, label: &str) {
    let start = Instant::now();
    while !predicate() {
        assert!(start.elapsed() < WAIT, "有界等待失败：{label}");
        thread::sleep(Duration::from_millis(2));
    }
}
fn host(id: &str) -> ServiceHost {
    ServiceHost::Remote(id.parse().expect("合法PeerId"))
}
fn selection(tag: u8) -> TraceSelection {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    TraceSelection {
        trace_id: TraceId::from_bytes([tag; 16]),
        expires_at: UNIX_EPOCH + Duration::from_secs(seconds + 30),
    }
}
fn ticks(value: SystemTime) -> i64 {
    i64::try_from(value.duration_since(UNIX_EPOCH).unwrap().as_nanos() / 100).unwrap()
}
fn registered() -> Arc<Fory> {
    let panic_on_error = fory_core::error::should_panic_on_error();
    eprintln!("FINAL_DEPENDENCY FORY_PANIC_ON_ERROR={panic_on_error}");
    assert!(
        !panic_on_error,
        "普通Err场景要求实际FORY_PANIC_ON_ERROR=false"
    );
    let mut table = Fory::default();
    register_fory_types(&mut table).unwrap();
    Arc::new(table)
}
fn default_codec() -> DefaultCodec {
    let table = registered();
    let codec = DefaultCodec::new(table.clone());
    assert!(Arc::ptr_eq(&*codec.default_codec(), &table));
    codec
}
fn multi_codec() -> MultiLayerCodec {
    let table = registered();
    let codec = MultiLayerCodec::new(Arc::new(Fory::default()));
    codec.register_layer_fory(COORDINATION_SERVICE_LAYER, table.clone());
    assert!(codec.has_layer(COORDINATION_SERVICE_LAYER));
    assert!(Arc::ptr_eq(
        &*codec.layer_fory(COORDINATION_SERVICE_LAYER).unwrap(),
        &table
    ));
    codec
}

/// 独立对端表示只用于输入与观察；消息由真实Fory处理，不复制生产业务转换。
#[derive(Debug, fory::ForyObject)]
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
fn peer_bytes(trace: [u8; 16], source: &[u8], deadline: i64) -> Vec<u8> {
    peer_codec()
        .default_encode(&PeerMessage {
            trace_id: trace,
            source: source.to_vec(),
            expires_at_100ns: deadline,
        })
        .unwrap()
}
#[derive(Clone)]
struct Packet {
    address: ServiceAddress,
    bytes: Vec<u8>,
}
fn assert_packet(packet: &Packet, target: &str, value: TraceSelection, source: &str) {
    assert_eq!(packet.address.host, host(target));
    assert_eq!(u16::from(packet.address.port), COORDINATION_SERVICE_PORT);
    let actual: PeerMessage = peer_codec().default_decode(packet.bytes.clone()).unwrap();
    assert_eq!(actual.trace_id, value.trace_id.to_bytes());
    assert_eq!(actual.source, source.as_bytes());
    assert_eq!(actual.expires_at_100ns, ticks(value.expires_at));
}

/// 仅测试宿主的接管/观察，不实现编解码、选择、准入、重试或任何产品状态。
/// outgoing属于宿主，不据其存在声称本库有后台队列；没有QUIC/IPC网络证明。
struct Wire<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>> {
    local: ServiceHost,
    outgoing: Mutex<VecDeque<Packet>>,
    history: Mutex<Vec<Packet>>,
    failure: Mutex<Option<ServiceError>>,
    immediate: Mutex<Option<ServiceRuntime<C>>>,
    completed: AtomicUsize,
}
impl<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>> ServiceTransport<C> for Wire<C> {
    fn local_host(&self) -> ServiceHost {
        self.local.clone()
    }
    fn send(&self, address: ServiceAddress, bytes: Vec<u8>) -> Result<(), ServiceError> {
        let packet = Packet { address, bytes };
        self.history.lock().unwrap().push(packet.clone());
        let immediate = self.immediate.lock().unwrap().clone();
        if let Some(runtime) = immediate {
            // 宿主选择同步交付，只poll真实生产Future；不在这里调用typed或记录器。
            ready(runtime.on_send(packet.address.port, packet.bytes, None))?;
            self.completed.fetch_add(1, Ordering::SeqCst);
        } else {
            self.outgoing.lock().unwrap().push_back(packet);
        }
        // 有外部接管之后再返回Err，保护“提交失败不能推定外部零效果”。
        match self.failure.lock().unwrap().take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
    fn rpc(
        &self,
        _address: ServiceAddress,
        _input: Vec<u8>,
        _timeout: Option<Duration>,
    ) -> futures::future::BoxFuture<'static, Result<Vec<u8>, ServiceError>> {
        panic!("本库不能增加RPC")
    }
    fn reconnect(
        &self,
        _target: ServiceHost,
    ) -> futures::future::BoxFuture<'static, Result<(), ServiceError>> {
        panic!("本库不能自行重连")
    }
    fn close_connection(
        &self,
        _target: ServiceHost,
    ) -> futures::future::BoxFuture<'static, Result<(), ServiceError>> {
        panic!("本库不能关闭共享连接")
    }
    fn is_connected(&self, _target: ServiceHost) -> bool {
        panic!("本库不能预探测目标")
    }
}
struct Host<C: ServiceCodec> {
    terminal: ServiceTerminal<C>,
    pause: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
impl<C: ServiceCodec> Deref for Host<C> {
    type Target = ServiceTerminal<C>;
    fn deref(&self) -> &Self::Target {
        let pause = self.pause.lock().unwrap().take();
        if let Some(pause) = pause {
            pause();
        }
        &self.terminal
    }
}
struct Environment<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>> {
    runtime: ServiceRuntime<C>,
    terminal: Arc<Host<C>>,
    wire: Arc<Wire<C>>,
    tasks: Arc<Mutex<VecDeque<Task>>>,
}
impl<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>> Environment<C> {
    fn new(id: &str, codec: C) -> Self {
        let tasks = Arc::new(Mutex::new(VecDeque::new()));
        let accepted = tasks.clone();
        let submitter = TaskSubmitter::from_fn(move |task| {
            accepted.lock().unwrap().push_back(task);
            Ok(())
        });
        let runtime = ServiceRuntime::new(
            ServiceContexts::new(),
            codec,
            Some(submitter),
            Default::default(),
            host(id),
            None,
        );
        let wire = Arc::new(Wire {
            local: host(id),
            outgoing: Mutex::new(VecDeque::new()),
            history: Mutex::new(Vec::new()),
            failure: Mutex::new(None),
            immediate: Mutex::new(None),
            completed: AtomicUsize::new(0),
        });
        let terminal = Arc::new(Host {
            terminal: runtime.terminal(wire.clone()),
            pause: Mutex::new(None),
        });
        Self {
            runtime,
            terminal,
            wire,
            tasks,
        }
    }
    fn drain_local(&self) {
        loop {
            let task = self.tasks.lock().unwrap().pop_front();
            match task {
                Some(task) => ready(task),
                None => break,
            }
        }
    }
    fn deliver(&self, bytes: Vec<u8>) {
        ready(
            self.runtime
                .on_send(COORDINATION_SERVICE_PORT.into(), bytes, None),
        )
        .unwrap();
    }
    fn packets(&self) -> Vec<Packet> {
        self.wire.history.lock().unwrap().clone()
    }
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
fn capture(action: impl FnOnce()) -> Vec<Warning> {
    let observer = Observer::default();
    tracing::subscriber::with_default(observer.clone(), action);
    let result = observer.0.lock().unwrap().clone();
    result
}
fn assert_warning(events: &[Warning], stage: &str, reason: &str) {
    assert_eq!(events.len(), 1, "拒绝必须只尝试一次本库日志");
    let event = &events[0];
    assert_eq!(event.target, "pi_tracing_coordinator::service");
    assert_eq!(event.level, Level::WARN);
    assert_eq!(
        event.fields.get("service_port").map(String::as_str),
        Some("4091")
    );
    assert_eq!(event.fields.get("stage").map(String::as_str), Some(stage));
    assert_eq!(event.fields.get("error").map(String::as_str), Some(reason));
    for forbidden in ["trace", "trace_id", "source", "operation_id"] {
        assert!(!event.fields.contains_key(forbidden));
    }
}

#[derive(Debug)]
struct Returned {
    remote: TraceNodeId,
    value: TraceSelection,
    outcome: Result<(), String>,
}
struct ObservedNotifier {
    delegate: Arc<dyn TraceNotifier>,
    returned: mpsc::SyncSender<Returned>,
}
impl TraceNotifier for ObservedNotifier {
    fn notify(&self, remote: &TraceNodeId, value: TraceSelection) -> anyhow::Result<()> {
        let result = self.delegate.notify(remote, value);
        self.returned
            .try_send(Returned {
                remote: remote.clone(),
                value,
                outcome: result.as_ref().map(|_| ()).map_err(ToString::to_string),
            })
            .expect("观测通道必须存活且不溢出");
        result
    }
}
#[derive(Debug)]
struct Exporter(Arc<AtomicUsize>);
impl SpanExporter for Exporter {
    async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct Recorder {
    recorder: FlightRecorder,
    guard: Option<FlightRecorderGuard>,
    returned: mpsc::Receiver<Returned>,
    exports: Arc<AtomicUsize>,
}
impl Recorder {
    fn new(local: &str, notifier: Arc<dyn TraceNotifier>) -> Self {
        let (send, returned) = mpsc::sync_channel(32);
        let notifier = Arc::new(ObservedNotifier {
            delegate: notifier,
            returned: send,
        });
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
            returned,
            exports,
        }
    }
    fn selected(&self, count: u64) {
        wait_for(
            || self.recorder.stats().selected_traces == count,
            "真实Worker选择效果",
        );
    }
    fn callback(&self, remote: &str, value: TraceSelection) {
        let result = self
            .returned
            .recv_timeout(WAIT)
            .expect("真实通知回调应在截止内返回");
        assert_eq!(result.remote.as_str(), remote);
        assert_eq!(result.value.trace_id, value.trace_id);
        assert_eq!(result.value.expires_at, value.expires_at);
        assert!(result.outcome.is_ok(), "{result:?}");
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

struct Pair<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>> {
    sender: Environment<C>,
    sender_coordinator: Coordinator<C>,
    receiver: Environment<C>,
    receiver_coordinator: Coordinator<C>,
    recorder: Recorder,
}
fn pair<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    codec: fn() -> C,
    construct: Constructor<C>,
    local: &str,
) -> Pair<C> {
    let sender = Environment::new(A, codec());
    let sender_coordinator =
        construct(&sender.terminal, &sender.runtime, TraceNodeId::new(A)).unwrap();
    let receiver = Environment::new(local, codec());
    let receiver_coordinator = construct(
        &receiver.terminal,
        &receiver.runtime,
        TraceNodeId::new(local),
    )
    .unwrap();
    let recorder = Recorder::new(local, receiver_coordinator.notifier());
    receiver_coordinator
        .bind_recorder(Some(recorder.recorder.clone()))
        .unwrap();
    // 仅接收环境安装唯一static，发送环境不安装第二个4091或另一codec适配。
    receiver_coordinator
        .install_service(&receiver.runtime)
        .unwrap();
    Pair {
        sender,
        sender_coordinator,
        receiver,
        receiver_coordinator,
        recorder,
    }
}
fn default_pair() -> Pair<DefaultCodec> {
    pair(default_codec, TracingCoordinator::new_default, B)
}
fn multi_pair() -> Pair<MultiLayerCodec> {
    pair(multi_codec, TracingCoordinator::new_multi_layer, B)
}
fn transfer<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(p: &Pair<C>) -> Vec<Warning> {
    let packets: Vec<Packet> = p.sender.wire.outgoing.lock().unwrap().drain(..).collect();
    capture(|| {
        for packet in packets {
            assert_eq!(packet.address.host, p.receiver.wire.local);
            assert_eq!(u16::from(packet.address.port), 4091);
            p.receiver.deliver(packet.bytes);
        }
        p.recorder.recorder.seal_current_thread();
    })
}
fn submit<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    p: &Pair<C>,
    value: TraceSelection,
    source: &str,
) {
    let remote = match &p.receiver.wire.local {
        ServiceHost::Remote(peer) => TraceNodeId::new(peer.to_string()),
        _ => panic!("固定远端环境"),
    };
    p.sender_coordinator
        .send_selection(&remote, value, &TraceNodeId::new(source))
        .unwrap();
}
/// 只作有限观察：有效控制须先证明同一真实装配活跃，不能用瞬时空队列证明全局完成。
fn quiet<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    p: &Pair<C>,
    selected: u64,
    outgoing: usize,
) {
    let start = Instant::now();
    loop {
        assert_eq!(p.recorder.recorder.stats().selected_traces, selected);
        assert_eq!(p.receiver.packets().len(), outgoing);
        assert!(matches!(
            p.recorder.returned.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        if start.elapsed() >= QUIET {
            break;
        }
        thread::sleep(Duration::from_millis(2));
    }
}

/// 双环境是两个合法宿主边界，不是两个已证明的网络参与进程。
fn complete_chain<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(mut p: Pair<C>) {
    let value = selection(0x21);
    assert!(p
        .recorder
        .recorder
        .record_peer(value.trace_id, TraceNodeId::new(A)));
    assert!(p
        .recorder
        .recorder
        .record_peer(value.trace_id, TraceNodeId::new(C)));
    p.recorder.recorder.seal_current_thread();
    submit(&p, value, A);
    let packets = p.sender.packets();
    assert_eq!(packets.len(), 1);
    assert_packet(&packets[0], B, value, A);
    assert_eq!(
        p.recorder.recorder.stats().selected_traces,
        0,
        "此宿主尚未交付字节，不能将本地受理当远端完成"
    );
    assert_eq!(p.recorder.recorder.stats().notification_attempts, 0);
    assert!(transfer(&p).is_empty());
    p.recorder.callback(C, value);
    p.recorder.selected(1);
    let forwarded = p.receiver.packets();
    assert_eq!(forwarded.len(), 1);
    assert_packet(&forwarded[0], C, value, B);
    // 真回调已返回，统计稍后由通知线程发布；不将开始计数代替回调返回。
    wait_for(
        || p.recorder.recorder.stats().notification_attempts == 1,
        "通知统计",
    );
    quiet(&p, 1, 1);
    p.recorder.stop();
}
pub(super) fn test_final_default_full_chain_separates_completion() {
    isolated(|| complete_chain(default_pair()));
}
pub(super) fn test_final_multi_full_chain_separates_completion() {
    isolated(|| complete_chain(multi_pair()));
}

pub(super) fn test_final_no_designated_call_has_no_notification() {
    isolated(|| {
        let p = default_pair();
        let value = selection(0x22);
        quiet(&p, 0, 0);
        assert!(p.sender.packets().is_empty());
        // 关联登记由真实宿主主动执行；登记本身不能让本库选择Trace或自动通知。
        assert!(p
            .recorder
            .recorder
            .record_peer(value.trace_id, TraceNodeId::new(C)));
        p.recorder.recorder.seal_current_thread();
        quiet(&p, 0, 0);
        assert!(p.sender.packets().is_empty());
        submit(&p, value, A);
        assert!(transfer(&p).is_empty());
        p.recorder.callback(C, value);
        p.recorder.selected(1);
        assert_eq!(p.sender.packets().len(), 1);
        quiet(&p, 1, 1);
    });
}

pub(super) fn test_final_receive_may_finish_before_send_returns() {
    isolated(|| {
        let p = default_pair();
        let value = selection(0x23);
        *p.sender.wire.immediate.lock().unwrap() = Some(p.receiver.runtime.clone());
        assert!(capture(|| submit(&p, value, A)).is_empty());
        assert_eq!(
            p.sender.wire.completed.load(Ordering::SeqCst),
            1,
            "真实宏已在宿主send栈内被poll完"
        );
        assert!(p.sender.wire.outgoing.lock().unwrap().is_empty());
        p.recorder.recorder.seal_current_thread();
        p.recorder.selected(1);
        *p.sender.wire.immediate.lock().unwrap() = None;
    });
}

pub(super) fn test_final_repeated_calls_are_not_cross_call_exactly_once() {
    isolated(|| {
        let p = default_pair();
        let value = selection(0x24);
        submit(&p, value, A);
        submit(&p, value, A);
        let packets = p.sender.packets();
        assert_eq!(packets.len(), 2);
        assert_packet(&packets[0], B, value, A);
        assert_packet(&packets[1], B, value, A);
        assert!(transfer(&p).is_empty());
        p.recorder.selected(1);
        quiet(&p, 1, 0);
    });
}

/// R2：阶段与直接原因联合判定；原因内的任意阶段词不能代替外层Submit。
/// 完整原因按真实ServiceError显示值核对，不接受Encode、混合阶段或缺失/不同原因。
fn submit_failure_matches(
    text: &str,
    cause: Option<&(dyn Error + 'static)>,
    expected_reason: &str,
) -> bool {
    let Some(cause) = cause else {
        return false;
    };
    matches!(cause.downcast_ref::<ServiceError>(),
        Some(ServiceError::RuntimeError(reason)) if reason == expected_reason)
        && text == format!("发送提交失败：{cause}")
}

pub(super) fn test_final_submit_error_preserves_taken_bytes_without_retry() {
    isolated(|| {
        let p = default_pair();
        let value = selection(0x25);
        *p.sender.wire.failure.lock().unwrap() = Some(ServiceError::RuntimeError(
            "宿主接管字节后的真实拒绝".into(),
        ));
        let error = p
            .sender_coordinator
            .send_selection(&TraceNodeId::new(B), value, &TraceNodeId::new(A))
            .unwrap_err();
        let expected_reason = "宿主接管字节后的真实拒绝";
        let cause = error.source().expect("必须借回真实直接ServiceError");
        assert!(submit_failure_matches(
            &error.to_string(),
            Some(cause),
            expected_reason,
        ));
        // 同一真实原因不变，只将外层阶段错标为Encode；旧断言会误接受。
        assert!(
            !submit_failure_matches(
                &format!("发送编码失败：{cause}"),
                Some(cause),
                expected_reason,
            ),
            "R2反例：同一直接原因不能把Encode冒充Submit",
        );
        for wrong_text in [
            format!("发送编码失败：{cause}；发送提交失败"),
            format!("发送提交成功：{cause}"),
            "发送提交失败：不同原因".to_owned(),
        ] {
            assert!(!submit_failure_matches(
                &wrong_text,
                Some(cause),
                expected_reason
            ));
        }
        assert!(!submit_failure_matches(
            &error.to_string(),
            None,
            expected_reason
        ));
        let different = ServiceError::RuntimeError("其它拒绝".into());
        assert!(!submit_failure_matches(
            &error.to_string(),
            Some(&different),
            expected_reason,
        ));
        let wrong_kind = ServiceError::CodecError(expected_reason.into());
        assert!(!submit_failure_matches(
            &format!("发送提交失败：{wrong_kind}"),
            Some(&wrong_kind),
            expected_reason,
        ));
        assert_eq!(p.sender.packets().len(), 1);
        assert_packet(&p.sender.packets()[0], B, value, A);
        // 外部已接管的同一字节仍能被真实接收链处理，Err不等于外部零效果。
        assert!(transfer(&p).is_empty());
        p.recorder.selected(1);
        quiet(&p, 1, 0);
    });
}

pub(super) fn test_final_send_preconditions_have_zero_submission() {
    isolated(|| {
        let p = default_pair();
        let value = selection(0x26);
        let bad_target = TraceNodeId::new("不是PeerId");
        let invalid = TraceSelection {
            trace_id: TraceId::INVALID,
            ..value
        };
        let target_error = p
            .sender_coordinator
            .send_selection(&bad_target, invalid, &TraceNodeId::new(""))
            .unwrap_err();
        assert!(target_error.to_string().starts_with("发送目标解析失败："));
        assert!(target_error.source().is_some());
        let trace_error = p
            .sender_coordinator
            .send_selection(&TraceNodeId::new(B), invalid, &TraceNodeId::new(""))
            .unwrap_err();
        assert_eq!(
            trace_error.to_string(),
            "发送协议业务转换失败：TraceId不能全零"
        );
        let source_error = p
            .sender_coordinator
            .send_selection(&TraceNodeId::new(B), value, &TraceNodeId::new(""))
            .unwrap_err();
        assert_eq!(
            source_error.to_string(),
            "发送协议业务转换失败：身份字段source不能为空"
        );
        assert!(p.sender.packets().is_empty());
        // 公开协调器要求登记发布后的稳定表；不以违反构造前提的缺表装配制造Encode失败。
        // Encode普通Err的公开可达性保留给独立源码/宿主证据，不热换表或伪造codec。
        // 终端弱失效优先于坏目标；与关闭的两个环境各自独立。
        let expired = Environment::new(A, default_codec());
        let gone = TracingCoordinator::new_default(
            &expired.terminal,
            &expired.runtime,
            TraceNodeId::new(A),
        )
        .unwrap();
        let Environment {
            terminal,
            wire,
            runtime: _,
            tasks: _,
        } = expired;
        drop(terminal);
        let error = gone
            .send_selection(&bad_target, invalid, &TraceNodeId::new(""))
            .unwrap_err();
        assert!(error.source().is_none());
        assert!(error.to_string().contains("终端不可用"));
        assert!(wire.history.lock().unwrap().is_empty());
        p.sender_coordinator.close();
        let error = p
            .sender_coordinator
            .send_selection(&bad_target, invalid, &TraceNodeId::new(""))
            .unwrap_err();
        assert_eq!(error.source().unwrap().to_string(), "协调绑定已关闭");
        assert!(p.sender.packets().is_empty());
        // 独立有效公开发送是观察器负对照，而非降低失败断言。
        p.receiver_coordinator
            .send_selection(&TraceNodeId::new(A), value, &TraceNodeId::new(B))
            .unwrap();
        assert_eq!(p.receiver.packets().len(), 1);
        assert_packet(&p.receiver.packets()[0], A, value, B);
        assert_eq!(p.recorder.recorder.stats().selected_traces, 0);
    });
}

fn decode_rejects<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(p: Pair<C>) {
    let actual = peer_codec()
        .default_decode::<PeerMessage>(Vec::new())
        .unwrap_err();
    assert!(matches!(actual, ServiceError::CodecError(_)));
    let events = capture(|| p.receiver.deliver(Vec::new()));
    assert!(events.is_empty(), "前置解码拒绝不能进入typed日志");
    assert_eq!(p.recorder.recorder.stats().selected_traces, 0);
    let value = selection(0x31);
    submit(&p, value, A);
    assert!(transfer(&p).is_empty());
    p.recorder.selected(1);
    quiet(&p, 1, 0);
}
pub(super) fn test_final_default_decode_rejection_stays_before_business() {
    isolated(|| decode_rejects(default_pair()));
}
pub(super) fn test_final_multi_decode_rejection_stays_before_business() {
    isolated(|| decode_rejects(multi_pair()));
}

fn business_rejects<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(p: Pair<C>) {
    let value = selection(0x32);
    let count = ticks(value.expires_at);
    let invalid_source = vec![0xff];
    let invalid_utf8 = std::str::from_utf8(&invalid_source).unwrap_err();
    let inputs = [
        (
            [0; 16],
            A.as_bytes().to_vec(),
            count,
            "TraceId不能全零".to_owned(),
        ),
        (
            [0x33; 16],
            invalid_source,
            count,
            format!("身份字段source不是合法UTF-8：{invalid_utf8}"),
        ),
        (
            [0x34; 16],
            Vec::new(),
            count,
            "身份字段source不能为空".to_owned(),
        ),
        (
            [0x35; 16],
            A.as_bytes().to_vec(),
            -116_444_736_000_000_001,
            "期限超出共同100ns计数范围".to_owned(),
        ),
        (
            [0x36; 16],
            A.as_bytes().to_vec(),
            9_106_927_300_854_775_808,
            "期限超出共同100ns计数范围".to_owned(),
        ),
    ];
    for (trace, source, deadline, reason) in inputs {
        let bytes = peer_bytes(trace, &source, deadline);
        let events = capture(|| p.receiver.deliver(bytes));
        assert_warning(&events, "validate", &reason);
        assert_eq!(p.recorder.recorder.stats().selected_traces, 0);
    }
    submit(&p, value, A);
    assert!(transfer(&p).is_empty());
    p.recorder.selected(1);
    quiet(&p, 1, 0);
}
pub(super) fn test_final_default_invalid_business_never_publishes() {
    isolated(|| business_rejects(default_pair()));
}
pub(super) fn test_final_multi_invalid_business_never_publishes() {
    isolated(|| business_rejects(multi_pair()));
}

fn receiver_rejects<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    codec: fn() -> C,
    construct: Constructor<C>,
    mode: &str,
) {
    let sender = Environment::new(A, codec());
    let s = construct(&sender.terminal, &sender.runtime, TraceNodeId::new(A)).unwrap();
    let receiver = Environment::new(B, codec());
    let r = construct(&receiver.terminal, &receiver.runtime, TraceNodeId::new(B)).unwrap();
    let mut recorder = Recorder::new(B, r.notifier());
    match mode {
        "none" => r.bind_recorder(None).unwrap(),
        "closed" => r.bind_recorder(Some(recorder.recorder.clone())).unwrap(),
        "unbound" => {}
        _ => panic!("固定场景"),
    }
    r.install_service(&receiver.runtime).unwrap();
    if mode == "closed" {
        r.close();
    }
    let value = selection(0x37);
    s.send_selection(&TraceNodeId::new(B), value, &TraceNodeId::new(A))
        .unwrap();
    let packet = sender.wire.outgoing.lock().unwrap().pop_front().unwrap();
    let events = capture(|| receiver.deliver(packet.bytes));
    assert_warning(
        &events,
        "submit",
        if mode == "closed" {
            "协调绑定已关闭"
        } else {
            "记录器不可用"
        },
    );
    assert_eq!(recorder.recorder.stats().selected_traces, 0);
    // 真实宿主记录器的独立合法出口仍可用，证明不是已死夹具掩盖误放行。
    assert!(recorder
        .recorder
        .apply_remote_selection(selection(0x38), TraceNodeId::new(A)));
    recorder.recorder.seal_current_thread();
    recorder.selected(1);
    if mode != "closed" && mode != "none" {
        r.bind_recorder(Some(recorder.recorder.clone())).unwrap();
        let control = selection(0x39);
        assert!(capture(|| receiver.deliver(peer_bytes(
            control.trace_id.to_bytes(),
            A.as_bytes(),
            ticks(control.expires_at)
        )))
        .is_empty());
        recorder.recorder.seal_current_thread();
        recorder.selected(2);
    }
    let expected = if mode == "unbound" { 2 } else { 1 };
    let start = Instant::now();
    while start.elapsed() < QUIET {
        assert_eq!(recorder.recorder.stats().selected_traces, expected);
        assert!(receiver.packets().is_empty());
        thread::sleep(Duration::from_millis(2));
    }
    recorder.stop();
    assert_eq!(recorder.recorder.stats().selected_traces, expected);
    assert!(receiver.packets().is_empty());
}
pub(super) fn test_final_default_unbound_receiver_has_zero_publication() {
    isolated(|| receiver_rejects(default_codec, TracingCoordinator::new_default, "unbound"));
}
pub(super) fn test_final_multi_unbound_receiver_has_zero_publication() {
    isolated(|| receiver_rejects(multi_codec, TracingCoordinator::new_multi_layer, "unbound"));
}
pub(super) fn test_final_default_none_receiver_has_zero_publication() {
    isolated(|| receiver_rejects(default_codec, TracingCoordinator::new_default, "none"));
}
pub(super) fn test_final_multi_none_receiver_has_zero_publication() {
    isolated(|| receiver_rejects(multi_codec, TracingCoordinator::new_multi_layer, "none"));
}
pub(super) fn test_final_default_closed_receiver_has_zero_publication() {
    isolated(|| receiver_rejects(default_codec, TracingCoordinator::new_default, "closed"));
}
pub(super) fn test_final_multi_closed_receiver_has_zero_publication() {
    isolated(|| receiver_rejects(multi_codec, TracingCoordinator::new_multi_layer, "closed"));
}

fn expired_does_not_propagate<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(p: Pair<C>) {
    let live = selection(0x41);
    let expired = TraceSelection {
        trace_id: TraceId::from_bytes([0x42; 16]),
        expires_at: UNIX_EPOCH,
    };
    for trace in [expired.trace_id, live.trace_id] {
        assert!(p.recorder.recorder.record_peer(trace, TraceNodeId::new(C)));
    }
    p.recorder.recorder.seal_current_thread();
    // 已过期仍由本库成功转换/编码并恰一次移交；只让真实记录器判断时效。
    submit(&p, expired, A);
    assert_packet(&p.sender.packets()[0], B, expired, A);
    assert_warning(&transfer(&p), "submit", "记录器事件未发布");
    quiet(&p, 0, 0);
    submit(&p, live, A);
    assert!(transfer(&p).is_empty());
    p.recorder.callback(C, live);
    p.recorder.selected(1);
    quiet(&p, 1, 1);
    assert_eq!(p.sender.packets().len(), 2);
    assert_packet(&p.receiver.packets()[0], C, live, B);
}
pub(super) fn test_final_default_expired_decision_does_not_propagate() {
    isolated(|| expired_does_not_propagate(default_pair()));
}
pub(super) fn test_final_multi_expired_decision_does_not_propagate() {
    isolated(|| expired_does_not_propagate(multi_pair()));
}

fn not_published_is_not_guessed<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    mut p: Pair<C>,
) {
    let value = selection(0x43);
    // self来源和已停Guard是两种不同真实false输入；库层仅报告同一个已知结果。
    submit(&p, value, B);
    assert_warning(&transfer(&p), "submit", "记录器事件未发布");
    quiet(&p, 0, 0);
    submit(&p, selection(0x45), A);
    assert!(transfer(&p).is_empty());
    p.recorder.selected(1);
    p.recorder.stop();
    submit(&p, selection(0x44), A);
    assert_warning(&transfer(&p), "submit", "记录器事件未发布");
    assert_eq!(p.sender.packets().len(), 3);
    assert_eq!(p.recorder.recorder.stats().selected_traces, 1);
    assert!(p.receiver.packets().is_empty());
}
pub(super) fn test_final_default_unpublished_keeps_only_real_classification() {
    isolated(|| not_published_is_not_guessed(default_pair()));
}
pub(super) fn test_final_multi_unpublished_keeps_only_real_classification() {
    isolated(|| not_published_is_not_guessed(multi_pair()));
}

/// 其它宿主服务只验证共享runtime/terminal仍能处理独立业务，不模拟协调业务。
struct Calls(AtomicUsize);
struct OtherService<C>(PhantomData<fn() -> C>);
impl<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>> Service for OtherService<C> {
    type Codec = C;
    fn id(&self) -> ServiceId {
        "svc://local/slice-final/host".into()
    }
    fn port(&self) -> ServicePort {
        4090_u16.into()
    }
    fn layer(&self) -> &'static str {
        "slice-final-host"
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
            .get_only_read::<Calls>()
            .map(|calls| calls as Arc<dyn Any + Send + Sync>)
            .ok_or_else(|| ServiceError::BindOnlyReadNotFound(4090, "calls".into(), "Calls".into()))
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
        input: Vec<u8>,
    ) -> futures::future::BoxFuture<'static, Result<(), ServiceError>> {
        Box::pin(async move {
            assert_eq!(input, [9, 8, 7]);
            params
                .downcast::<Calls>()
                .map_err(|_| ServiceError::DowncastParamsFailed(4090))?
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
        panic!("独立宿主服务不使用RPC")
    }
}
static OTHER_DEFAULT: OtherService<DefaultCodec> = OtherService(PhantomData);
static OTHER_MULTI: OtherService<MultiLayerCodec> = OtherService(PhantomData);
fn check_shared<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    p: Pair<C>,
    service: &'static dyn Service<Codec = C>,
) {
    let calls = Arc::new(Calls(AtomicUsize::new(0)));
    p.receiver
        .runtime
        .contexts()
        .insert_only_read(calls.clone());
    p.receiver.runtime.registry().register(service).unwrap();
    p.receiver
        .runtime
        .registry()
        .init(&p.receiver.runtime, service.port())
        .unwrap();
    let send_other = || {
        p.receiver
            .terminal
            .terminal
            .send(
                ServiceAddress::new(host(B), 4090_u16.into()),
                vec![9, 8, 7],
                None,
            )
            .unwrap();
        p.receiver.drain_local();
    };
    send_other();
    assert_eq!(calls.0.load(Ordering::SeqCst), 1);
    // 已提交字节不撤回；这里先让宿主接管，再关闭接收owner，随后真实接收准入拒绝。
    submit(&p, selection(0x51), A);
    p.receiver_coordinator.close();
    p.receiver_coordinator.close();
    assert_warning(&transfer(&p), "submit", "协调绑定已关闭");
    assert!(matches!(
        p.receiver
            .runtime
            .get_status(COORDINATION_SERVICE_PORT.into()),
        Some(ServiceStatus::Running)
    ));
    assert!(matches!(
        p.receiver.runtime.get_status(4090_u16.into()),
        Some(ServiceStatus::Running)
    ));
    assert_eq!(
        p.receiver
            .runtime
            .get_meta(COORDINATION_SERVICE_PORT.into())
            .unwrap()
            .layer,
        COORDINATION_SERVICE_LAYER
    );
    let error = p
        .receiver_coordinator
        .send_selection(&TraceNodeId::new(A), selection(0x52), &TraceNodeId::new(B))
        .unwrap_err();
    assert_eq!(error.source().unwrap().to_string(), "协调绑定已关闭");
    assert!(p.receiver.packets().is_empty());
    send_other();
    assert_eq!(calls.0.load(Ordering::SeqCst), 2);
    // close没有停止共享Guard/记录器；直接宿主能力仍可发布不同Trace。
    assert!(p
        .recorder
        .recorder
        .apply_remote_selection(selection(0x53), TraceNodeId::new(A)));
    p.recorder.recorder.seal_current_thread();
    p.recorder.selected(1);
    quiet(&p, 1, 0);
}
pub(super) fn test_final_default_close_preserves_other_host_business() {
    isolated(|| check_shared(default_pair(), &OTHER_DEFAULT));
}
pub(super) fn test_final_multi_close_preserves_other_host_business() {
    isolated(|| check_shared(multi_pair(), &OTHER_MULTI));
}

fn admitted_send<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(p: Pair<C>) {
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    *p.sender.terminal.pause.lock().unwrap() = Some(Box::new(move || {
        entered_tx.send(()).unwrap();
        release_rx.recv_timeout(WAIT).expect("准入后暂停必须有截止");
    }));
    let notifier = p.sender_coordinator.notifier();
    let running = notifier.clone();
    let value = selection(0x54);
    let worker = thread::spawn(move || running.notify(&TraceNodeId::new(B), value));
    entered_rx
        .recv_timeout(WAIT)
        .expect("必须实际进入准入后的宿主Deref");
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let owner = p.sender_coordinator;
    let closer = thread::spawn(move || {
        owner.close();
        done_tx.send(()).unwrap();
        owner
    });
    done_rx.recv_timeout(WAIT).expect("close不应等待已准入者");
    assert!(p.sender.packets().is_empty());
    release_tx.send(()).unwrap();
    worker.join().unwrap().unwrap();
    let owner = closer.join().unwrap();
    assert_eq!(p.sender.packets().len(), 1);
    assert_packet(&p.sender.packets()[0], B, value, A);
    let packet = p.sender.wire.outgoing.lock().unwrap().pop_front().unwrap();
    assert!(capture(|| p.receiver.deliver(packet.bytes)).is_empty());
    p.recorder.recorder.seal_current_thread();
    p.recorder.selected(1);
    let error = notifier
        .notify(&TraceNodeId::new(B), selection(0x55))
        .unwrap_err();
    let error = error.downcast_ref::<CoordinationSendError>().unwrap();
    assert_eq!(error.source().unwrap().to_string(), "协调绑定已关闭");
    assert_eq!(p.sender.packets().len(), 1);
    drop(owner);
}
pub(super) fn test_final_default_admitted_send_finishes_after_close() {
    isolated(|| admitted_send(default_pair()));
}
pub(super) fn test_final_multi_admitted_send_finishes_after_close() {
    isolated(|| admitted_send(multi_pair()));
}

pub(super) fn test_final_explicit_source_is_preserved_without_normalizing_identity() {
    isolated(|| {
        let mut p = default_pair();
        let value = selection(0x56);
        let source = "  本跳é/e\u{301}🦀\0 ";
        // 记录器合法接受原始非空身份；它与下游C都真实登记，原始来源必须被排除。
        // 若接收端trim/规范化/改用local，原始邻居不在contacted中，会多出失败回调。
        for remote in [source, C] {
            assert!(p
                .recorder
                .recorder
                .record_peer(value.trace_id, TraceNodeId::new(remote)));
        }
        p.recorder.recorder.seal_current_thread();
        submit(&p, value, source);
        assert_eq!(p.sender.packets().len(), 1);
        assert_packet(&p.sender.packets()[0], B, value, source);
        assert!(transfer(&p).is_empty());
        p.recorder.callback(C, value);
        p.recorder.selected(1);
        wait_for(
            || p.recorder.recorder.stats().notification_attempts == 1,
            "原始来源排除后的真实通知",
        );
        assert_eq!(p.recorder.recorder.stats().failed_notifications, 0);
        assert_eq!(p.receiver.packets().len(), 1);
        assert_packet(&p.receiver.packets()[0], C, value, B);
        quiet(&p, 1, 1);

        // 不同Trace复用相同两邻居，但改由C发来：原始身份此时不是来源，必须尝试通知。
        // 该身份不是PeerId，真实生产notifier应返回目标解析错误；不能让Wire伪造成功。
        let control = selection(0x59);
        for remote in [source, C] {
            assert!(p
                .recorder
                .recorder
                .record_peer(control.trace_id, TraceNodeId::new(remote)));
        }
        p.recorder.recorder.seal_current_thread();
        submit(&p, control, C);
        assert!(transfer(&p).is_empty());
        let returned = p
            .recorder
            .returned
            .recv_timeout(WAIT)
            .expect("原始邻居对照必须实际返回");
        assert_eq!(returned.remote.as_str(), source);
        assert_eq!(returned.value.trace_id, control.trace_id);
        assert_eq!(returned.value.expires_at, control.expires_at);
        assert!(
            matches!(returned.outcome, Err(ref error) if error.starts_with("发送目标解析失败："))
        );
        p.recorder.selected(2);
        wait_for(
            || p.recorder.recorder.stats().notification_attempts == 2,
            "来源反转对照通知",
        );
        wait_for(
            || p.recorder.recorder.stats().failed_notifications == 1,
            "原始身份解析错误统计",
        );
        assert_eq!(p.sender.packets().len(), 2);
        assert_packet(&p.sender.packets()[1], B, control, C);
        assert_eq!(p.receiver.packets().len(), 1, "解析失败不能额外提交终端");
        quiet(&p, 2, 1);
        p.recorder.stop();
        assert_eq!(p.recorder.recorder.stats().selected_traces, 2);
        assert_eq!(p.recorder.recorder.stats().notification_attempts, 2);
        assert_eq!(p.recorder.recorder.stats().failed_notifications, 1);
    });
}

#[cfg(target_os = "linux")]
pub(super) fn test_final_linux_normalizes_once_and_preserves_deadline() {
    isolated(|| {
        let p = default_pair();
        let mut value = selection(0x57);
        value.expires_at += Duration::from_nanos(123_456_789);
        let expected = TraceSelection {
            expires_at: value.expires_at - Duration::from_nanos(89),
            ..value
        };
        assert!(p
            .recorder
            .recorder
            .record_peer(value.trace_id, TraceNodeId::new(C)));
        p.recorder.recorder.seal_current_thread();
        submit(&p, value, A);
        assert_packet(&p.sender.packets()[0], B, expected, A);
        assert!(transfer(&p).is_empty());
        p.recorder.callback(C, expected);
        p.recorder.selected(1);
        assert_packet(&p.receiver.packets()[0], C, expected, B);
        quiet(&p, 1, 1);
    });
}

pub(super) fn test_final_aligned_deadline_survives_each_boundary() {
    isolated(|| {
        let p = multi_pair();
        let mut value = selection(0x58);
        value.expires_at += Duration::from_nanos(123_456_700);
        assert!(p
            .recorder
            .recorder
            .record_peer(value.trace_id, TraceNodeId::new(C)));
        p.recorder.recorder.seal_current_thread();
        submit(&p, value, A);
        assert_packet(&p.sender.packets()[0], B, value, A);
        assert!(transfer(&p).is_empty());
        p.recorder.callback(C, value);
        p.recorder.selected(1);
        assert_packet(&p.receiver.packets()[0], C, value, B);
        quiet(&p, 1, 1);
    });
}

/// 冷启动使用同一父测试依次启动并wait旧/新进程；没有在同一进程重造对象冒充重启。
/// 此处证明协调器/真实记录器内存不恢复，宿主Wire不是固定证书或网络会话重连证明。
fn restart(variant: &str) {
    let name = thread::current().name().unwrap().to_owned();
    if std::env::var(CHILD).ok().as_deref() != Some(name.as_str()) {
        let old = child(&name, "old");
        assert!(old.contains("FINAL_OLD_PROCESS_CLOSED"));
        // child返回已wait退出；新OS进程之后才启动，固定或变化合法PeerId由父场景决定。
        let new = child(&name, variant);
        assert!(new.contains("FINAL_NEW_PROCESS_CHECKED"));
        return;
    }
    let panics = panic_probe();
    let phase = std::env::var(PHASE).unwrap();
    if phase == "old" {
        check_shutdown_panic_observer(&panics);
    }
    let local = if phase == "changed" { D } else { B };
    let mut p = pair(default_codec, TracingCoordinator::new_default, local);
    // 绝对期限由固定合法未来秒构成；旧通知若被重放仍未过期，不能用时效遮蔽恢复缺陷。
    let old = TraceSelection {
        trace_id: TraceId::from_bytes([0x61; 16]),
        expires_at: UNIX_EPOCH + Duration::from_secs(2_000_000_000),
    };
    let neighbor_only = TraceId::from_bytes([0x62; 16]);
    if phase == "old" {
        assert!(p
            .recorder
            .recorder
            .record_peer(old.trace_id, TraceNodeId::new(C)));
        assert!(p
            .recorder
            .recorder
            .record_peer(neighbor_only, TraceNodeId::new(C)));
        p.recorder.recorder.seal_current_thread();
        p.sender_coordinator
            .send_selection(&TraceNodeId::new(local), old, &TraceNodeId::new(A))
            .unwrap();
        let events = transfer(&p);
        assert!(events.is_empty());
        // 记录器可按60秒窗口缩短，旧值不用于传输后续期限的相等断言。
        let returned = p.recorder.returned.recv_timeout(WAIT).unwrap();
        assert_eq!(returned.remote.as_str(), C);
        assert_eq!(returned.value.trace_id, old.trace_id);
        assert!(returned.outcome.is_ok());
        p.recorder.selected(1);
        p.receiver_coordinator.close();
        p.sender_coordinator.close();
    } else {
        quiet(&p, 0, 0);
        assert!(p.sender.packets().is_empty(), "新进程不能重放旧通知");
        // 选一个旧进程从未联系过的E；复用C会被旧contacted去重，反而掩盖错误恢复。
        // 若旧选择恢复，新增同Trace邻居E会触发真实通知；这里不提供新上报调用。
        assert!(p
            .recorder
            .recorder
            .record_peer(old.trace_id, TraceNodeId::new(E)));
        p.recorder.recorder.seal_current_thread();
        quiet(&p, 0, 0);
        // 旧邻居专用Trace没有新登记；新通知应选中但不继承旧目标C。
        let first = TraceSelection {
            trace_id: neighbor_only,
            ..selection(0x63)
        };
        p.sender_coordinator
            .send_selection(&TraceNodeId::new(local), first, &TraceNodeId::new(A))
            .unwrap();
        assert!(transfer(&p).is_empty());
        p.recorder.selected(1);
        quiet(&p, 1, 0);
        // 同一新记录器的有效对照：当前登记后收到新通知，真实通知线程可正常发给C。
        let live = selection(0x64);
        assert!(p
            .recorder
            .recorder
            .record_peer(live.trace_id, TraceNodeId::new(C)));
        p.recorder.recorder.seal_current_thread();
        p.sender_coordinator
            .send_selection(&TraceNodeId::new(local), live, &TraceNodeId::new(A))
            .unwrap();
        assert!(transfer(&p).is_empty());
        p.recorder.callback(C, live);
        p.recorder.selected(2);
        assert_packet(&p.receiver.packets()[0], C, live, local);
        quiet(&p, 2, 1);
    }
    // 宿主主动关闭两端准入，停止/join真实记录与导出Worker，再释放Pair。
    // 只有同步收尾都返回后才检查panic；shutdown不是任意通知回调的排空屏障。
    let observed = panics_after_teardown(&panics, || {
        p.receiver_coordinator.close();
        p.sender_coordinator.close();
        p.recorder.stop();
        drop(p);
    });
    assert!(observed.is_empty(), "冷启收尾捕获意外panic：{observed:?}");
    if phase == "old" {
        println!("FINAL_OLD_PROCESS_CLOSED");
    } else {
        println!("FINAL_NEW_PROCESS_CHECKED");
    }
}
pub(super) fn test_final_cold_start_fixed_identity_does_not_restore_memory() {
    restart("fixed");
}
pub(super) fn test_final_cold_start_changed_identity_does_not_restore_memory() {
    restart("changed");
}
