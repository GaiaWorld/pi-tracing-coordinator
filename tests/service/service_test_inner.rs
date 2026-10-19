//! slice-service：只经冻结service/protocol/binding壳和真实依赖公开API观测。
//! @api:src/service.rs @impl:src/service_inner.rs
//! @test:tests/service/service_test.rs
//! @design-doc:design/tracing-coordinator/07-design/call-chain.md#slice-service
//! 取证与未闭合门禁：[测试证据](../../docs/SLICE_SERVICE_TEST_EVIDENCE.md#slice-service-test-evidence)。
//!
//! 每个声明在独立子进程中运行，仅安装一种codec适配。PeerMessage仅生成远端输入，
//! 使用真实Fory派生/登记/编码；不手写解码器、不构造生产私有字段或合法结果。
//! 真实registry创建params并初始化，真实on_send被poll；不以框架Ok代替业务效果。
//! 精确零/一次调用、全分支临时资源释放、无缓存和仅Weak构造需实现后独立源码审查。
//! selected_traces含异步/去重，不是API调用计数；有界worker效果及最终排空配合审查取证。

use crate::{
    binding::{BindingError, CoordinationBinding},
    protocol::{
        register_fory_types, selection_to_message, validate_selection_message, SelectionMessage,
    },
    service::{self, CoordinationServiceContext, COORDINATION_SERVICE_LAYER},
};
use fory::Fory;
use opentelemetry_sdk::{
    error::{OTelSdkError, OTelSdkResult},
    trace::{SpanData, SpanExporter},
    Resource,
};
use pi_logger::{
    FlightRecorder, FlightRecorderGuard, TraceId, TraceNodeId, TraceNotifier, TraceSelection,
};
use pi_service::{
    DefaultCodec, MultiLayerCodec, Service, ServiceAccessMode, ServiceCodec, ServiceContexts,
    ServiceError, ServiceHost, ServicePort, ServiceRuntime, ServiceStatus, ServiceStatusEvent,
    ServiceStatusListener,
};
use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    io::Read,
    panic::{catch_unwind, panic_any},
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

const LAYER: &str = "tracing-coordinator";
const TARGET: &str = "pi_tracing_coordinator::service";
const LOCAL: &str = "本地/服务-B";
const SOURCE: &str = "  节点é/e\u{301}🦀\0 ";
const OBSERVER: &str = "观察/节点-C";
const OTHER_SOURCE: &str = "独立/节点-D";
const OTHER_TRACE: [u8; 16] = [0xa7; 16];
const TRACE: [u8; 16] = [
    1, 35, 69, 103, 137, 171, 205, 239, 254, 220, 186, 152, 118, 84, 50, 16,
];
const WAIT: Duration = Duration::from_secs(3);

/// 仅设置真实远端的公开线表示；同编号/字段/配置经编码字节等价与两接收codec解码校准。
/// 它不是生产SelectionMessage或服务替身，不参与业务校验/准入/事件发布。
#[derive(fory::ForyObject)]
struct PeerMessage {
    trace_id: [u8; 16],
    expires_at_100ns: i64,
    source: Vec<u8>,
}

/// 独立进程隔离适配及记录器线程，20秒硬截止；恰好命中当前声明才可采信。
/// 前置TODO与目标TODO通过SERVICE_STEP区分，超时/环境失败不计RED。
fn isolated(action: impl FnOnce(&PanicProbe)) {
    const ENV: &str = "PI_SLICE_SERVICE_TEST_CHILD";
    let current = thread::current();
    let name = current.name().expect("libtest场景名称").to_owned();
    if std::env::var(ENV).ok().as_deref() == Some(name.as_str()) {
        // 每个子进程仅运行一个场景，hook覆盖业务线程及真实记录器线程；不改变框架捕获策略。
        let panics = PanicProbe::install();
        action(&panics);
        panics.assert_clean();
        return;
    }
    let mut child = Command::new(std::env::current_exe().expect("定位测试程序"))
        .args(["--exact", &name, "--nocapture", "--test-threads=1"])
        .env(ENV, &name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("夹具失败：启动测试子进程");
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let output_thread = thread::spawn(move || {
        let mut text = String::new();
        stdout.read_to_string(&mut text).unwrap();
        text
    });
    let error_thread = thread::spawn(move || {
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
        if start.elapsed() > Duration::from_secs(20) {
            timed_out = true;
            child.kill().unwrap();
            break child.wait().unwrap();
        }
        thread::sleep(Duration::from_millis(5));
    };
    let output = output_thread.join().unwrap();
    let errors = error_thread.join().unwrap();
    assert!(
        output.contains("running 1 test"),
        "不是有效测试命中：{output}\n{errors}"
    );
    assert!(
        !timed_out,
        "场景超时，非有效RED：{name}\n{output}\n{errors}"
    );
    assert!(
        status.success(),
        "场景失败，按SERVICE_STEP及具体TODO判定目标红线：{name} {status}\n{output}\n{errors}"
    );
    assert!(
        output.contains("1 passed; 0 failed; 0 ignored"),
        "场景未完成：{output}\n{errors}"
    );
}

/// 只观测panic，即使真实框架捕获后返回Ok也不能让场景假绿。
/// 仅在单场景子进程安装；正常场景不允许panic，校准标记必须精确消费后归零。
struct PanicProbe(Arc<Mutex<Vec<String>>>);
impl PanicProbe {
    fn install() -> Self {
        let events = Arc::new(Mutex::new(Vec::new()));
        let saved = events.clone();
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let message = if let Some(value) = info.payload().downcast_ref::<&str>() {
                (*value).to_owned()
            } else if let Some(value) = info.payload().downcast_ref::<String>() {
                value.clone()
            } else {
                "非字符串panic载荷".to_owned()
            };
            saved
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push(message);
            previous(info);
        }));
        Self(events)
    }
    fn assert_clean(&self) {
        let events = self.0.lock().unwrap().clone();
        assert!(events.is_empty(), "框架捕获也不得隐藏意外panic：{events:?}");
    }
    /// 在尚未启动记录器前校准：副作用已发生、panic已被捕获，探针仍必须精确看见。
    fn calibrate(&self) {
        self.assert_clean();
        let effects = AtomicUsize::new(0);
        let caught = catch_unwind(|| {
            effects.fetch_add(1, Ordering::SeqCst);
            panic_any("SERVICE_PANIC_PROBE_CALIBRATION");
        });
        assert!(caught.is_err());
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        let events = std::mem::take(&mut *self.0.lock().unwrap());
        assert_eq!(events, ["SERVICE_PANIC_PROBE_CALIBRATION"]);
        self.assert_clean();
    }
}

fn step(label: &str) {
    eprintln!("SERVICE_STEP={label}");
}
fn port() -> ServicePort {
    ServicePort::from(4091u16)
}

/// typed业务同步，真实框架本场景应在一次poll内完成；不新增执行器/任务或复制框架。
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("同步静态服务本次poll未完成，不能把Pending当作持续占用worker"),
    }
}

/// 普通Err矩阵的实际编译前提；不拿当前环境变量猜测依赖构建，不捕获Fory panic冒充Err。
fn require_normal_errors() {
    let enabled = fory_core::error::should_panic_on_error();
    eprintln!("slice-service: FORY_PANIC_ON_ERROR={enabled}");
    assert!(!enabled, "环境阻塞：普通Err矩阵要求Fory实际构建开关为false");
}

/// 宿主冷路径先完成唯一真实登记再发布；不在接收路径补登记。
fn registered() -> Arc<Fory> {
    let mut table = Fory::default();
    register_fory_types(&mut table).expect("前置：第2块真实集中登记");
    Arc::new(table)
}
fn default_codec() -> DefaultCodec {
    DefaultCodec::new(registered())
}
fn multi_codec() -> MultiLayerCodec {
    let table = registered();
    let codec = MultiLayerCodec::new(Arc::new(Fory::default()));
    codec.register_layer_fory(LAYER, table.clone());
    assert!(codec.has_layer(LAYER));
    assert!(Arc::ptr_eq(&*codec.layer_fory(LAYER).unwrap(), &table));
    codec
}

/// 不同真实Fory实例中的同号同结构远端表示；没有修改生产消息可见性或手工补解码逻辑。
fn peer_bytes(trace: [u8; 16], source: &[u8], ticks: i64) -> Vec<u8> {
    let mut table = Fory::default();
    table
        .register::<PeerMessage>(4300)
        .expect("夹具：远端候选登记");
    DefaultCodec::new(Arc::new(table))
        .default_encode(&PeerMessage {
            trace_id: trace,
            source: source.to_vec(),
            expires_at_100ns: ticks,
        })
        .expect("夹具：真实远端编码")
}

/// 整秒后30秒，低于记录器60秒窗口；只为真实时效依赖准备输入，不复制其裁剪策略。
fn deadline() -> SystemTime {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    UNIX_EPOCH
        .checked_add(Duration::from_secs(seconds + 30))
        .unwrap()
}
fn valid_bytes(trace: [u8; 16], source: &str, expires_at: SystemTime) -> Vec<u8> {
    let message = selection_to_message(
        TraceSelection {
            trace_id: TraceId::from_bytes(trace),
            expires_at,
        },
        &TraceNodeId::new(source),
    )
    .expect("前置：已验收协议转换");
    default_codec()
        .default_encode(&message)
        .expect("前置：真实发送端编码")
}

/// 框架同步状态通知观察器；只保存不可变事件，不重入registry或初始化。
#[derive(Default)]
struct StatusObserver(Mutex<Vec<(ServiceStatusEvent, thread::ThreadId)>>);
impl ServiceStatusListener for StatusObserver {
    fn on_status_event(&self, event: ServiceStatusEvent) {
        self.0.lock().unwrap().push((event, thread::current().id()));
    }
}
fn runtime<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    codec: C,
    contexts: ServiceContexts,
    listener: &Arc<StatusObserver>,
) -> ServiceRuntime<C> {
    ServiceRuntime::new(
        contexts,
        codec,
        None,
        Default::default(),
        ServiceHost::Local(7),
        Some(listener.clone()),
    )
}

struct Harness<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>> {
    runtime: ServiceRuntime<C>,
    owner: Arc<CoordinationBinding>,
    listener: Arc<StatusObserver>,
}
impl<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>> Harness<C> {
    /// 真实registry向同一宏服务bind_params/init；绝不伪造params或调用生产芯。
    fn new(codec: C, service: &'static dyn Service<Codec = C>) -> Self {
        let owner = Arc::new(CoordinationBinding::new());
        step("前置:new_context，尚未到达本场景接收目标");
        let context = Arc::new(CoordinationServiceContext::new(owner.handle()));
        let contexts = ServiceContexts::new();
        assert!(contexts.insert_only_read(context).is_none());
        let listener = Arc::new(StatusObserver::default());
        let runtime = runtime(codec, contexts, &listener);
        runtime
            .registry()
            .register(service)
            .expect("前置：真实静态注册");
        runtime
            .registry()
            .init(&runtime, port())
            .expect("前置：真实宏初始化");
        assert!(matches!(
            runtime.get_status(port()),
            Some(ServiceStatus::Running)
        ));
        Self {
            runtime,
            owner,
            listener,
        }
    }
    fn deliver(&self, bytes: Vec<u8>) {
        step("目标:框架实际poll→宏decode→typed共同接收芯");
        ready(self.runtime.on_send(port(), bytes, None)).expect("框架静态send仅向外返回准入结果");
    }
}

#[derive(Clone, Debug)]
struct Warning {
    level: Level,
    target: String,
    fields: BTreeMap<String, String>,
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

/// 捕获整个本库命名空间，错误target/级别/额外日志同样可见；不安装全局subscriber。
#[derive(Clone)]
struct Observer {
    enabled: bool,
    warnings: Arc<Mutex<Vec<Warning>>>,
    after_event: Option<Arc<dyn Fn() + Send + Sync>>,
}
impl Observer {
    fn new() -> Self {
        Self {
            enabled: true,
            warnings: Arc::default(),
            after_event: None,
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
        if !self.enabled(event.metadata()) {
            return;
        }
        let mut fields = Fields(BTreeMap::new());
        event.record(&mut fields);
        self.warnings.lock().unwrap().push(Warning {
            level: *event.metadata().level(),
            target: event.metadata().target().to_owned(),
            fields: fields.0,
            thread: thread::current().id(),
        });
        // 先释放观察器锁再执行宿主钩子，不制造观察器自身死锁。
        if let Some(action) = &self.after_event {
            action();
        }
    }
}
/// 必需字段精确匹配，但允许合法的固定说明字段；有限样本不能证明所有编码形式的隐私。
/// 附加字段还须经同错误/不同私有输入的成对比较，最终在实现后审查其确实不读取输入。
fn warning_matches(event: &Warning, stage: &str, error: &str) -> bool {
    event.level == Level::WARN
        && event.target == TARGET
        && event.thread == thread::current().id()
        && event
            .fields
            .get("service_port")
            .is_some_and(|value| value == "4091")
        && event
            .fields
            .get("stage")
            .is_some_and(|value| value == stage)
        && event
            .fields
            .get("error")
            .is_some_and(|value| value == error)
        && ![
            "source",
            "trace_id",
            "trace",
            "operation_id",
            "message_input",
        ]
        .iter()
        .any(|key| event.fields.contains_key(*key))
        && !event.fields.values().any(|value| {
            value.contains(SOURCE)
                || value.contains(OTHER_SOURCE)
                || value.contains("不要泄露")
                || value.contains(&TraceId::from_bytes(TRACE).to_string())
                || value.contains(&TraceId::from_bytes(OTHER_TRACE).to_string())
        })
}
fn assert_warning(event: &Warning, stage: &str, error: &str) {
    assert!(
        warning_matches(event, stage, error),
        "日志必须匹配固定target/WARN及必需字段，且不得泄露私有输入：{event:?}"
    );
}
fn extra_fields(event: &Warning) -> BTreeMap<&str, &str> {
    event
        .fields
        .iter()
        .filter(|(key, _)| !["service_port", "stage", "error"].contains(&key.as_str()))
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect()
}
fn warning_pair_matches(first: &Warning, second: &Warning, stage: &str, error: &str) -> bool {
    warning_matches(first, stage, error)
        && warning_matches(second, stage, error)
        && extra_fields(first) == extra_fields(second)
}
fn assert_private_pair(first: &Warning, second: &Warning, stage: &str, error: &str) {
    assert!(
        warning_pair_matches(first, second, stage, error),
        "同一拒绝原因下更换私有输入不能改变附加日志字段：{first:?} / {second:?}"
    );
}

#[derive(Debug, Default)]
struct ExportObservation {
    calls: AtomicUsize,
    shutdowns: AtomicUsize,
}
#[derive(Debug)]
struct ObservedExporter(Arc<ExportObservation>);
impl SpanExporter for ObservedExporter {
    async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        Err(OTelSdkError::InternalFailure(
            "未创建Span，不应导出".to_owned(),
        ))
    }
    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.0.shutdowns.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct Notices(mpsc::Sender<(TraceNodeId, TraceSelection)>);
impl TraceNotifier for Notices {
    fn notify(&self, remote: &TraceNodeId, selection: TraceSelection) -> anyhow::Result<()> {
        self.0
            .send((remote.clone(), selection))
            .map_err(|error| anyhow::anyhow!("观测通道关闭：{error}"))
    }
}

/// 真实公开start/enable_coordination，依赖worker推进选择；观察器只接收真实通知。
struct Recorder {
    recorder: FlightRecorder,
    guard: Option<FlightRecorderGuard>,
    notices: mpsc::Receiver<(TraceNodeId, TraceSelection)>,
    exports: Arc<ExportObservation>,
}
impl Recorder {
    fn new(coordination: bool) -> Self {
        let exports = Arc::new(ExportObservation::default());
        let guard = FlightRecorder::start(
            pi_logger::observability::FlightRecorderConfig {
                retention_secs: 60,
                followup_secs: 0,
                worker_poll_ms: 5,
                export_retries: 0,
                ..Default::default()
            },
            ObservedExporter(exports.clone()),
            Resource::builder_empty().build(),
        )
        .expect("前置：真实记录器start");
        let recorder = guard.recorder();
        let (tx, notices) = mpsc::channel();
        if coordination {
            recorder
                .enable_coordination(TraceNodeId::new(LOCAL), Arc::new(Notices(tx)))
                .expect("前置：真实协调器启用");
        }
        assert_eq!(recorder.stats().selected_traces, 0);
        Self {
            recorder,
            guard: Some(guard),
            notices,
            exports,
        }
    }
    fn wait_selected(&self, expected: u64) {
        let start = Instant::now();
        loop {
            let actual = self.recorder.stats().selected_traces;
            assert!(actual <= expected, "出现额外不同Trace：{actual}/{expected}");
            if actual == expected {
                break;
            }
            assert!(
                start.elapsed() < WAIT,
                "真实worker未在3秒内处理：{actual}/{expected}"
            );
            thread::sleep(Duration::from_millis(2));
        }
    }
    // 等待采集/导出worker，通知队列可能被丢弃且在途回调不保证已退出；不当作通知flush。
    fn stop(&mut self) {
        if let Some(guard) = self.guard.take() {
            guard.shutdown();
        }
    }
    fn assert_final(&self, selected: u64, notifications: u64) {
        assert!(
            self.guard.is_none(),
            "必须先关闭并等待真实采集/导出worker结束"
        );
        // 通知统计与通道为空仅是此时观察；精确全集/来源绝不回送仍须独立源码审查。
        assert_eq!(self.recorder.stats().selected_traces, selected);
        assert_eq!(self.recorder.stats().notification_attempts, notifications);
        assert_eq!(self.recorder.stats().failed_notifications, 0);
        assert_eq!(self.exports.calls.load(Ordering::SeqCst), 0);
        assert_eq!(self.exports.shutdowns.load(Ordering::SeqCst), 1);
        assert!(self.notices.try_recv().is_err(), "出现额外真实通知");
    }
}
impl Drop for Recorder {
    fn drop(&mut self) {
        self.stop();
    }
}

/// 真实编码字节等价、两接收codec解码校准；不声称MultiLayer编码或反解PeerMessage。
pub(super) fn test_service_fixture_wire_representation_and_error_controls() {
    isolated(|panics| {
        panics.calibrate();
        require_normal_errors();
        let receiver = default_codec();
        let mut multi = multi_codec();
        multi.switch_layer(LAYER).unwrap();
        for (trace, source, ticks, time) in [
            (
                TRACE,
                SOURCE,
                -1,
                UNIX_EPOCH.checked_sub(Duration::from_nanos(100)).unwrap(),
            ),
            ([0x82; 16], "not-a-peer-id", 0, UNIX_EPOCH),
        ] {
            let encoded = peer_bytes(trace, source.as_bytes(), ticks);
            let production = selection_to_message(
                TraceSelection {
                    trace_id: TraceId::from_bytes(trace),
                    expires_at: time,
                },
                &TraceNodeId::new(source),
            )
            .unwrap();
            assert_eq!(receiver.default_encode(&production).unwrap(), encoded);
            for decoded in [
                receiver
                    .default_decode::<SelectionMessage>(encoded.clone())
                    .unwrap(),
                multi
                    .default_decode::<SelectionMessage>(encoded.clone())
                    .unwrap(),
            ] {
                let (selection, actual) = validate_selection_message(decoded).unwrap().into_parts();
                assert_eq!(selection.trace_id.to_bytes(), trace);
                assert_eq!(selection.expires_at, time);
                assert_eq!(actual.as_str(), source);
            }
        }
        let unregistered_bytes = peer_bytes(TRACE, SOURCE.as_bytes(), 0);
        assert!(matches!(
            DefaultCodec::default().default_decode::<SelectionMessage>(unregistered_bytes),
            Err(ServiceError::CodecError(_))
        ));
        let bytes = peer_bytes([0; 16], &[0xff], i64::MAX);
        let decoded: SelectionMessage = receiver.default_decode(bytes).unwrap();
        assert!(matches!(
            validate_selection_message(decoded),
            Err(crate::protocol::ProtocolError::InvalidTraceId)
        ));
        assert!(matches!(
            receiver.default_decode::<SelectionMessage>(vec![]),
            Err(ServiceError::CodecError(_))
        ));
        let missing = MultiLayerCodec::with_default_layer("other", registered());
        assert!(!missing.has_layer(LAYER) && !missing.has_layer("default"));
        let listener = Arc::new(StatusObserver::default());
        let runtime = runtime(missing, ServiceContexts::new(), &listener);
        assert!(
            matches!(runtime.codec_for_layer(LAYER), Err(ServiceError::CodecLayerNotFound(layer)) if layer == LAYER)
        );
        assert!(listener.0.lock().unwrap().is_empty());
    });
}

/// 观察器必须能发现错误target/级别和额外本库事件，实际记录器不以去重值计调用次数。
pub(super) fn test_service_fixture_recorder_and_observer_controls() {
    isolated(|_| {
        let mut recorder = Recorder::new(true);
        let future = deadline();
        for trace in [TRACE, TRACE, [0x83; 16]] {
            assert!(recorder.recorder.apply_remote_selection(
                TraceSelection {
                    trace_id: TraceId::from_bytes(trace),
                    expires_at: future,
                },
                TraceNodeId::new(SOURCE)
            ));
        }
        recorder.wait_selected(2);
        recorder.stop();
        recorder.assert_final(2, 0);
        let mut notices = Recorder::new(true);
        let notice_trace = TraceId::from_bytes([0x84; 16]);
        assert!(notices
            .recorder
            .record_peer(notice_trace, TraceNodeId::new(SOURCE)));
        assert!(notices
            .recorder
            .record_peer(notice_trace, TraceNodeId::new(OBSERVER)));
        assert!(notices.recorder.apply_remote_selection(
            TraceSelection {
                trace_id: notice_trace,
                expires_at: future,
            },
            TraceNodeId::new(SOURCE)
        ));
        let (remote, actual) = notices
            .notices
            .recv_timeout(WAIT)
            .expect("真实通知夹具校准");
        assert_eq!(remote.as_str(), OBSERVER);
        assert_eq!(actual.trace_id, notice_trace);
        assert_eq!(actual.expires_at, future);
        notices.wait_selected(1);
        notices.stop();
        notices.assert_final(1, 1);
        let observer = Observer::new();
        tracing::subscriber::with_default(observer.clone(), || {
            tracing::warn!(target: "pi_tracing_coordinator::service", service_port = 4091u16, stage = "validate", error = %"校准原因");
            tracing::info!(target: "pi_tracing_coordinator::other", "错误级别也要可见");
            tracing::warn!(target: "pi_tracing_coordinator", "根target也要可见");
            tracing::warn!(target: "pi_service::service", "依赖噪声");
            tracing::warn!(target: "pi_tracing_coordinator_other", "相似前缀不计");
        });
        let events = observer.saved();
        assert_eq!(events.len(), 3);
        assert_warning(&events[0], "validate", "校准原因");
        assert_eq!(events[1].level, Level::INFO);
        assert_eq!(events[1].target, "pi_tracing_coordinator::other");
        assert_eq!(events[2].target, "pi_tracing_coordinator");
        let fixed = Observer::new();
        tracing::subscriber::with_default(fixed.clone(), || {
            for _ in 0..2 {
                tracing::warn!(target: "pi_tracing_coordinator::service", service_port = 4091u16, stage = "validate", error = %"校准原因", category = "coordination", "接收参数校验失败");
            }
        });
        let fixed_events = fixed.saved();
        assert_eq!(fixed_events.len(), 2);
        assert_private_pair(&fixed_events[0], &fixed_events[1], "validate", "校准原因");
        // 真事件成对负面对照：额外字段任意命名，字节/Trace/转义表示随私有输入变化都必须拒绝。
        let negative = Observer::new();
        tracing::subscriber::with_default(negative.clone(), || {
            for source in [SOURCE, OTHER_SOURCE] {
                tracing::warn!(target: "pi_tracing_coordinator::service", service_port = 4091u16, stage = "validate", error = %"校准原因", raw_source = ?source.as_bytes());
            }
            for trace in [TRACE, OTHER_TRACE] {
                tracing::warn!(target: "pi_tracing_coordinator::service", service_port = 4091u16, stage = "validate", error = %"校准原因", payload = ?trace);
            }
            for source in [SOURCE, OTHER_SOURCE] {
                tracing::warn!(target: "pi_tracing_coordinator::service", service_port = 4091u16, stage = "validate", error = %"校准原因", message = ?source);
            }
        });
        let bad_events = negative.saved();
        assert_eq!(bad_events.len(), 6);
        let (pairs, remainder) = bad_events.as_chunks::<2>();
        assert!(remainder.is_empty());
        for pair in pairs {
            assert!(
                !warning_pair_matches(&pair[0], &pair[1], "validate", "校准原因"),
                "原始输入变化必须被隐私对照拒绝"
            );
        }
        let mut disabled = Observer::new();
        disabled.enabled = false;
        tracing::subscriber::with_default(disabled.clone(), || {
            tracing::warn!(target: "pi_tracing_coordinator::service", "禁用事件");
        });
        assert!(disabled.saved().is_empty());
    });
}

/// 已关闭句柄也允许组合；构造不得升级检查、发布context或注册服务。
/// 精确无升级/无锁通过实现后源码审查补足，不把此行为场景声称为内部操作计数。
pub(super) fn test_service_context_accepts_closed_handle_without_installing() {
    isolated(|_| {
        let owner = CoordinationBinding::new();
        let handle = owner.handle();
        owner.close();
        let contexts = ServiceContexts::new();
        let listener = Arc::new(StatusObserver::default());
        let runtime = runtime(default_codec(), contexts.clone(), &listener);
        step("目标:new_context接管已关闭弱句柄");
        let context = Arc::new(CoordinationServiceContext::new(handle));
        drop(owner);
        assert!(contexts
            .get_only_read::<CoordinationServiceContext>()
            .is_none());
        assert!(runtime.get_status(port()).is_none());
        assert!(listener.0.lock().unwrap().is_empty());
        assert!(contexts.insert_only_read(context.clone()).is_none());
        let actual = contexts
            .get_only_read::<CoordinationServiceContext>()
            .unwrap();
        assert!(Arc::ptr_eq(&actual, &context));
    });
}

fn assert_meta<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    runtime: &ServiceRuntime<C>,
    service: &'static dyn Service<Codec = C>,
) {
    assert_eq!(COORDINATION_SERVICE_LAYER, LAYER);
    assert_eq!(
        service.id().as_str(),
        "svc://local/observability/tracing/coordinator"
    );
    assert_eq!(u16::from(service.port()), 4091);
    assert_eq!(service.layer(), LAYER);
    assert_eq!(service.mode(), ServiceAccessMode::Send);
    assert_eq!(service.version().to_string(), "0.1.0");
    let meta = runtime.get_meta(port()).expect("真实注册元信息");
    assert_eq!(meta.id, service.id());
    assert_eq!(meta.port, port());
    assert_eq!(meta.layer, LAYER);
    assert_eq!(meta.mode, ServiceAccessMode::Send);
    assert_eq!(meta.version.to_string(), "0.1.0");
}

/// 取得借用不能改变空runtime；只有显式register后才Registered，仍未init。
fn check_static<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    codec: C,
    getter: fn() -> &'static dyn Service<Codec = C>,
) {
    let listener = Arc::new(StatusObserver::default());
    let runtime = runtime(codec, ServiceContexts::new(), &listener);
    assert!(runtime.get_status(port()).is_none());
    step("目标:取得同一静态Service借用");
    let observer = Observer::new();
    let (first, second) =
        tracing::subscriber::with_default(observer.clone(), || (getter(), getter()));
    assert!(
        observer.saved().is_empty(),
        "重复取得静态借用不得记录本库日志"
    );
    assert!(std::ptr::eq(first, second));
    assert!(runtime.registry().get_service(port()).is_none());
    assert!(runtime
        .contexts()
        .get_only_read::<CoordinationServiceContext>()
        .is_none());
    assert!(listener.0.lock().unwrap().is_empty());
    runtime.registry().register(first).unwrap();
    assert_meta(&runtime, first);
    assert!(matches!(
        runtime.get_status(port()),
        Some(ServiceStatus::Registered)
    ));
    let events = listener.0.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert!(events[0].0.prev_status.is_none());
    assert!(matches!(
        events[0].0.next_status,
        Some(ServiceStatus::Registered)
    ));
    assert_eq!(events[0].1, thread::current().id());
    drop(events);
    assert!(matches!(
        ready(runtime.on_send(port(), vec![], None)),
        Err(ServiceError::ServiceNotRunning(4091))
    ));
}

/// 真实registry调用该服务bind_params，缺失与预绑定失败是不同阶段。
fn check_missing_context<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    codec: C,
    service: &'static dyn Service<Codec = C>,
) {
    let listener = Arc::new(StatusObserver::default());
    let runtime = runtime(codec, ServiceContexts::new(), &listener);
    runtime.registry().register(service).unwrap();
    step("目标:registry.init缺少只读context");
    match runtime.registry().init(&runtime, port()) {
        Err(ServiceError::BindOnlyReadNotFound(actual, field, ty)) => {
            assert_eq!(actual, 4091);
            assert_eq!(field, "context");
            assert_eq!(ty, "CoordinationServiceContext");
        }
        other => panic!("必须是实际BindOnlyReadNotFound：{other:?}"),
    }
    assert!(matches!(
        runtime.get_status(port()),
        Some(ServiceStatus::Registered)
    ));
    assert_meta(&runtime, service);
    assert_eq!(
        listener.0.lock().unwrap().len(),
        1,
        "失败不发布Failed或Running事件"
    );
}

/// 公共bind_params创建的合法params强持context；仅借用宏公开接口，不downcast私有生成类型。
fn check_bound_params<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    codec: C,
    service: &'static dyn Service<Codec = C>,
) {
    let owner = CoordinationBinding::new();
    let context = Arc::new(CoordinationServiceContext::new(owner.handle()));
    let weak = Arc::downgrade(&context);
    let contexts = ServiceContexts::new();
    contexts.insert_only_read(context.clone());
    let listener = Arc::new(StatusObserver::default());
    let runtime = runtime(codec, contexts.clone(), &listener);
    step("目标:同一静态服务bind_params创建合法拥有式params");
    let params = service.bind_params(&contexts).unwrap();
    let observer = Observer::new();
    let error = tracing::subscriber::with_default(observer.clone(), || {
        ready(service.handle_send(runtime.clone(), params.clone(), vec![]))
    });
    assert!(
        matches!(error, Err(ServiceError::RuntimeError(reason)) if reason == "service 4091 codec not initialized")
    );
    assert!(
        observer.saved().is_empty(),
        "尚未init的合法params不能进入typed"
    );
    drop(context);
    drop(contexts);
    drop(runtime);
    drop(owner);
    assert!(weak.upgrade().is_some(), "params须独立保留context");
    assert!(
        listener.0.lock().unwrap().is_empty(),
        "直接绑定不安装或初始化"
    );
    drop(params);
    assert!(
        weak.upgrade().is_none(),
        "最终params释放后不得额外保存context"
    );
}

/// 标准安装完成不要求记录器存在；同步事件与元信息保护真实registry装配。
fn check_initialization<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    h: Harness<C>,
    service: &'static dyn Service<Codec = C>,
) {
    assert_meta(&h.runtime, service);
    let events = h.listener.0.lock().unwrap();
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
    assert!(events
        .iter()
        .all(|(_, actual_thread)| *actual_thread == thread::current().id()));
    drop(events);
    let observer = Observer::new();
    tracing::subscriber::with_default(observer.clone(), || {
        h.deliver(valid_bytes(TRACE, SOURCE, deadline()))
    });
    let events = observer.saved();
    assert_eq!(events.len(), 1, "Running不意味着记录器已就绪");
    assert_warning(
        &events[0],
        "submit",
        &BindingError::RecorderUnavailable.to_string(),
    );
}

/// 保存的opaque条目快照在runtime销毁后仍保留params/context/codec；不假定清理TLS。
fn check_running_lifetime<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    codec: C,
    table: Arc<Fory>,
    service: &'static dyn Service<Codec = C>,
) {
    let owner = CoordinationBinding::new();
    let context = Arc::new(CoordinationServiceContext::new(owner.handle()));
    let weak_context = Arc::downgrade(&context);
    let weak_table = Arc::downgrade(&table);
    drop(table);
    let contexts = ServiceContexts::new();
    contexts.insert_only_read(context.clone());
    let listener = Arc::new(StatusObserver::default());
    let runtime = runtime(codec, contexts, &listener);
    runtime.registry().register(service).unwrap();
    runtime.registry().init(&runtime, port()).unwrap();
    let entry = runtime.registry().get_service(port()).unwrap();
    owner.close();
    drop(owner);
    assert!(
        matches!(runtime.get_status(port()), Some(ServiceStatus::Running)),
        "binding.close不改变registry状态"
    );
    drop(context);
    drop(runtime);
    assert!(
        weak_context.upgrade().is_some(),
        "Running快照持有params及context"
    );
    assert!(
        weak_table.upgrade().is_some(),
        "Running params保留已绑定codec表"
    );
    drop(entry);
    assert!(weak_context.upgrade().is_none());
    assert!(
        weak_table.upgrade().is_none(),
        "本场景未编解码，不含TLS或其它宿主持有"
    );
}

pub(super) fn test_service_multi_prebind_failure_retains_failed_params() {
    isolated(|_| {
        require_normal_errors();
        step("前置:multi_layer_service");
        let service = service::multi_layer_service();
        let table = registered();
        let weak_table = Arc::downgrade(&table);
        // 不存在目标层和default，才能观察实际CodecLayerNotFound，不用错误params伪造init失败。
        let codec = MultiLayerCodec::with_default_layer("unrelated", table);
        assert!(!codec.has_layer(LAYER) && !codec.has_layer("default"));
        let owner = CoordinationBinding::new();
        let context = Arc::new(CoordinationServiceContext::new(owner.handle()));
        let weak_context = Arc::downgrade(&context);
        let contexts = ServiceContexts::new();
        contexts.insert_only_read(context.clone());
        let listener = Arc::new(StatusObserver::default());
        let runtime = runtime(codec, contexts, &listener);
        runtime.registry().register(service).unwrap();
        step("目标:合法bind_params后实际codec预绑定失败");
        assert!(
            matches!(runtime.registry().init(&runtime, port()), Err(ServiceError::CodecLayerNotFound(layer)) if layer == LAYER)
        );
        assert!(
            matches!(runtime.get_status(port()), Some(ServiceStatus::Failed(ServiceError::CodecLayerNotFound(layer))) if layer == LAYER)
        );
        let events = listener.0.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert!(matches!(
            events[1].0.prev_status,
            Some(ServiceStatus::Registered)
        ));
        assert!(
            matches!(&events[1].0.next_status, Some(ServiceStatus::Failed(ServiceError::CodecLayerNotFound(layer))) if layer == LAYER)
        );
        assert_eq!(events[1].1, thread::current().id());
        drop(events);
        let entry = runtime.registry().get_service(port()).unwrap();
        owner.close();
        drop(owner);
        drop(context);
        drop(runtime);
        assert!(
            weak_context.upgrade().is_some(),
            "Failed条目仍持params/context"
        );
        assert!(
            weak_table.upgrade().is_none(),
            "预绑定失败未保存codec，模板已释放"
        );
        drop(entry);
        assert!(weak_context.upgrade().is_none());
    });
}

/// 获准前Future的创建/丢弃均不得解码或发布；不同Trace控制避免去重掩盖提前执行。
fn check_lazy<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(h: Harness<C>) {
    let mut recorder = Recorder::new(true);
    h.owner
        .bind_recorder(Some(recorder.recorder.clone()))
        .unwrap();
    let observer = Observer::new();
    tracing::subscriber::with_default(observer.clone(), || {
        let future = h
            .runtime
            .on_send(port(), valid_bytes(TRACE, SOURCE, deadline()), None);
        assert!(observer.saved().is_empty());
        assert_eq!(recorder.recorder.stats().selected_traces, 0);
        drop(future);
        assert!(observer.saved().is_empty());
        h.deliver(valid_bytes([0x91; 16], SOURCE, deadline()));
    });
    recorder.wait_selected(1);
    recorder.stop();
    recorder.assert_final(1, 0);
    assert!(observer.saved().is_empty());
}

/// 空输入是有界确定性解码错误；先证明真实codec返回Err，再从框架poll观察零业务效果。
fn check_decode_failure<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>> + Clone>(h: Harness<C>) {
    require_normal_errors();
    let mut recorder = Recorder::new(true);
    h.owner
        .bind_recorder(Some(recorder.recorder.clone()))
        .unwrap();
    assert!(matches!(
        h.runtime
            .codec_for_layer(LAYER)
            .unwrap()
            .default_decode::<SelectionMessage>(vec![]),
        Err(ServiceError::CodecError(_))
    ));
    let observer = Observer::new();
    tracing::subscriber::with_default(observer.clone(), || {
        h.deliver(vec![]);
        assert!(
            observer.saved().is_empty(),
            "框架解码失败不应进typed本库警告"
        );
        h.deliver(valid_bytes([0x92; 16], SOURCE, deadline()));
    });
    recorder.wait_selected(1);
    recorder.stop();
    recorder.assert_final(1, 0);
    assert!(observer.saved().is_empty());
}

/// 输入是对端真实Fory编码后宏成功解码的业务非法值，不是任意异常字节安全测试。
/// 每次拒绝精确核验validate日志，最终独立Trace控制与排空共同证明无错误选择。
fn check_invalid<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(h: Harness<C>) {
    use crate::protocol::ProtocolError;
    let mut recorder = Recorder::new(true);
    h.owner
        .bind_recorder(Some(recorder.recorder.clone()))
        .unwrap();
    let valid_ticks = 4_102_444_800_i64 * 10_000_000;
    let mut examples = vec![(
        peer_bytes([0; 16], SOURCE.as_bytes(), valid_ticks),
        peer_bytes([0; 16], OTHER_SOURCE.as_bytes(), valid_ticks),
        ProtocolError::InvalidTraceId,
    )];
    for (raw, other) in [
        (&[0x80][..], &[0x81][..]),
        (&[0xff][..], &[0xfe][..]),
        (&[0xc0, 0xaf][..], &[0xc1, 0xaf][..]),
        (&[0xed, 0xa0, 0x80][..], &[0xed, 0xbf, 0x80][..]),
        (&[0xf4, 0x90, 0x80, 0x80][..], &[0xf4, 0x91, 0x80, 0x80][..]),
        (&[b'A', 0xe2, 0x82][..], &[b'B', 0xe2, 0x83][..]),
        (&[b'A', 0xe2, b'('][..], &[b'B', 0xe2, b')'][..]),
    ] {
        let error = std::str::from_utf8(raw).unwrap_err();
        assert_eq!(
            std::str::from_utf8(other).unwrap_err(),
            error,
            "隐私对照不能改变错误语义"
        );
        examples.push((
            peer_bytes(TRACE, raw, valid_ticks),
            peer_bytes(OTHER_TRACE, other, valid_ticks),
            ProtocolError::InvalidIdentityUtf8 {
                field: "source",
                source: error,
            },
        ));
    }
    let mut secret = "不要泄露/来源秘密".as_bytes().to_vec();
    let mut other_secret = vec![b'X'; secret.len()];
    secret.push(0xff);
    other_secret.push(0xfe);
    let error = std::str::from_utf8(&secret).unwrap_err();
    assert_eq!(std::str::from_utf8(&other_secret).unwrap_err(), error);
    examples.push((
        peer_bytes(TRACE, &secret, valid_ticks),
        peer_bytes(OTHER_TRACE, &other_secret, valid_ticks),
        ProtocolError::InvalidIdentityUtf8 {
            field: "source",
            source: error,
        },
    ));
    examples.push((
        peer_bytes(TRACE, &[], valid_ticks),
        peer_bytes(OTHER_TRACE, &[], valid_ticks),
        ProtocolError::EmptyIdentity { field: "source" },
    ));
    for ticks in [
        i64::MIN,
        -116_444_736_000_000_001,
        9_106_927_300_854_775_808,
        i64::MAX,
    ] {
        examples.push((
            peer_bytes(TRACE, SOURCE.as_bytes(), ticks),
            peer_bytes(OTHER_TRACE, OTHER_SOURCE.as_bytes(), ticks),
            ProtocolError::DeadlineOutOfRange,
        ));
    }
    let observer = Observer::new();
    for (index, (bytes, private_variant, error)) in examples.into_iter().enumerate() {
        tracing::subscriber::with_default(observer.clone(), || h.deliver(bytes));
        let events = observer.saved();
        assert_eq!(events.len(), index * 2 + 1, "每次输入恰一次警告");
        assert_warning(&events[index * 2], "validate", &error.to_string());
        tracing::subscriber::with_default(observer.clone(), || h.deliver(private_variant));
        let events = observer.saved();
        assert_eq!(events.len(), index * 2 + 2, "隐私对照也恰一次警告");
        assert_private_pair(
            &events[index * 2],
            &events[index * 2 + 1],
            "validate",
            &error.to_string(),
        );
    }
    let count = observer.saved().len();
    tracing::subscriber::with_default(observer.clone(), || {
        h.deliver(valid_bytes([0x93; 16], SOURCE, deadline()))
    });
    assert_eq!(observer.saved().len(), count, "成功无日志");
    recorder.wait_selected(1);
    recorder.stop();
    recorder.assert_final(1, 0);
    assert!(
        h.owner.handle().admit_send().is_ok(),
        "非法输入不能推进关闭或消耗绑定状态"
    );
}

/// 多项非法与Closed并存时仍先完整protocol校验；不让binding错误覆盖业务首错。
fn check_priority<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(h: Harness<C>) {
    use crate::protocol::ProtocolError;
    h.owner.close();
    let observer = Observer::new();
    let invalid_utf8 = vec![0xff];
    let cases = [
        (
            peer_bytes([0; 16], &invalid_utf8, i64::MAX),
            ProtocolError::InvalidTraceId,
        ),
        (
            peer_bytes(TRACE, &[0xff], i64::MIN),
            ProtocolError::InvalidIdentityUtf8 {
                field: "source",
                source: std::str::from_utf8(&invalid_utf8).unwrap_err(),
            },
        ),
        (
            peer_bytes(TRACE, &[], i64::MAX),
            ProtocolError::EmptyIdentity { field: "source" },
        ),
        (
            peer_bytes(TRACE, SOURCE.as_bytes(), i64::MIN),
            ProtocolError::DeadlineOutOfRange,
        ),
    ];
    for (index, (bytes, expected)) in cases.into_iter().enumerate() {
        tracing::subscriber::with_default(observer.clone(), || h.deliver(bytes));
        let events = observer.saved();
        assert_eq!(events.len(), index + 1);
        assert_warning(&events[index], "validate", &expected.to_string());
    }
    tracing::subscriber::with_default(observer.clone(), || {
        h.deliver(valid_bytes(TRACE, SOURCE, deadline()))
    });
    let events = observer.saved();
    assert_eq!(events.len(), 5);
    assert_warning(&events[4], "submit", &BindingError::Closed.to_string());
}

/// 未绑定/明确None都允许Running，接收以真实Unavailable结束；不把启动等同记录器就绪。
fn check_unavailable<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(h: Harness<C>) {
    let observer = Observer::new();
    tracing::subscriber::with_default(observer.clone(), || {
        h.deliver(valid_bytes(TRACE, SOURCE, deadline()))
    });
    h.owner.bind_recorder(None).unwrap();
    tracing::subscriber::with_default(observer.clone(), || {
        h.deliver(valid_bytes([0x94; 16], OTHER_SOURCE, deadline()))
    });
    let events = observer.saved();
    assert_eq!(events.len(), 2);
    for event in &events {
        assert_warning(
            event,
            "submit",
            &BindingError::RecorderUnavailable.to_string(),
        );
    }
    assert_private_pair(
        &events[0],
        &events[1],
        "submit",
        &BindingError::RecorderUnavailable.to_string(),
    );
    assert!(matches!(
        h.runtime.get_status(port()),
        Some(ServiceStatus::Running)
    ));
    assert!(h.owner.handle().admit_send().is_ok());
    assert!(matches!(
        h.owner.bind_recorder(None),
        Err(BindingError::RecorderAlreadyBound)
    ));
}

/// close只停止本库新准入，不注销服务或关闭宿主Guard；独立不同Trace直接发布证明宿主仍可用。
fn check_closed<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(h: Harness<C>) {
    let mut recorder = Recorder::new(true);
    h.owner
        .bind_recorder(Some(recorder.recorder.clone()))
        .unwrap();
    h.owner.close();
    let observer = Observer::new();
    tracing::subscriber::with_default(observer.clone(), || {
        h.deliver(valid_bytes(TRACE, SOURCE, deadline()))
    });
    tracing::subscriber::with_default(observer.clone(), || {
        h.deliver(valid_bytes(OTHER_TRACE, OTHER_SOURCE, deadline()))
    });
    let events = observer.saved();
    assert_eq!(events.len(), 2);
    assert_private_pair(
        &events[0],
        &events[1],
        "submit",
        &BindingError::Closed.to_string(),
    );
    assert!(matches!(
        h.runtime.get_status(port()),
        Some(ServiceStatus::Running)
    ));
    assert!(h
        .runtime
        .contexts()
        .get_only_read::<CoordinationServiceContext>()
        .is_some());
    assert!(recorder.recorder.apply_remote_selection(
        TraceSelection {
            trace_id: TraceId::from_bytes([0x95; 16]),
            expires_at: deadline(),
        },
        TraceNodeId::new(SOURCE)
    ));
    recorder.wait_selected(1);
    recorder.stop();
    recorder.assert_final(1, 0);
}

/// params/context存活不得保持Owner开放；Owner Drop后真实宏接收按Closed拒绝。
fn check_owner_drop<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(h: Harness<C>) {
    let mut recorder = Recorder::new(true);
    h.owner
        .bind_recorder(Some(recorder.recorder.clone()))
        .unwrap();
    let Harness {
        runtime,
        owner,
        listener: _,
    } = h;
    let observer = Observer::new();
    drop(owner);
    assert!(runtime
        .contexts()
        .get_only_read::<CoordinationServiceContext>()
        .is_some());
    tracing::subscriber::with_default(observer.clone(), || {
        step("目标:Owner释放后context/params仍存活，实际poll拒绝");
        ready(runtime.on_send(port(), valid_bytes(TRACE, SOURCE, deadline()), None)).unwrap();
    });
    let events = observer.saved();
    assert_eq!(events.len(), 1);
    assert_warning(&events[0], "submit", &BindingError::Closed.to_string());
    assert!(recorder.recorder.apply_remote_selection(
        TraceSelection {
            trace_id: TraceId::from_bytes([0x96; 16]),
            expires_at: deadline(),
        },
        TraceNodeId::new(SOURCE)
    ));
    recorder.wait_selected(1);
    recorder.stop();
    recorder.assert_final(1, 0);
}

/// 四种真实false原因只作为输入装配，service统一报告未发布；不由布尔结果猜原因。
fn check_false<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    codec: fn() -> C,
    getter: fn() -> &'static dyn Service<Codec = C>,
) {
    for reason in ["self", "expired", "not-installed", "stopped"] {
        let h = Harness::new(codec(), getter());
        let mut recorder = Recorder::new(reason != "not-installed");
        h.owner
            .bind_recorder(Some(recorder.recorder.clone()))
            .unwrap();
        if reason == "stopped" {
            recorder.stop();
        }
        let source = if reason == "self" { LOCAL } else { SOURCE };
        let expires = if reason == "expired" {
            UNIX_EPOCH
        } else {
            deadline()
        };
        let observer = Observer::new();
        tracing::subscriber::with_default(observer.clone(), || {
            h.deliver(valid_bytes(TRACE, source, expires))
        });
        let events = observer.saved();
        assert_eq!(events.len(), 1, "{reason}必须恰一次submit警告");
        assert_warning(
            &events[0],
            "submit",
            &BindingError::EventNotPublished.to_string(),
        );
        tracing::subscriber::with_default(observer.clone(), || {
            let private_source = if reason == "self" {
                LOCAL
            } else {
                OTHER_SOURCE
            };
            h.deliver(valid_bytes(OTHER_TRACE, private_source, expires));
        });
        let paired = observer.saved();
        assert_eq!(paired.len(), 2, "同原因私有输入对照恰一次新增警告");
        assert_private_pair(
            &paired[0],
            &paired[1],
            "submit",
            &BindingError::EventNotPublished.to_string(),
        );
        // 可恢复的false用不同Trace合法对照；已关闭Guard按真实合同不再开放。
        let expected = if reason != "stopped" && reason != "not-installed" {
            tracing::subscriber::with_default(observer.clone(), || {
                h.deliver(valid_bytes([0x97; 16], SOURCE, deadline()))
            });
            recorder.wait_selected(1);
            1
        } else {
            0
        };
        assert_eq!(observer.saved().len(), 2, "成功无日志，false无补偿/重试");
        recorder.stop();
        recorder.assert_final(expected, 0);
    }
}

/// 真实记录器通知保真：宿主自行登记来源和第三方邻居；收到来源不回传、第三方收到原Trace/期限。
/// record_peer只在宿主夹具使用；service/binding不得借此补关联或伪造交接。
fn check_success<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(h: Harness<C>) {
    let mut recorder = Recorder::new(true);
    h.owner
        .bind_recorder(Some(recorder.recorder.clone()))
        .unwrap();
    let source = TraceNodeId::new(SOURCE);
    let neighbor = TraceNodeId::new(OBSERVER);
    let mut first = [0; 16];
    first[0] = 1;
    let mut last = [0; 16];
    last[15] = 1;
    let expires = deadline();
    let observer = Observer::new();
    for (index, trace) in [TRACE, first, last].into_iter().enumerate() {
        assert!(recorder
            .recorder
            .record_peer(TraceId::from_bytes(trace), source.clone()));
        assert!(recorder
            .recorder
            .record_peer(TraceId::from_bytes(trace), neighbor.clone()));
        tracing::subscriber::with_default(observer.clone(), || {
            h.deliver(valid_bytes(trace, SOURCE, expires))
        });
        let (remote, actual) = recorder
            .notices
            .recv_timeout(WAIT)
            .expect("真实通知线程应在3秒内交出结果");
        assert_eq!(
            remote.as_str(),
            OBSERVER,
            "原始source必须被排除，不能被trim/替换"
        );
        assert_eq!(actual.trace_id.to_bytes(), trace);
        assert_eq!(actual.expires_at, expires);
        recorder.wait_selected(index as u64 + 1);
    }
    assert!(observer.saved().is_empty(), "成功不记录本库日志");
    recorder.stop();
    recorder.assert_final(3, 3);
}

/// 宿主过滤只影响观察，不能让非法消息进入binding、不能阻止合法发布。
fn check_filtered<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(h: Harness<C>) {
    let mut recorder = Recorder::new(true);
    h.owner
        .bind_recorder(Some(recorder.recorder.clone()))
        .unwrap();
    let mut observer = Observer::new();
    observer.enabled = false;
    tracing::subscriber::with_default(observer.clone(), || {
        // 只有UTF-8非法，其余条件合法；不可用过期拒绝遮蔽服务误放行。
        let expires = deadline();
        let ticks = i64::try_from(expires.duration_since(UNIX_EPOCH).unwrap().as_secs()).unwrap()
            * 10_000_000;
        h.deliver(peer_bytes(TRACE, &[0xff], ticks));
        h.deliver(valid_bytes([0x98; 16], SOURCE, expires));
    });
    recorder.wait_selected(1);
    recorder.stop();
    recorder.assert_final(1, 0);
    assert!(observer.saved().is_empty());
}

/// 宿主subscriber重入同Owner的close必须完成；若service日志仍在binding锁内会由子进程截止暴露。
/// 这里只动态证明Unavailable路径；全部分支临时资源释放仍需独立源码审查。
fn check_log_after_return<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(h: Harness<C>) {
    let owner = h.owner.clone();
    let actions = Arc::new(AtomicUsize::new(0));
    let seen = actions.clone();
    let mut observer = Observer::new();
    observer.after_event = Some(Arc::new(move || {
        owner.close();
        assert!(matches!(
            owner.handle().admit_send(),
            Err(BindingError::Closed)
        ));
        seen.fetch_add(1, Ordering::SeqCst);
    }));
    tracing::subscriber::with_default(observer.clone(), || {
        h.deliver(valid_bytes(TRACE, SOURCE, deadline()))
    });
    assert_eq!(actions.load(Ordering::SeqCst), 1);
    let events = observer.saved();
    assert_eq!(events.len(), 1);
    assert_warning(
        &events[0],
        "submit",
        &BindingError::RecorderUnavailable.to_string(),
    );
    assert!(matches!(
        h.owner.handle().admit_send(),
        Err(BindingError::Closed)
    ));
    assert!(matches!(
        h.runtime.get_status(port()),
        Some(ServiceStatus::Running)
    ));
}

/// default：静态身份及元信息且不提前安装；经真实registry/框架及冻结壳。
pub(super) fn test_service_default_static_identity_and_metadata() {
    isolated(|_| {
        check_static(default_codec(), service::default_service);
    });
}

/// default：缺上下文保留旧状态；经真实registry/框架及冻结壳。
pub(super) fn test_service_default_missing_context_preserves_registered() {
    isolated(|_| {
        check_missing_context(default_codec(), service::default_service());
    });
}

/// default：合法bind_params保留context且codec未绑定；经真实registry/框架及冻结壳。
pub(super) fn test_service_default_bound_params_hold_context() {
    isolated(|_| {
        check_bound_params(default_codec(), service::default_service());
    });
}

/// default：真实宏初始化不等于记录器就绪；经真实registry/框架及冻结壳。
pub(super) fn test_service_default_registry_initialization_without_recorder() {
    isolated(|_| {
        let service = service::default_service();
        check_initialization(Harness::new(default_codec(), service), service);
    });
}

/// default：Running条目延长params资源生命周期；经真实registry/框架及冻结壳。
pub(super) fn test_service_default_running_entry_retains_context_and_codec() {
    isolated(|_| {
        let table = registered();
        check_running_lifetime(
            DefaultCodec::new(table.clone()),
            table,
            service::default_service(),
        );
    });
}

/// default：Future创建和未poll丢弃不处理消息；经真实registry/框架及冻结壳。
pub(super) fn test_service_default_unpolled_future_has_no_business_effect() {
    isolated(|_| {
        check_lazy(Harness::new(default_codec(), service::default_service()));
    });
}

/// default：前置decode失败不进入typed；经真实registry/框架及冻结壳。
pub(super) fn test_service_default_decode_failure_stays_before_typed() {
    isolated(|_| {
        check_decode_failure(Harness::new(default_codec(), service::default_service()));
    });
}

/// default：业务非法只产生validate拒绝；经真实registry/框架及冻结壳。
pub(super) fn test_service_default_invalid_business_is_validate_only() {
    isolated(|_| {
        check_invalid(Harness::new(default_codec(), service::default_service()));
    });
}

/// default：Closed不覆盖protocol首错；经真实registry/框架及冻结壳。
pub(super) fn test_service_default_protocol_priority_precedes_closed() {
    isolated(|_| {
        check_priority(Harness::new(default_codec(), service::default_service()));
    });
}

/// default：未绑定和None均接收拒绝；经真实registry/框架及冻结壳。
pub(super) fn test_service_default_unbound_and_none_report_unavailable() {
    isolated(|_| {
        check_unavailable(Harness::new(default_codec(), service::default_service()));
    });
}

/// default：关闭拒绝新接收且不关闭宿主；经真实registry/框架及冻结壳。
pub(super) fn test_service_default_closed_rejects_without_stopping_host() {
    isolated(|_| {
        check_closed(Harness::new(default_codec(), service::default_service()));
    });
}

/// default：Owner释放后params仍存活但接收关闭；经真实registry/框架及冻结壳。
pub(super) fn test_service_default_owner_drop_leaves_weak_context() {
    isolated(|_| {
        check_owner_drop(Harness::new(default_codec(), service::default_service()));
    });
}

/// default：真实false只报告未发布；经真实registry/框架及冻结壳。
pub(super) fn test_service_default_recorder_false_is_submit_not_published() {
    isolated(|_| {
        check_false(default_codec, service::default_service);
    });
}

/// default：真实成功保真且无日志；经真实registry/框架及冻结壳。
pub(super) fn test_service_default_success_preserves_trace_deadline_and_source() {
    isolated(|_| {
        check_success(Harness::new(default_codec(), service::default_service()));
    });
}

/// default：宿主过滤不影响业务；经真实registry/框架及冻结壳。
pub(super) fn test_service_default_filtered_logs_do_not_change_business() {
    isolated(|_| {
        check_filtered(Harness::new(default_codec(), service::default_service()));
    });
}

/// default：submit警告允许宿主重入关闭；经真实registry/框架及冻结壳。
pub(super) fn test_service_default_submit_log_runs_after_binding_return() {
    isolated(|_| {
        check_log_after_return(Harness::new(default_codec(), service::default_service()));
    });
}

/// multi：静态身份及元信息且不提前安装；经真实registry/框架及冻结壳。
pub(super) fn test_service_multi_static_identity_and_metadata() {
    isolated(|_| {
        check_static(multi_codec(), service::multi_layer_service);
    });
}

/// multi：缺上下文保留旧状态；经真实registry/框架及冻结壳。
pub(super) fn test_service_multi_missing_context_preserves_registered() {
    isolated(|_| {
        check_missing_context(multi_codec(), service::multi_layer_service());
    });
}

/// multi：合法bind_params保留context且codec未绑定；经真实registry/框架及冻结壳。
pub(super) fn test_service_multi_bound_params_hold_context() {
    isolated(|_| {
        check_bound_params(multi_codec(), service::multi_layer_service());
    });
}

/// multi：真实宏初始化不等于记录器就绪；经真实registry/框架及冻结壳。
pub(super) fn test_service_multi_registry_initialization_without_recorder() {
    isolated(|_| {
        let service = service::multi_layer_service();
        check_initialization(Harness::new(multi_codec(), service), service);
    });
}

/// multi：Running条目延长params资源生命周期；经真实registry/框架及冻结壳。
pub(super) fn test_service_multi_running_entry_retains_context_and_codec() {
    isolated(|_| {
        let table = registered();
        let codec = MultiLayerCodec::new(Arc::new(Fory::default()));
        codec.register_layer_fory(LAYER, table.clone());
        check_running_lifetime(codec, table, service::multi_layer_service());
    });
}

/// multi：Future创建和未poll丢弃不处理消息；经真实registry/框架及冻结壳。
pub(super) fn test_service_multi_unpolled_future_has_no_business_effect() {
    isolated(|_| {
        check_lazy(Harness::new(multi_codec(), service::multi_layer_service()));
    });
}

/// multi：前置decode失败不进入typed；经真实registry/框架及冻结壳。
pub(super) fn test_service_multi_decode_failure_stays_before_typed() {
    isolated(|_| {
        check_decode_failure(Harness::new(multi_codec(), service::multi_layer_service()));
    });
}

/// multi：业务非法只产生validate拒绝；经真实registry/框架及冻结壳。
pub(super) fn test_service_multi_invalid_business_is_validate_only() {
    isolated(|_| {
        check_invalid(Harness::new(multi_codec(), service::multi_layer_service()));
    });
}

/// multi：Closed不覆盖protocol首错；经真实registry/框架及冻结壳。
pub(super) fn test_service_multi_protocol_priority_precedes_closed() {
    isolated(|_| {
        check_priority(Harness::new(multi_codec(), service::multi_layer_service()));
    });
}

/// multi：未绑定和None均接收拒绝；经真实registry/框架及冻结壳。
pub(super) fn test_service_multi_unbound_and_none_report_unavailable() {
    isolated(|_| {
        check_unavailable(Harness::new(multi_codec(), service::multi_layer_service()));
    });
}

/// multi：关闭拒绝新接收且不关闭宿主；经真实registry/框架及冻结壳。
pub(super) fn test_service_multi_closed_rejects_without_stopping_host() {
    isolated(|_| {
        check_closed(Harness::new(multi_codec(), service::multi_layer_service()));
    });
}

/// multi：Owner释放后params仍存活但接收关闭；经真实registry/框架及冻结壳。
pub(super) fn test_service_multi_owner_drop_leaves_weak_context() {
    isolated(|_| {
        check_owner_drop(Harness::new(multi_codec(), service::multi_layer_service()));
    });
}

/// multi：真实false只报告未发布；经真实registry/框架及冻结壳。
pub(super) fn test_service_multi_recorder_false_is_submit_not_published() {
    isolated(|_| {
        check_false(multi_codec, service::multi_layer_service);
    });
}

/// multi：真实成功保真且无日志；经真实registry/框架及冻结壳。
pub(super) fn test_service_multi_success_preserves_trace_deadline_and_source() {
    isolated(|_| {
        check_success(Harness::new(multi_codec(), service::multi_layer_service()));
    });
}

/// multi：宿主过滤不影响业务；经真实registry/框架及冻结壳。
pub(super) fn test_service_multi_filtered_logs_do_not_change_business() {
    isolated(|_| {
        check_filtered(Harness::new(multi_codec(), service::multi_layer_service()));
    });
}

/// multi：submit警告允许宿主重入关闭；经真实registry/框架及冻结壳。
pub(super) fn test_service_multi_submit_log_runs_after_binding_return() {
    isolated(|_| {
        check_log_after_return(Harness::new(multi_codec(), service::multi_layer_service()));
    });
}

/// 正常冷路径允许空类型表进入Running，但真实解码不能因此成功；证明初始化不代替登记。
fn check_running_without_registration<
    C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>> + Clone,
>(
    h: Harness<C>,
) {
    require_normal_errors();
    let mut recorder = Recorder::new(true);
    h.owner
        .bind_recorder(Some(recorder.recorder.clone()))
        .unwrap();
    let bytes = valid_bytes(TRACE, SOURCE, deadline());
    let receiver = h.runtime.codec_for_layer(LAYER).unwrap();
    assert!(matches!(
        receiver.default_decode::<SelectionMessage>(bytes.clone()),
        Err(ServiceError::CodecError(_))
    ));
    let observer = Observer::new();
    tracing::subscriber::with_default(observer.clone(), || h.deliver(bytes));
    assert!(
        observer.saved().is_empty(),
        "未登记前置解码失败不能进入typed日志"
    );
    assert!(matches!(
        h.runtime.get_status(port()),
        Some(ServiceStatus::Running)
    ));
    // 记录器的独立合法控制不经过服务，证明最终0不是宿主记录器不可用造成。
    assert!(recorder.recorder.apply_remote_selection(
        TraceSelection {
            trace_id: TraceId::from_bytes([0x99; 16]),
            expires_at: deadline(),
        },
        TraceNodeId::new(SOURCE)
    ));
    recorder.wait_selected(1);
    recorder.stop();
    recorder.assert_final(1, 0);
}

/// Default预绑定成功及Running不证明真实消息类型已登记。
pub(super) fn test_service_default_running_does_not_prove_registration() {
    isolated(|_| {
        check_running_without_registration(Harness::new(
            DefaultCodec::default(),
            service::default_service(),
        ));
    });
}

/// 明确存在的MultiLayer目标表也可能缺少消息登记；不能把选层成功当作登记证据。
pub(super) fn test_service_multi_running_does_not_prove_registration() {
    isolated(|_| {
        let codec = MultiLayerCodec::with_default_layer(LAYER, Arc::new(Fory::default()));
        assert!(codec.has_layer(LAYER));
        check_running_without_registration(Harness::new(codec, service::multi_layer_service()));
    });
}
