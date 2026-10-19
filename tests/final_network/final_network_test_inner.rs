//! slice-final R1真实宿主实现面：只经根公开API驱动协调器。
//! @api:src/coordinator.rs @impl:src/coordinator_inner.rs @test:tests/final_network_test.rs
//! @design-doc:design/tracing-coordinator/07-design/call-chain.md#slice-final
//! 控制文件仅协调角色就绪/结束；通知字节只走真实P2pServiceBuilder/QUIC。
//! 每角色单独OS进程、密钥及唯一4091适配。宿主登记关联，测试不手调接收或发布选择。

use fory::Fory;
use opentelemetry_sdk::{
    error::OTelSdkResult,
    trace::{SpanData, SpanExporter},
    Resource,
};
use pi_logger::{
    FlightRecorder, FlightRecorderGuard, TraceId, TraceNodeId, TraceNotifier, TraceSelection,
};
use pi_p2p::{AuthMode, HostId, P2pConfig, SeedhostEntry, TransportSource};
use pi_p2p_service::{
    EventHub, NetworkEventHandler, P2pServiceBuilder, P2pServiceTerminal, TransportEvent,
    TransportEventFilter, TransportEventKind,
};
use pi_service::{
    DefaultCodec, MultiLayerCodec, ServiceAddress, ServiceCodec, ServiceContexts, ServiceError,
    ServiceHost, ServiceRuntime, ServiceStatus, ServiceTerminal, ServiceTransport,
};
use pi_tracing_coordinator::{
    register_fory_types, CoordinationError, TracingCoordinator, COORDINATION_SERVICE_LAYER,
    COORDINATION_SERVICE_PORT,
};
use std::{
    collections::BTreeMap,
    fmt, fs,
    future::Future,
    io::Read,
    net::UdpSocket,
    ops::Deref,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tracing::{
    field::{Field, Visit},
    span::{Attributes, Id, Record},
    Event, Metadata, Subscriber,
};

const ROLE: &str = "PI_SLICE_FINAL_NETWORK_ROLE";
const CASE: &str = "PI_SLICE_FINAL_NETWORK_CASE";
const ROOT: &str = "PI_SLICE_FINAL_NETWORK_ROOT";
const WAIT: Duration = Duration::from_secs(10);
const QUIET: Duration = Duration::from_millis(150);
const HARD_LIMIT: Duration = Duration::from_secs(60);
type Constructor<C> = fn(
    &Arc<NetworkHost<C>>,
    &ServiceRuntime<C>,
    TraceNodeId,
) -> Result<TracingCoordinator<NetworkHost<C>, C>, CoordinationError>;

/// 只等待外部可观察事实，所有等待有界；超时不是产品缺失的有效RED。
fn wait_for(mut predicate: impl FnMut() -> bool, label: &str) {
    let start = Instant::now();
    while !predicate() {
        assert!(
            start.elapsed() < WAIT,
            "R1有界等待失败（非有效RED）：{label}"
        );
        thread::sleep(Duration::from_millis(3));
    }
}

/// 对真实Future使用现有执行器；不会替换网络/宏/记录器功能。
fn run<F: Future>(future: F) -> F::Output {
    futures::executor::block_on(future)
}

/// 全线程panic观察必须晚于同步收尾采样，避免框架吞panic假绿。
fn panic_probe() -> Arc<Mutex<Vec<String>>> {
    let saved = Arc::new(Mutex::new(Vec::new()));
    let copy = saved.clone();
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        copy.lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(info.to_string());
        previous(info);
    }));
    saved
}

#[derive(Clone)]
struct EncodedSend {
    address: ServiceAddress,
    bytes: Vec<u8>,
}

/// 透明宿主观察器：记录公开终端已编码字节，再恰一次委托真实服务终端。
/// 不保存待发任务、不重试、不poll接收、不向记录器注入业务；观测副本不属于产品缓存。
struct ObservedTransport<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>> {
    native: Arc<P2pServiceTerminal<C>>,
    submitted: Arc<Mutex<Vec<EncodedSend>>>,
}
impl<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>> ServiceTransport<C>
    for ObservedTransport<C>
{
    fn local_host(&self) -> ServiceHost {
        self.native.service_terminal().local_host()
    }
    fn send(&self, address: ServiceAddress, input: Vec<u8>) -> Result<(), ServiceError> {
        self.submitted.lock().unwrap().push(EncodedSend {
            address: address.clone(),
            bytes: input.clone(),
        });
        self.native.service_terminal().send(address, input, None)
    }
    fn rpc(
        &self,
        address: ServiceAddress,
        input: Vec<u8>,
        timeout: Option<Duration>,
    ) -> futures::future::BoxFuture<'static, Result<Vec<u8>, ServiceError>> {
        let native = self.native.clone();
        Box::pin(async move {
            native
                .service_terminal()
                .rpc(address, input, timeout, None)
                .await
        })
    }
    fn reconnect(
        &self,
        target: ServiceHost,
    ) -> futures::future::BoxFuture<'static, Result<(), ServiceError>> {
        let native = self.native.clone();
        Box::pin(async move { native.service_terminal().reconnect(target).await })
    }
    fn close_connection(
        &self,
        target: ServiceHost,
    ) -> futures::future::BoxFuture<'static, Result<(), ServiceError>> {
        let native = self.native.clone();
        Box::pin(async move { native.service_terminal().close_connection(target).await })
    }
    fn is_connected(&self, target: ServiceHost) -> bool {
        self.native.service_terminal().is_connected(target)
    }
}
struct NetworkHost<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>> {
    terminal: ServiceTerminal<C>,
}
impl<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>> Deref for NetworkHost<C> {
    type Target = ServiceTerminal<C>;
    fn deref(&self) -> &Self::Target {
        &self.terminal
    }
}

/// 独立对端表示只作真实Fory字节观测，不导入生产私有消息或重写转换逻辑。
#[derive(Debug, fory::ForyObject)]
struct PeerMessage {
    trace_id: [u8; 16],
    expires_at_100ns: i64,
    source: Vec<u8>,
}
fn registered() -> Arc<Fory> {
    let panic_on_error = fory_core::error::should_panic_on_error();
    eprintln!("R1_DEPENDENCY FORY_PANIC_ON_ERROR={panic_on_error}");
    assert!(!panic_on_error);
    let mut fory = Fory::default();
    register_fory_types(&mut fory).unwrap();
    Arc::new(fory)
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
fn assert_send(send: &EncodedSend, target: HostId, source: HostId, value: TraceSelection) {
    assert_eq!(send.address.host, ServiceHost::Remote(target));
    assert_eq!(u16::from(send.address.port), COORDINATION_SERVICE_PORT);
    let mut table = Fory::default();
    table.register::<PeerMessage>(4300).unwrap();
    let actual: PeerMessage = DefaultCodec::new(Arc::new(table))
        .default_decode(send.bytes.clone())
        .unwrap();
    assert_eq!(actual.trace_id, value.trace_id.to_bytes());
    assert_eq!(actual.source, source.to_string().as_bytes());
    let count = i64::try_from(
        value
            .expires_at
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            / 100,
    )
    .unwrap();
    assert_eq!(actual.expires_at_100ns, count);
}

#[derive(Debug)]
struct Exporter(Arc<AtomicUsize>);
impl SpanExporter for Exporter {
    async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[derive(Clone, Debug)]
struct Returned {
    remote: TraceNodeId,
    selection: TraceSelection,
    result: Result<(), String>,
}
/// 原样委托生产notifier，仅在真实返回后报告；不替换选择或发送能力。
struct ObservedNotifier {
    delegate: Arc<dyn TraceNotifier>,
    returned: mpsc::SyncSender<Returned>,
}
impl TraceNotifier for ObservedNotifier {
    fn notify(&self, remote: &TraceNodeId, selection: TraceSelection) -> anyhow::Result<()> {
        let result = self.delegate.notify(remote, selection);
        self.returned
            .try_send(Returned {
                remote: remote.clone(),
                selection,
                result: result.as_ref().map(|_| ()).map_err(ToString::to_string),
            })
            .unwrap();
        result
    }
}

#[derive(Clone, Default)]
struct Warnings(Arc<Mutex<Vec<BTreeMap<String, String>>>>);
struct Fields(BTreeMap<String, String>);
impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
}
impl Subscriber for Warnings {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target() == "pi_tracing_coordinator"
            || metadata.target().starts_with("pi_tracing_coordinator::")
    }
    fn new_span(&self, _attrs: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _id: &Id, _record: &Record<'_>) {}
    fn record_follows_from(&self, _id: &Id, _follows: &Id) {}
    fn enter(&self, _id: &Id) {}
    fn exit(&self, _id: &Id) {}
    fn event(&self, event: &Event<'_>) {
        if self.enabled(event.metadata()) {
            let mut fields = Fields(BTreeMap::new());
            event.record(&mut fields);
            self.0.lock().unwrap().push(fields.0);
        }
    }
}

#[derive(Clone)]
struct Peer {
    id: HostId,
    port: u16,
}
fn publish(root: &Path, name: &str, value: &str) {
    let temporary = root.join(format!("{name}.tmp"));
    fs::write(&temporary, value).unwrap();
    fs::rename(temporary, root.join(name)).unwrap();
}
fn peer(root: &Path, role: &str) -> Peer {
    let file = root.join(format!("{role}.ready"));
    wait_for(|| file.is_file(), "真实角色接收服务就绪");
    let text = fs::read_to_string(file).unwrap();
    let mut parts = text.split_whitespace();
    let id = parts.next().unwrap().parse().unwrap();
    let port = parts.next().unwrap().parse().unwrap();
    assert!(parts.next().is_none());
    Peer { id, port }
}
fn live_selection(root: &Path, tag: u8) -> TraceSelection {
    let seconds: u64 = fs::read_to_string(root.join("deadline"))
        .unwrap()
        .parse()
        .unwrap();
    TraceSelection {
        trace_id: TraceId::from_bytes([tag; 16]),
        expires_at: UNIX_EPOCH + Duration::from_secs(seconds),
    }
}
fn config(root: &Path, role: &str) -> (P2pConfig, u16) {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    let mut config = P2pConfig::default();
    config.identity.keypair_path = root
        .join(format!("{role}.key"))
        .to_string_lossy()
        .into_owned();
    config.listen_addrs = vec![format!("/ip4/127.0.0.1/udp/{port}/quic-v1")];
    config.mdns.enabled = false;
    config.mdns.auto_dial = false;
    config.metrics.enabled = false;
    config.metrics.runtime_toggle = false;
    config.logs.enabled = false;
    config.auth.mode = AuthMode::Disabled;
    config.governance.enabled = false;
    config.ipc.enabled = false;
    config.liveness.phi.enabled = false;
    config.liveness.ping.enabled = false;
    config.swarm_runtime.worker_threads = Some(2);
    config.swarm_runtime.max_blocking_threads = Some(4);
    (config, port)
}

/// 真实Error事件的完整联合匹配，不通过原因关键词猜peer或传输种类。
fn expected_async_failure(event: &TransportEvent, unknown: HostId) -> bool {
    event.peer_id == unknown
        && event.source == TransportSource::Quic
        && event.kind == TransportEventKind::Error
        && event.latency.is_none()
        && event.error.as_deref()
            == Some(
                "send backlog dropped after dial failure: NoAddresses; dropped 1 pending send(s)",
            )
}
fn assert_failure_controls(actual: &TransportEvent, unknown: HostId, other: HostId) {
    assert!(expected_async_failure(actual, unknown));
    let mut wrong = actual.clone();
    wrong.peer_id = other;
    assert!(!expected_async_failure(&wrong, unknown));
    wrong = actual.clone();
    wrong.source = TransportSource::Ipc;
    assert!(!expected_async_failure(&wrong, unknown));
    wrong = actual.clone();
    wrong.kind = TransportEventKind::Disconnected;
    assert!(!expected_async_failure(&wrong, unknown));
    wrong = actual.clone();
    wrong.error = Some("different host error".into());
    assert!(!expected_async_failure(&wrong, unknown));
    wrong.error = None;
    assert!(!expected_async_failure(&wrong, unknown));
}

const CLOSED_SEND: &str = "发送绑定准入失败：协调绑定已关闭";

/// 计数只作辅助；必须联合核对真实Worker回调的目标、Trace、精确期限及Closed结果。
fn received_content_matches(
    selected: u64,
    actual: &Returned,
    remote: &TraceNodeId,
    expected: TraceSelection,
) -> bool {
    selected == 1
        && actual.remote == *remote
        && actual.selection.trace_id == expected.trace_id
        && actual.selection.expires_at == expected.expires_at
        && actual.result.as_ref().map_err(String::as_str) == Err(CLOSED_SEND)
}

/// 目标及负对照共用判定器；克隆仅用于观察校准，绝不注入网络或生产事件。
fn assert_received_content_controls(
    selected: u64,
    actual: &Returned,
    remote: &TraceNodeId,
    expected: TraceSelection,
) {
    assert!(received_content_matches(selected, actual, remote, expected));
    let mut wrong = actual.clone();
    wrong.selection.trace_id = TraceId::from_bytes([0x7f; 16]);
    assert!(
        !received_content_matches(selected, &wrong, remote, expected),
        "R1-N1/N2：累计选择1不能接受错误Trace"
    );
    wrong = actual.clone();
    wrong.selection.expires_at -= Duration::from_nanos(100);
    assert!(
        !received_content_matches(selected, &wrong, remote, expected),
        "R1-N1/N2：必须拒绝提前100ns的期限"
    );
    wrong = actual.clone();
    wrong.selection.expires_at += Duration::from_nanos(100);
    assert!(
        !received_content_matches(selected, &wrong, remote, expected),
        "R1-N1/N2：必须拒绝延后100ns的期限"
    );
}

/// 仅在原网络事实和静默断言完成后登记新邻居，读取真实Worker保存的决定。
/// close只拒绝本库新发送，不清记录器选择；实际notifier以Closed返回而不会追加网络提交。
/// 不调用trigger/apply_remote_selection或手调notify，登记本身不能制造选择。
fn observe_received_content<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    owner: &TracingCoordinator<NetworkHost<C>, C>,
    recorder: &FlightRecorder,
    returned: &mpsc::Receiver<Returned>,
    warnings: &Warnings,
    probe: &TraceNodeId,
    expected: TraceSelection,
) -> Returned {
    assert_eq!(recorder.stats().selected_traces, 1);
    assert_eq!(recorder.stats().notification_attempts, 0);
    assert_eq!(recorder.stats().failed_notifications, 0);
    assert!(warnings.0.lock().unwrap().is_empty());
    assert!(matches!(
        returned.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    owner.close();
    assert!(recorder.record_peer(expected.trace_id, probe.clone()));
    recorder.seal_current_thread();
    let actual = returned
        .recv_timeout(WAIT)
        .expect("接收内容探针必须由真实Worker回调");
    // 回调实参来自记录器已保存的选择；目标与负对照再共用完整判定器。
    assert_eq!(actual.remote, *probe);
    assert_eq!(actual.selection.trace_id, expected.trace_id);
    assert_eq!(actual.selection.expires_at, expected.expires_at);
    assert_eq!(
        actual.result.as_ref().map_err(String::as_str),
        Err(CLOSED_SEND)
    );
    wait_for(
        || {
            recorder.stats().notification_attempts == 1
                && recorder.stats().failed_notifications == 1
        },
        "真实关闭探针回调结果已记账",
    );
    assert_received_content_controls(recorder.stats().selected_traces, &actual, probe, expected);
    let expected_warning = BTreeMap::from([
        ("remote".to_owned(), probe.as_str().to_owned()),
        (
            "service_port".to_owned(),
            COORDINATION_SERVICE_PORT.to_string(),
        ),
        ("error".to_owned(), CLOSED_SEND.to_owned()),
    ]);
    // 仅消费逐字段相等的一条真实Closed警告，不清空其它诊断或缩小target过滤。
    {
        let mut saved = warnings.0.lock().unwrap();
        assert_eq!(*saved, vec![expected_warning]);
        let _observed = saved.pop().unwrap();
    }
    actual
}

/// 控制屏障保存的是实际Worker回调值，不是固定的trace=73成功文案。
fn received_content_receipt(actual: &Returned) -> String {
    format!(
        "trace={} expires_at_100ns={}",
        actual.selection.trace_id,
        actual
            .selection
            .expires_at
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            / 100
    )
}

/// 一个角色的真实生产装配。只有宿主record_peer登记；接收、选择、通知均由真实线程推进。
fn role<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    root: &Path,
    name: &str,
    case: &str,
    codec: C,
    construct: Constructor<C>,
) {
    let panics = panic_probe();
    let warnings = Warnings::default();
    tracing::subscriber::set_global_default(warnings.clone()).unwrap();
    let (config, port) = config(root, name);
    // 无预绑定执行器，真实P2pServiceBuilder在完成Terminal后绑定其唯一HostRuntime。
    let runtime = ServiceRuntime::new(
        ServiceContexts::new(),
        codec,
        None,
        Default::default(),
        ServiceHost::Local(0),
        None,
    );
    let hub = Arc::new(EventHub::new());
    let (errors_tx, errors_rx) = mpsc::sync_channel(32);
    let listener = hub.register_listener(Arc::new(NetworkEventHandler::new().on_transport(
        move |events| {
            for event in events {
                errors_tx.try_send(event).expect("有界错误观察通道不能溢出");
            }
        },
    )));
    let mut filter = TransportEventFilter::allow_all();
    filter.kinds = Some(vec![TransportEventKind::Error]);
    let subscription = hub.subscribe_transport(listener, filter).unwrap();
    let native = Arc::new(
        run(P2pServiceBuilder::new(runtime.clone(), config)
            .with_debug_label(format!("slice-final-R1-{case}-{name}"))
            .with_event_hub(hub.clone())
            .prefer_ipc(false)
            .auto_ipc(false)
            .build_with_terminal())
        .unwrap(),
    );
    let local = native.p2p_terminal().registry().local_peer_id();
    assert_eq!(
        native.service_terminal().local_host(),
        ServiceHost::Remote(local)
    );
    let sends = Arc::new(Mutex::new(Vec::new()));
    let terminal = Arc::new(NetworkHost {
        terminal: runtime.terminal(Arc::new(ObservedTransport {
            native: native.clone(),
            submitted: sends.clone(),
        })),
    });
    let owner = construct(&terminal, &runtime, TraceNodeId::new(local.to_string())).unwrap();
    let (returned_tx, returned_rx) = mpsc::sync_channel(16);
    let exports = Arc::new(AtomicUsize::new(0));
    let guard: FlightRecorderGuard = FlightRecorder::start(
        pi_logger::observability::FlightRecorderConfig {
            retention_secs: 120,
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
        .enable_coordination(
            TraceNodeId::new(local.to_string()),
            Arc::new(ObservedNotifier {
                delegate: owner.notifier(),
                returned: returned_tx,
            }),
        )
        .unwrap();
    owner.bind_recorder(Some(recorder.clone())).unwrap();
    owner.install_service(&runtime).unwrap();
    assert!(matches!(
        runtime.get_status(COORDINATION_SERVICE_PORT.into()),
        Some(ServiceStatus::Running)
    ));
    publish(root, &format!("{name}.ready"), &format!("{local} {port}"));

    let names: &[&str] = if case == "async" {
        &["A", "B"]
    } else {
        &["A", "B", "C"]
    };
    let peers: BTreeMap<&str, Peer> = names
        .iter()
        .map(|remote| (*remote, peer(root, remote)))
        .collect();
    assert_eq!(peers[name].id, local);
    for (remote, peer) in &peers {
        if *remote == name {
            continue;
        }
        native
            .p2p_terminal()
            .add_seedhost(SeedhostEntry {
                peer_id: peer.id,
                multiaddrs: vec![format!("/ip4/127.0.0.1/udp/{}/quic-v1", peer.port)
                    .parse()
                    .unwrap()],
                priority: 0,
                tags: Vec::new(),
                enabled: true,
            })
            .unwrap();
    }
    wait_for(
        || {
            peers.iter().all(|(remote, peer)| {
                *remote == name
                    || native
                        .service_terminal()
                        .is_connected(ServiceHost::Remote(peer.id))
            })
        },
        "真实QUIC双向连接快照",
    );
    let trace = TraceId::from_bytes([0x71; 16]);
    if case == "chain" {
        let neighbors: &[&str] = match name {
            "A" => &["C"],
            "B" => &["A", "C"],
            "C" => &["B", "A"],
            _ => unreachable!(),
        };
        for neighbor in neighbors {
            assert!(recorder.record_peer(trace, TraceNodeId::new(peers[neighbor].id.to_string())));
        }
        recorder.seal_current_thread();
    }
    assert_eq!(recorder.stats().selected_traces, 0);
    assert_eq!(recorder.stats().notification_attempts, 0);
    assert!(sends.lock().unwrap().is_empty());
    assert!(warnings.0.lock().unwrap().is_empty());
    assert!(matches!(
        errors_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    publish(root, &format!("{name}.prepared"), "ready");
    wait_for(|| root.join("go").is_file(), "所有角色完成配置后统一开始");

    if case == "chain" {
        let value = live_selection(root, 0x71);
        if name == "A" {
            owner
                .send_selection(
                    &TraceNodeId::new(peers["B"].id.to_string()),
                    value,
                    &TraceNodeId::new(local.to_string()),
                )
                .unwrap();
            println!("R1_LOCAL_ACCEPT role=A trace=71; 此项不是远端完成回执");
        }
        wait_for(
            || recorder.stats().selected_traces == 1,
            "实际接收宏及真实Worker选择",
        );
        let expected_callbacks = u64::from(name != "A");
        if name != "A" {
            let callback = returned_rx
                .recv_timeout(WAIT)
                .expect("真实notifier必须返回");
            let target = if name == "B" { "C" } else { "A" };
            assert_eq!(callback.remote.as_str(), peers[target].id.to_string());
            assert_eq!(callback.selection.trace_id, value.trace_id);
            assert_eq!(callback.selection.expires_at, value.expires_at);
            assert!(callback.result.is_ok(), "{callback:?}");
        }
        wait_for(
            || recorder.stats().notification_attempts == expected_callbacks,
            "实际回调后的通知统计",
        );
        assert_eq!(recorder.stats().failed_notifications, 0);
        let target = match name {
            "A" => "B",
            "B" => "C",
            "C" => "A",
            _ => unreachable!(),
        };
        let history = sends.lock().unwrap().clone();
        assert_eq!(history.len(), 1, "每跳只能一次本库提交，不能由去重遮蔽重试");
        assert_send(&history[0], peers[target].id, local, value);
        println!("R1_NETWORK_RECEIVED role={name} selected=1 outgoing_bytes_checked=true propagated_content_checked={} callbacks={expected_callbacks}", name != "A");
    } else if name == "A" {
        let unknown = HostId::random();
        assert!(peers.values().all(|peer| peer.id != unknown));
        assert!(!native
            .service_terminal()
            .is_connected(ServiceHost::Remote(unknown)));
        let value = live_selection(root, 0x72);
        owner
            .send_selection(
                &TraceNodeId::new(unknown.to_string()),
                value,
                &TraceNodeId::new(local.to_string()),
            )
            .unwrap();
        println!("R1_LOCAL_ACCEPT role=A trace=72 unknown={unknown}; 后续仍可能异步失败");
        // 没有地址的合法目标由真实Swarm自己拨号失败；绝不手调handle/ingest事件。
        let actual = errors_rx
            .recv_timeout(WAIT)
            .expect("真实宿主异步通信错误必须到达");
        assert_failure_controls(&actual, unknown, peers["B"].id);
        println!(
            "R1_ASYNC_ERROR peer={} source={:?} reason={}",
            actual.peer_id,
            actual.source,
            actual.error.as_deref().unwrap()
        );
        let history = sends.lock().unwrap().clone();
        assert_eq!(history.len(), 1);
        assert_send(&history[0], unknown, local, value);
        let control = live_selection(root, 0x73);
        owner
            .send_selection(
                &TraceNodeId::new(peers["B"].id.to_string()),
                control,
                &TraceNodeId::new(local.to_string()),
            )
            .unwrap();
        wait_for(
            || root.join("B.selection-counted").is_file(),
            "B初步累计选择屏障（不是内容证据）",
        );
        let history = sends.lock().unwrap().clone();
        assert_eq!(history.len(), 2);
        assert_send(&history[1], peers["B"].id, local, control);
        assert_eq!(recorder.stats().selected_traces, 0);
    } else {
        wait_for(
            || recorder.stats().selected_traces == 1,
            "B初步累计选择1（内容随后独立核验）",
        );
        assert!(sends.lock().unwrap().is_empty());
        assert_eq!(recorder.stats().notification_attempts, 0);
        publish(
            root,
            "B.selection-counted",
            "selected=1; content not yet proved",
        );
    }

    // 所有角色先完成其事实，再在有限窗口内核对无意外提交/回调；不是全局flush保证。
    publish(root, &format!("{name}.checked"), "checked");
    for remote in names {
        wait_for(
            || root.join(format!("{remote}.checked")).is_file(),
            "其它角色证据完成",
        );
    }
    let start = Instant::now();
    while start.elapsed() < QUIET {
        assert!(warnings.0.lock().unwrap().is_empty());
        assert!(matches!(
            returned_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        assert!(
            matches!(errors_rx.try_recv(), Err(mpsc::TryRecvError::Empty)),
            "有效流或失败后不应出现额外错误"
        );
        let expected = if case == "chain" {
            1
        } else if name == "A" {
            2
        } else {
            0
        };
        assert_eq!(sends.lock().unwrap().len(), expected);
        assert_eq!(
            recorder.stats().selected_traces,
            u64::from(case == "chain" || name == "B")
        );
        assert_eq!(
            recorder.stats().notification_attempts,
            u64::from(case == "chain" && name != "A")
        );
        assert_eq!(recorder.stats().failed_notifications, 0);
        thread::sleep(Duration::from_millis(3));
    }
    // 原有限静默结束后才追加内容探针，不能让新登记掩盖原来的零回传断言。
    let content_observed = (case == "chain" && name == "A") || (case == "async" && name == "B");
    if content_observed {
        let probe_id = HostId::random();
        assert!(peers.values().all(|peer| peer.id != probe_id));
        let probe = TraceNodeId::new(probe_id.to_string());
        let expected = live_selection(root, if case == "chain" { 0x71 } else { 0x73 });
        let actual =
            observe_received_content(&owner, &recorder, &returned_rx, &warnings, &probe, expected);
        let receipt = received_content_receipt(&actual);
        println!("R1_RECEIVED_CONTENT role={name} {receipt} negative_controls=trace,deadline_minus_100ns,deadline_plus_100ns");
        if case == "async" {
            publish(root, "B.control-received", &receipt);
        }
    }
    if case == "async" && name == "A" {
        wait_for(
            || root.join("B.control-received").is_file(),
            "B实际回调内容核验完成",
        );
        let control = live_selection(root, 0x73);
        let expected = format!(
            "trace={} expires_at_100ns={}",
            control.trace_id,
            control
                .expires_at
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                / 100
        );
        assert_eq!(
            fs::read_to_string(root.join("B.control-received")).unwrap(),
            expected
        );
    }
    let quiet = Instant::now();
    while quiet.elapsed() < QUIET {
        let expected_sends = if case == "chain" {
            1
        } else if name == "A" {
            2
        } else {
            0
        };
        assert_eq!(sends.lock().unwrap().len(), expected_sends);
        assert!(matches!(
            returned_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        assert!(matches!(
            errors_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        assert!(warnings.0.lock().unwrap().is_empty());
        assert_eq!(
            recorder.stats().selected_traces,
            u64::from(case == "chain" || name == "B")
        );
        assert_eq!(
            recorder.stats().notification_attempts,
            u64::from(case == "chain" || content_observed)
        );
        assert_eq!(
            recorder.stats().failed_notifications,
            u64::from(content_observed)
        );
        thread::sleep(Duration::from_millis(3));
    }
    publish(root, &format!("{name}.observed"), "observed");
    wait_for(|| root.join("stop").is_file(), "宿主统一发出收尾指令");
    owner.close();
    guard.shutdown();
    assert_eq!(exports.load(Ordering::SeqCst), 0, "无Span输入不能意外导出");
    assert!(hub.unsubscribe(subscription));
    run(native.p2p_terminal().shutdown_host_runtime()).unwrap();
    drop(owner);
    drop(recorder);
    drop(terminal);
    drop(native);
    drop(runtime);
    drop(hub);
    assert!(warnings.0.lock().unwrap().is_empty());
    assert!(
        panics.lock().unwrap().is_empty(),
        "同步收尾完成后不得有意外panic"
    );
    println!("R1_NETWORK_CLOSED role={name}");
}

struct RoleProcess {
    role: &'static str,
    process: Child,
    status: Option<ExitStatus>,
    stdout: Option<thread::JoinHandle<String>>,
    stderr: Option<thread::JoinHandle<String>>,
}
impl RoleProcess {
    fn spawn(role: &'static str, test: &str, root: &Path, case: &str) -> Self {
        let mut process = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test, "--nocapture", "--test-threads=1"])
            .env(ROLE, role)
            .env(CASE, case)
            .env(ROOT, root)
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
        Self {
            role,
            process,
            status: None,
            stdout: Some(out),
            stderr: Some(err),
        }
    }
    fn collect(&mut self) -> String {
        let stdout = self.stdout.take().unwrap().join().unwrap();
        let stderr = self.stderr.take().unwrap().join().unwrap();
        let output = format!("{stdout}\n{stderr}");
        println!(
            "R1_ROLE role={} status={:?}\n{output}",
            self.role, self.status
        );
        output
    }
}
impl Drop for RoleProcess {
    fn drop(&mut self) {
        if self.status.is_none() {
            let _ = self.process.kill();
            let _ = self.process.wait();
        }
    }
}
struct TemporaryRoot(PathBuf);
impl Drop for TemporaryRoot {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).expect("所有角色wait退出后清理临时根");
    }
}

/// 父进程只协调角色屏障及退出；字节不经过控制文件，网络只走真实QUIC。
fn scenario<C: ServiceCodec<Input = Vec<u8>, Output = Vec<u8>>>(
    case: &str,
    codec: fn() -> C,
    construct: Constructor<C>,
) {
    if let Ok(name) = std::env::var(ROLE) {
        assert_eq!(std::env::var(CASE).unwrap(), case);
        role(
            &PathBuf::from(std::env::var(ROOT).unwrap()),
            &name,
            case,
            codec(),
            construct,
        );
        return;
    }
    let test = thread::current().name().unwrap().to_owned();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = TemporaryRoot(
        std::env::temp_dir().join(format!("pi-final-network-{}-{unique}", std::process::id())),
    );
    fs::create_dir(&root.0).unwrap();
    let names: &[&str] = if case == "async" {
        &["A", "B"]
    } else {
        &["A", "B", "C"]
    };
    let mut processes: Vec<_> = names
        .iter()
        .map(|name| RoleProcess::spawn(name, &test, &root.0, case))
        .collect();
    let start = Instant::now();
    let mut started = false;
    let mut stopping = false;
    let mut timed_out = false;
    loop {
        let mut early_failure = false;
        for child in &mut processes {
            if child.status.is_none() {
                child.status = child.process.try_wait().unwrap();
                if child
                    .status
                    .as_ref()
                    .is_some_and(|status| !status.success())
                {
                    early_failure = true;
                }
            }
        }
        if processes.iter().all(|child| child.status.is_some()) {
            break;
        }
        if early_failure || start.elapsed() >= HARD_LIMIT {
            timed_out = start.elapsed() >= HARD_LIMIT;
            for child in &mut processes {
                if child.status.is_none() {
                    let _ = child.process.kill();
                    child.status = Some(child.process.wait().unwrap());
                }
            }
            break;
        }
        if !started
            && names
                .iter()
                .all(|name| root.0.join(format!("{name}.prepared")).is_file())
        {
            let seconds = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 30;
            publish(&root.0, "deadline", &seconds.to_string());
            publish(&root.0, "go", "go");
            started = true;
        }
        if started
            && !stopping
            && names
                .iter()
                .all(|name| root.0.join(format!("{name}.observed")).is_file())
        {
            publish(&root.0, "stop", "stop");
            stopping = true;
        }
        thread::sleep(Duration::from_millis(5));
    }
    let outputs: Vec<_> = processes.iter_mut().map(RoleProcess::collect).collect();
    assert!(!timed_out, "角色超出60秒硬截止，不是有效RED");
    assert!(started && stopping, "角色未全部就绪或未完成观测");
    for (child, output) in processes.iter().zip(outputs.iter()) {
        assert!(
            child.status.as_ref().unwrap().success(),
            "角色{}失败",
            child.role
        );
        assert!(
            output.contains("running 1 test") && output.contains("1 passed; 0 failed; 0 ignored"),
            "零命中不能当证据"
        );
        assert!(output.contains(&format!("R1_NETWORK_CLOSED role={}", child.role)));
    }
}
pub(super) fn test_final_network_default_three_processes_receive_and_forward() {
    scenario("chain", default_codec, TracingCoordinator::new_default);
}
pub(super) fn test_final_network_multi_three_processes_receive_and_forward() {
    scenario("chain", multi_codec, TracingCoordinator::new_multi_layer);
}
pub(super) fn test_final_network_async_failure_preserves_host_granularity() {
    scenario("async", default_codec, TracingCoordinator::new_default);
}
