//! slice-binding 测试实现：只经冻结 binding 壳及真实 protocol/pi_logger 出口驱动。
//! @api:src/binding.rs @impl:src/binding_inner.rs
//! @test:tests/binding/binding_test.rs
//! @design-doc:design/tracing-coordinator/07-design/call-chain.md#slice-binding
//!
//! 证据边界：不读取状态/锁/Weak，不导入生产芯，不伪造 DecodedSelection。
//! 真实记录器通过公开初始化创建，独立子进程隔离全局 subscriber/logger 与 TODO Drop 二次 panic。
//! selected_traces 只证明异步选择效果，不是 apply_remote_selection 调用计数。
//! 用户已确认的组合取证：docs/SLICE_BINDING_TEST_EVIDENCE.md#slice-binding-test-evidence。
//! 冻结 API 无中毒注入、发布前暂停和析构观测出口；真实可观察行为由本文件测试，
//! 不可见内部属性在实际实现后逐项独立源码审查，双方闭合前不能放行：
//! - R1：中毒只转Closed/移出旧值、保留中毒标志；trait分类不冒充真实中毒路径。
//! - R2：拒绝零次、准入恰一次apply_remote_selection；最终去重统计仍不是调用计数。
//! - R3/R4：Weak不升级/不取锁、临时持有及时释放、Owner主动关闭及锁外消费/调用/析构。
//! - R5：接收已准入后close可先返回且调用仍收尾；一次Barrier竞争不是命中内部窗口的证明。
//!
//! R6经用户批准的固定SDK开发依赖、公开start和双真实实例取证；不得用脚手架替代选择。
//! 目标10芯仍TODO时，依赖夹具绿色、测试可编译或完成观察方案都不代表binding实现通过。

use crate::binding::{BindingError, CoordinationBinding};
use crate::protocol::{selection_to_message, validate_selection_message, DecodedSelection};
use opentelemetry_sdk::{
    error::{OTelSdkError, OTelSdkResult},
    trace::{SpanData, SpanExporter},
    Resource,
};
use pi_logger::{
    FlightRecorder, FlightRecorderGuard, ObservabilityGuards, TraceId, TraceNodeId, TraceNotifier,
    TraceSelection,
};
use std::{
    error::Error,
    fmt,
    io::Read,
    net::TcpListener,
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, Barrier,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const CHILD_ENV: &str = "PI_SLICE_BINDING_TEST_CHILD";
const CHILD_LIMIT: Duration = Duration::from_secs(20);
const WAIT_LIMIT: Duration = Duration::from_secs(3);
const TRACE_BYTES: [u8; 16] = [
    1, 35, 69, 103, 137, 171, 205, 239, 254, 220, 186, 152, 118, 84, 50, 16,
];
// 拒绝输入与成功对照必须使用不同Trace，避免上游去重掩盖错误发布。
const CONTROL_TRACE_BYTES: [u8; 16] = [0xa5; 16];
const LOCAL: &str = "本地/节点-B";
const SOURCE: &str = "  节点é/e\u{301}🦀\0 ";

/// 子进程只运行当前声明单元；超时杀死并收集完整输出，不把环境失败当有效 RED。
/// 子进程内的断言没有替换/补全生产行为；返回成功必须以该场景真实正常退出为前提。
fn isolated(action: impl FnOnce()) {
    let current = thread::current();
    let name = current
        .name()
        .expect("libtest 应提供当前场景名称")
        .to_owned();
    if std::env::var(CHILD_ENV).ok().as_deref() == Some(name.as_str()) {
        action();
        return;
    }
    let executable = std::env::current_exe().expect("夹具失败：无法定位真实测试可执行文件");
    let mut child = Command::new(executable)
        .args(["--exact", name.as_str(), "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, &name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("夹具失败：不能启动隔离测试");
    let mut stdout = child.stdout.take().expect("子进程 stdout");
    let mut stderr = child.stderr.take().expect("子进程 stderr");
    let output_reader = thread::spawn(move || {
        let mut output = String::new();
        stdout
            .read_to_string(&mut output)
            .expect("读取子进程 stdout");
        output
    });
    let error_reader = thread::spawn(move || {
        let mut output = String::new();
        stderr
            .read_to_string(&mut output)
            .expect("读取子进程 stderr");
        output
    });
    let started = Instant::now();
    let mut timed_out = false;
    let status = loop {
        if let Some(status) = child.try_wait().expect("读取子进程状态") {
            break status;
        }
        if started.elapsed() >= CHILD_LIMIT {
            timed_out = true;
            child.kill().expect("终止超时测试子进程");
            break child.wait().expect("回收超时测试子进程");
        }
        thread::sleep(Duration::from_millis(5));
    };
    let output = output_reader.join().expect("stdout 收集线程退出");
    let errors = error_reader.join().expect("stderr 收集线程退出");
    assert!(
        output.contains("running 1 test"),
        "子进程没有准确命中一个场景，不是有效RED：{output}\n{errors}"
    );
    assert!(
        !timed_out,
        "测试未完成/非有效RED：{name}超过20秒；stdout={output}；stderr={errors}"
    );
    assert!(status.success(), "隔离场景失败：{name}，退出={status}；按最后 BINDING_STEP 区分目标或前置失败，非目标构造/Drop panic不是当前目标RED。\nstdout={output}\nstderr={errors}");
    assert!(
        output.contains("1 passed; 0 failed; 0 ignored"),
        "子进程没有完成唯一场景的成功断言：{output}\n{errors}"
    );
}

/// 明确下一次真实调用的身份，使构造等前置 TODO 不会被误报为其它目标的红线。
fn step(label: &str) {
    eprintln!("BINDING_STEP={label}");
}

/// 合法且稳定的未来期限为2100-01-01；不复制记录器的时效/裁剪算法。
fn future_deadline() -> SystemTime {
    UNIX_EPOCH
        .checked_add(Duration::from_secs(4_102_444_800))
        .expect("固定合法期限")
}

/// 只组合第1块已验收的合法出口；没有直接构造协议字段或借用生产输入。
fn decoded(bytes: [u8; 16], source: &str, expires_at: SystemTime) -> DecodedSelection {
    let selection = TraceSelection {
        trace_id: TraceId::from_bytes(bytes),
        expires_at,
    };
    let message = selection_to_message(selection, &TraceNodeId::new(source))
        .expect("前置失败：第1块发送转换，应保持已验收合法出口");
    validate_selection_message(message).expect("前置失败：第1块接收校验，应产生合法交接结果")
}

/// 外部 notifier 仅为真实记录器初始化提供合法宿主实现；未登记关联，不应发生通知。
/// 此实现不转发、不推进状态，也不伪造本库发送或记录器接收行为。
struct NoPeers;
impl TraceNotifier for NoPeers {
    fn notify(&self, remote: &TraceNodeId, selection: TraceSelection) -> anyhow::Result<()> {
        panic!(
            "夹具未登记关联，不应通知：remote={} trace={:?}",
            remote.as_str(),
            selection.trace_id
        );
    }
}

/// 仅观测真实记录器通知线程已发出的业务值；通道不推进或补全记录器状态。
struct ObservedNotice(mpsc::Sender<(TraceNodeId, TraceSelection)>);
impl TraceNotifier for ObservedNotice {
    fn notify(&self, remote: &TraceNodeId, selection: TraceSelection) -> anyhow::Result<()> {
        self.0
            .send((remote.clone(), selection))
            .map_err(|error| anyhow::anyhow!("通知观测接收端关闭：{error}"))
    }
}

/// 每个子进程独立创建真实记录器及依赖 worker，宿主守卫始终由夹具负责关闭。
/// 不创建 Span，不产生导出负载；保留独占回环端口以免依赖端点指向外部服务。
struct RecorderFixture {
    recorder: FlightRecorder,
    guards: Option<ObservabilityGuards>,
    _endpoint: TcpListener,
}
impl RecorderFixture {
    fn new(coordination: bool, local: &str) -> Self {
        Self::with_notifier(coordination, local, Arc::new(NoPeers))
    }
    /// 允许宿主在同一真实装配中观测后续通知；不替换记录器或binding。
    fn with_notifier(coordination: bool, local: &str, notifier: Arc<dyn TraceNotifier>) -> Self {
        step("前置:真实pi_logger公开初始化");
        let endpoint = TcpListener::bind(("127.0.0.1", 0)).expect("夹具回环端点");
        let mut config = pi_logger::observability::observability_config_from_rust_log(Some("off"))
            .expect("夹具配置");
        config.log.console = None;
        config.log.dynamic.enabled = false;
        config.trace.enabled = true;
        config.trace.endpoint = format!("http://{}", endpoint.local_addr().expect("回环地址"));
        config.trace.flight_recorder = Some(pi_logger::observability::FlightRecorderConfig {
            retention_secs: 60,
            followup_secs: 0,
            worker_poll_ms: 5,
            export_retries: 0,
            ..Default::default()
        });
        let (_, guards) = if coordination {
            pi_logger::init_observability_with_coordination(
                config,
                TraceNodeId::new(local),
                notifier,
            )
        } else {
            pi_logger::observability::init_observability(config)
        }
        .expect("夹具初始化失败，不是目标RED");
        let recorder = guards
            .flight_recorder
            .as_ref()
            .expect("真实记录器已启用")
            .recorder();
        assert_eq!(recorder.stats().selected_traces, 0);
        Self {
            recorder,
            guards: Some(guards),
            _endpoint: endpoint,
        }
    }
    fn stop(&mut self) {
        if let Some(guards) = self.guards.take() {
            guards.shutdown();
        }
    }
    /// 此统计只用于已发布事件的独立后续效果，不证明提交次数或同步完成。
    fn wait_selected(&self, expected: u64) {
        let started = Instant::now();
        loop {
            let actual = self.recorder.stats().selected_traces;
            assert!(
                actual <= expected,
                "出现额外不同Trace选择：actual={actual} expected={expected}"
            );
            if actual == expected {
                break;
            }
            assert!(
                started.elapsed() < WAIT_LIMIT,
                "真实worker未在3秒内形成预期效果：actual={actual} expected={expected}"
            );
            thread::sleep(Duration::from_millis(2));
        }
    }
}
impl Drop for RecorderFixture {
    fn drop(&mut self) {
        self.stop();
    }
}

/// 只观测宿主导出出口，不能参与接收/选择/绑定；没有Span的场景必须没有导出调用。
#[derive(Debug, Default)]
struct ExportObservation {
    export_calls: AtomicUsize,
    shutdown_calls: AtomicUsize,
}

/// start所需的合法宿主导出器；意外导出记录为失败，绝不替代真实FlightRecorder。
#[derive(Debug)]
struct ObservedExporter(Arc<ExportObservation>);
impl SpanExporter for ObservedExporter {
    async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
        self.0.export_calls.fetch_add(1, Ordering::SeqCst);
        Err(OTelSdkError::InternalFailure(
            "测试没有创建Span，不应发生导出".to_owned(),
        ))
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.0.shutdown_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// 经公开start建立独立真实实例，不安装全局subscriber；每实例有自己的Guard/Worker/身份。
/// SDK开发依赖已获用户明确批准，导出观察器只提供必要出口，不伪造选择效果。
struct StandaloneRecorderFixture {
    recorder: FlightRecorder,
    guard: Option<FlightRecorderGuard>,
    export_observation: Arc<ExportObservation>,
}
impl StandaloneRecorderFixture {
    fn new(local: &str) -> Self {
        step("前置:公开start构造独立真实记录器");
        let export_observation = Arc::new(ExportObservation::default());
        let guard = FlightRecorder::start(
            pi_logger::observability::FlightRecorderConfig {
                retention_secs: 60,
                followup_secs: 0,
                worker_poll_ms: 5,
                export_retries: 0,
                ..Default::default()
            },
            ObservedExporter(export_observation.clone()),
            Resource::builder_empty().build(),
        )
        .expect("夹具失败：公开start未能创建真实记录器");
        let recorder = guard.recorder();
        recorder
            .enable_coordination(TraceNodeId::new(local), Arc::new(NoPeers))
            .expect("夹具失败：独立真实记录器未能安装协调器");
        assert_eq!(recorder.stats().selected_traces, 0);
        Self {
            recorder,
            guard: Some(guard),
            export_observation,
        }
    }

    fn stop(&mut self) {
        if let Some(guard) = self.guard.take() {
            guard.shutdown();
        }
    }

    /// 只在真实Worker/导出线程已退出后读取，不以未处理的即时0快照证明无副作用。
    fn assert_stopped_without_export(&self) {
        assert!(self.guard.is_none(), "必须先由宿主关闭真实Guard");
        assert_eq!(
            self.export_observation.export_calls.load(Ordering::SeqCst),
            0
        );
        assert_eq!(
            self.export_observation
                .shutdown_calls
                .load(Ordering::SeqCst),
            1
        );
        assert_eq!(self.recorder.stats().notification_attempts, 0);
    }
}
impl Drop for StandaloneRecorderFixture {
    fn drop(&mut self) {
        // 断言显式执行；Drop只清理，避免目标TODO展开时由测试断言制造二次panic。
        self.stop();
    }
}

/// 校准真实装配；本例直接调用依赖接收出口，不计作 binding 功能通过证据。
pub(super) fn test_binding_real_recorder_fixture_controls() {
    isolated(|| {
        let mut fixture = RecorderFixture::new(true, LOCAL);
        let notice = TraceSelection {
            trace_id: TraceId::from_bytes(TRACE_BYTES),
            expires_at: future_deadline(),
        };
        assert!(!fixture
            .recorder
            .apply_remote_selection(notice, TraceNodeId::new(LOCAL)));
        assert!(!fixture.recorder.apply_remote_selection(
            TraceSelection {
                expires_at: UNIX_EPOCH,
                ..notice
            },
            TraceNodeId::new(SOURCE)
        ));
        assert_eq!(fixture.recorder.stats().selected_traces, 0);
        assert!(fixture
            .recorder
            .apply_remote_selection(notice, TraceNodeId::new(SOURCE)));
        fixture.wait_selected(1);
        fixture.stop();
        assert!(!fixture
            .recorder
            .apply_remote_selection(notice, TraceNodeId::new(SOURCE)));
        assert_eq!(fixture.recorder.stats().selected_traces, 1);
        assert_eq!(fixture.recorder.stats().notification_attempts, 0);
    });
}

/// 首次调用目标为new；后续准入/接收只是通过壳观察独立初态。
pub(super) fn test_binding_new_creates_independent_open_unbound_owners() {
    isolated(|| {
        step("目标:new");
        let first = CoordinationBinding::new();
        let second = CoordinationBinding::new();
        let first_handle = first.handle();
        let second_handle = second.handle();
        assert!(first_handle.admit_send().is_ok());
        assert!(second_handle.admit_send().is_ok());
        assert!(matches!(
            first_handle.submit_received(decoded(TRACE_BYTES, SOURCE, future_deadline())),
            Err(BindingError::RecorderUnavailable)
        ));
        first.bind_recorder(None).unwrap();
        first.close();
        assert!(matches!(
            first_handle.admit_send(),
            Err(BindingError::Closed)
        ));
        assert!(second_handle.admit_send().is_ok());
        assert!(matches!(
            second_handle.submit_received(decoded(TRACE_BYTES, SOURCE, future_deadline())),
            Err(BindingError::RecorderUnavailable)
        ));
        assert!(second.bind_recorder(None).is_ok());
    });
}

/// 多次handle与Clone后仍能完成唯一首次绑定；不以引用计数赋值当功能证据。
pub(super) fn test_binding_handles_and_clones_preserve_binding_opportunity() {
    isolated(|| {
        step("前置:new");
        let owner = CoordinationBinding::new();
        step("目标:handle/clone");
        let first = owner.handle();
        let second = owner.handle();
        let cloned = first.clone();
        assert!(owner.bind_recorder(None).is_ok());
        for handle in [&first, &second, &cloned] {
            assert!(handle.admit_send().is_ok());
            assert!(matches!(
                handle.submit_received(decoded(TRACE_BYTES, SOURCE, future_deadline())),
                Err(BindingError::RecorderUnavailable)
            ));
        }
        assert!(matches!(
            owner.bind_recorder(None),
            Err(BindingError::RecorderAlreadyBound)
        ));
        owner.close();
        for handle in [&first, &second, &cloned] {
            assert!(matches!(handle.admit_send(), Err(BindingError::Closed)));
        }
    });
}

/// Closed之后允许取得及复制弱句柄，但不重新开放准入或记录器绑定。
pub(super) fn test_binding_handle_and_clone_after_close_do_not_reopen() {
    isolated(|| {
        step("前置:new/close");
        let owner = CoordinationBinding::new();
        owner.close();
        step("目标:Closed状态handle/clone");
        let original = owner.handle();
        let cloned = original.clone();
        for handle in [&original, &cloned] {
            assert!(matches!(handle.admit_send(), Err(BindingError::Closed)));
            assert!(matches!(
                handle.submit_received(decoded(TRACE_BYTES, SOURCE, future_deadline())),
                Err(BindingError::Closed)
            ));
        }
        assert!(matches!(
            owner.bind_recorder(None),
            Err(BindingError::Closed)
        ));
    });
}

/// None消耗机会；后续None和Some均拒绝，接收仍不可用而发送保持开放。
pub(super) fn test_binding_first_none_cannot_be_replaced_by_none_or_some() {
    isolated(|| {
        let mut fixture = RecorderFixture::new(true, LOCAL);
        step("前置:new/handle");
        let owner = CoordinationBinding::new();
        let handle = owner.handle();
        step("目标:bind_recorder(None)");
        owner.bind_recorder(None).unwrap();
        assert!(matches!(
            owner.bind_recorder(None),
            Err(BindingError::RecorderAlreadyBound)
        ));
        assert!(matches!(
            owner.bind_recorder(Some(fixture.recorder.clone())),
            Err(BindingError::RecorderAlreadyBound)
        ));
        assert!(handle.admit_send().is_ok());
        assert!(matches!(
            handle.submit_received(decoded(TRACE_BYTES, SOURCE, future_deadline())),
            Err(BindingError::RecorderUnavailable)
        ));
        // 所有目标调用已返回；宿主关闭并等待真实Worker退出后读取最终效果。
        fixture.stop();
        assert_eq!(fixture.recorder.stats().selected_traces, 0);
    });
}

/// 首次真实A绑定后拒绝不同真实B和None；独立身份使错误替换无法借同实例去重蒙混。
/// B的本地身份设为SOURCE：目标输入对A合法、对B为自来源；另以LOCAL给B发布对照证明B存活。
/// 目标场景只经冻结binding壳交接；直接依赖调用仅为不经过binding的B存活校准。
pub(super) fn test_binding_first_some_survives_rejected_some_and_none() {
    isolated(|| {
        let mut original = StandaloneRecorderFixture::new(LOCAL);
        let mut candidate = StandaloneRecorderFixture::new(SOURCE);
        step("前置:new/handle");
        let owner = CoordinationBinding::new();
        let handle = owner.handle();
        step("目标:bind_recorder(Some(A))拒绝不同Some(B)/None");
        owner
            .bind_recorder(Some(original.recorder.clone()))
            .unwrap();
        for (replacement, bytes) in [
            (Some(candidate.recorder.clone()), TRACE_BYTES),
            (None, [0x5a; 16]),
        ] {
            assert!(matches!(
                owner.bind_recorder(replacement),
                Err(BindingError::RecorderAlreadyBound)
            ));
            // 每次拒绝后立即独立交接，不让后一次操作修正先前误替换而掩盖中间错误。
            // 若替换为B，来源对B是自身，真实依赖会false；若None清空则无法接收。
            handle
                .submit_received(decoded(bytes, SOURCE, future_deadline()))
                .unwrap();
        }
        // 唯一B校准输入和目标Trace可区分，排空后的B=1也能捕获错误的跨实例额外发布。
        assert!(candidate.recorder.apply_remote_selection(
            TraceSelection {
                trace_id: TraceId::from_bytes(CONTROL_TRACE_BYTES),
                expires_at: future_deadline(),
            },
            TraceNodeId::new(LOCAL),
        ));
        owner.close();
        original.stop();
        candidate.stop();
        assert_eq!(original.recorder.stats().selected_traces, 2);
        assert_eq!(candidate.recorder.stats().selected_traces, 1);
        original.assert_stopped_without_export();
        candidate.assert_stopped_without_export();
    });
}

/// 三种健康初态关闭后，Closed优先于重复；Some输入被按值移交，无返还出口。
pub(super) fn test_binding_closed_precedes_already_bound_for_every_healthy_state() {
    isolated(|| {
        let mut fixture = RecorderFixture::new(true, LOCAL);
        for state in 0..3 {
            step("前置:new/可选首次绑定/close");
            let owner = CoordinationBinding::new();
            match state {
                0 => {}
                1 => owner.bind_recorder(None).unwrap(),
                _ => owner.bind_recorder(Some(fixture.recorder.clone())).unwrap(),
            }
            owner.close();
            step("目标:关闭后绑定的错误优先级");
            assert!(matches!(
                owner.bind_recorder(None),
                Err(BindingError::Closed)
            ));
            assert!(matches!(
                owner.bind_recorder(Some(fixture.recorder.clone())),
                Err(BindingError::Closed)
            ));
        }
        // 所有目标调用已返回；宿主关闭并等待真实Worker退出后读取最终效果。
        fixture.stop();
        assert_eq!(fixture.recorder.stats().selected_traces, 0);
    });
}

/// 只返回单位成功；有限重复调用不产生可观察的记录器选择或通知。
pub(super) fn test_binding_admit_send_is_unit_success_in_all_open_states() {
    isolated(|| {
        let mut fixture = RecorderFixture::new(true, LOCAL);
        for state in 0..3 {
            step("前置:new/handle/可选绑定");
            let owner = CoordinationBinding::new();
            let handle = owner.handle();
            match state {
                0 => {}
                1 => owner.bind_recorder(None).unwrap(),
                _ => owner.bind_recorder(Some(fixture.recorder.clone())).unwrap(),
            }
            step("目标:admit_send开放状态");
            for _ in 0..3 {
                let result: Result<(), BindingError> = handle.admit_send();
                result.unwrap();
            }
        }
        // 所有目标调用已返回；宿主关闭并等待真实Worker退出后读取最终效果。
        fixture.stop();
        assert_eq!(fixture.recorder.stats().selected_traces, 0);
        assert_eq!(fixture.recorder.stats().notification_attempts, 0);
    });
}

/// 无记录器两状态的拒绝是binding语义，不依赖记录器自引用或期限拒绝。
pub(super) fn test_binding_receive_rejects_unbound_and_explicit_none() {
    isolated(|| {
        for bind_none in [false, true] {
            step("前置:new/handle/可选None");
            let owner = CoordinationBinding::new();
            let handle = owner.handle();
            if bind_none {
                owner.bind_recorder(None).unwrap();
            }
            step("目标:submit_received无记录器");
            assert!(matches!(
                handle.submit_received(decoded(TRACE_BYTES, SOURCE, future_deadline())),
                Err(BindingError::RecorderUnavailable)
            ));
            assert!(handle.admit_send().is_ok());
        }
    });
}

/// 已绑定真实记录器也必须由Closed先拒绝；新开放对象的成功作为记录器有效负面对照。
pub(super) fn test_binding_receive_closed_rejection_has_live_recorder_control() {
    isolated(|| {
        let mut fixture = RecorderFixture::new(true, LOCAL);
        step("前置:new/handle/绑定/close");
        let closed = CoordinationBinding::new();
        let handle = closed.handle();
        closed
            .bind_recorder(Some(fixture.recorder.clone()))
            .unwrap();
        closed.close();
        step("目标:submit_received关闭先行");
        assert!(matches!(
            handle.submit_received(decoded(TRACE_BYTES, SOURCE, future_deadline())),
            Err(BindingError::Closed)
        ));
        assert!(matches!(handle.admit_send(), Err(BindingError::Closed)));
        let control = CoordinationBinding::new();
        control
            .bind_recorder(Some(fixture.recorder.clone()))
            .unwrap();
        control
            .handle()
            .submit_received(decoded(CONTROL_TRACE_BYTES, SOURCE, future_deadline()))
            .unwrap();
        // 对照Trace与被拒绝Trace不同；若拒绝路径误发布，排空后会出现额外选择。
        fixture.wait_selected(1);
        control.close();
        fixture.stop();
        assert_eq!(fixture.recorder.stats().selected_traces, 1);
    });
}

/// 真实记录器发布成功才返回Ok；异步效果单独有截止地等待，不把返回当导出完成。
pub(super) fn test_binding_receive_publishes_to_real_recorder() {
    isolated(|| {
        let fixture = RecorderFixture::new(true, LOCAL);
        step("前置:new/handle/绑定");
        let owner = CoordinationBinding::new();
        let handle = owner.handle();
        owner.bind_recorder(Some(fixture.recorder.clone())).unwrap();
        step("目标:submit_received真实发布true");
        handle
            .submit_received(decoded(TRACE_BYTES, SOURCE, future_deadline()))
            .unwrap();
        fixture.wait_selected(1);
        assert_eq!(fixture.recorder.stats().notification_attempts, 0);
    });
}

/// 不同合法Trace产生不同选择，原始来源包含空白/非ASCII/NUL；这里只证明外部效果，不计调用次数。
pub(super) fn test_binding_receive_keeps_distinct_valid_decisions() {
    isolated(|| {
        let (notices_tx, notices_rx) = mpsc::channel();
        let fixture =
            RecorderFixture::with_notifier(true, LOCAL, Arc::new(ObservedNotice(notices_tx)));
        let observer = TraceNodeId::new("观测/节点-C");
        step("前置:new/handle/绑定");
        let owner = CoordinationBinding::new();
        let handle = owner.handle();
        owner.bind_recorder(Some(fixture.recorder.clone())).unwrap();
        let mut first = [0; 16];
        first[0] = 1;
        let mut last = [0; 16];
        last[15] = 1;
        // 取当前秒后的30秒、精确到整秒且低于本地60秒窗口，避免将上游裁剪当本库篡改。
        // 等待有3秒截止、子进程20秒截止；此处只为真实时效依赖准备合法输入。
        let seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let deadline = UNIX_EPOCH
            .checked_add(Duration::from_secs(seconds + 30))
            .unwrap();
        for (index, bytes) in [TRACE_BYTES, first, last].into_iter().enumerate() {
            // 宿主独立登记观测邻居，不由binding补关系；真实通知可反查交接后的Trace及绝对期限。
            assert!(fixture
                .recorder
                .record_peer(TraceId::from_bytes(bytes), observer.clone()));
            step("目标:submit_received不同合法决定");
            handle
                .submit_received(decoded(bytes, SOURCE, deadline))
                .unwrap();
            let (remote, actual) = notices_rx
                .recv_timeout(WAIT_LIMIT)
                .expect("真实通知线程在3秒内交出业务值");
            assert_eq!(remote.as_str(), observer.as_str());
            assert_eq!(actual.trace_id.to_bytes(), bytes);
            assert_eq!(actual.expires_at, deadline);
            fixture.wait_selected(index as u64 + 1);
        }
    });
}

/// 自来源由记录器拒绝而非binding判定；原始身份相等控制与不同来源成功控制共用真实记录器。
pub(super) fn test_binding_receive_self_source_is_event_not_published() {
    isolated(|| {
        let mut fixture = RecorderFixture::new(true, SOURCE);
        step("前置:new/handle/绑定");
        let owner = CoordinationBinding::new();
        let handle = owner.handle();
        owner.bind_recorder(Some(fixture.recorder.clone())).unwrap();
        step("目标:submit_received真实发布false/自来源");
        assert!(matches!(
            handle.submit_received(decoded(TRACE_BYTES, SOURCE, future_deadline())),
            Err(BindingError::EventNotPublished)
        ));
        handle
            .submit_received(decoded(CONTROL_TRACE_BYTES, LOCAL, future_deadline()))
            .unwrap();
        fixture.wait_selected(1);
        fixture.stop();
        assert_eq!(fixture.recorder.stats().selected_traces, 1);
    });
}

/// 协议合法的纪元期限已经过期，Some仍为可用记录器，错误必须是未发布而非不存在。
pub(super) fn test_binding_receive_expired_decision_is_event_not_published() {
    isolated(|| {
        let mut fixture = RecorderFixture::new(true, LOCAL);
        step("前置:new/handle/绑定");
        let owner = CoordinationBinding::new();
        let handle = owner.handle();
        owner.bind_recorder(Some(fixture.recorder.clone())).unwrap();
        step("目标:submit_received真实发布false/过期");
        assert!(matches!(
            handle.submit_received(decoded(TRACE_BYTES, SOURCE, UNIX_EPOCH)),
            Err(BindingError::EventNotPublished)
        ));
        handle
            .submit_received(decoded(CONTROL_TRACE_BYTES, SOURCE, future_deadline()))
            .unwrap();
        fixture.wait_selected(1);
        fixture.stop();
        assert_eq!(fixture.recorder.stats().selected_traces, 1);
    });
}

/// 真实Some尚未安装协调器时先未发布；随后合法安装的成功对照证明原因为真实依赖前置。
pub(super) fn test_binding_receive_uninstalled_recorder_is_event_not_published() {
    isolated(|| {
        let mut fixture = RecorderFixture::new(false, LOCAL);
        step("前置:new/handle/绑定");
        let owner = CoordinationBinding::new();
        let handle = owner.handle();
        owner.bind_recorder(Some(fixture.recorder.clone())).unwrap();
        step("目标:submit_received真实发布false/未安装协调器");
        assert!(matches!(
            handle.submit_received(decoded(TRACE_BYTES, SOURCE, future_deadline())),
            Err(BindingError::EventNotPublished)
        ));
        fixture
            .recorder
            .enable_coordination(TraceNodeId::new(LOCAL), Arc::new(NoPeers))
            .unwrap();
        handle
            .submit_received(decoded(CONTROL_TRACE_BYTES, SOURCE, future_deadline()))
            .unwrap();
        fixture.wait_selected(1);
        fixture.stop();
        assert_eq!(fixture.recorder.stats().selected_traces, 1);
    });
}

/// 宿主停止记录器不关闭binding；发送仍准入，Some接收只按真实false返回未发布。
pub(super) fn test_binding_receive_stopped_recorder_is_event_not_published() {
    isolated(|| {
        let mut fixture = RecorderFixture::new(true, LOCAL);
        step("前置:new/handle/绑定/宿主停止记录器");
        let owner = CoordinationBinding::new();
        let handle = owner.handle();
        owner.bind_recorder(Some(fixture.recorder.clone())).unwrap();
        fixture.stop();
        step("目标:submit_received真实发布false/已停止");
        assert!(matches!(
            handle.submit_received(decoded(TRACE_BYTES, SOURCE, future_deadline())),
            Err(BindingError::EventNotPublished)
        ));
        assert!(handle.admit_send().is_ok());
        assert_eq!(fixture.recorder.stats().selected_traces, 0);
    });
}

/// 重复close保持终态；共享Guard/记录器仍由宿主控制，直接调用依赖作局部存活控制。
pub(super) fn test_binding_close_is_idempotent_and_does_not_stop_host_recorder() {
    isolated(|| {
        let fixture = RecorderFixture::new(true, LOCAL);
        for state in 0..3 {
            step("前置:new/handle/可选绑定");
            let owner = CoordinationBinding::new();
            let handle = owner.handle();
            match state {
                0 => {}
                1 => owner.bind_recorder(None).unwrap(),
                _ => owner.bind_recorder(Some(fixture.recorder.clone())).unwrap(),
            }
            step("目标:close重复关闭");
            owner.close();
            owner.close();
            assert!(matches!(handle.admit_send(), Err(BindingError::Closed)));
            assert!(matches!(
                handle.submit_received(decoded(TRACE_BYTES, SOURCE, future_deadline())),
                Err(BindingError::Closed)
            ));
        }
        assert!(fixture.recorder.apply_remote_selection(
            TraceSelection {
                trace_id: TraceId::from_bytes(TRACE_BYTES),
                expires_at: future_deadline()
            },
            TraceNodeId::new(SOURCE)
        ));
        fixture.wait_selected(1);
    });
}

/// 只验证弱句柄的外部关闭行为，不假称已观察私有Weak升级或临时Arc保活分支。
pub(super) fn test_binding_owner_drop_closes_original_and_cloned_handles() {
    isolated(|| {
        let fixture = RecorderFixture::new(true, LOCAL);
        for state in 0..3 {
            step("前置:new/handle/clone/可选绑定");
            let owner = CoordinationBinding::new();
            let handle = owner.handle();
            let cloned = handle.clone();
            match state {
                0 => {}
                1 => owner.bind_recorder(None).unwrap(),
                _ => owner.bind_recorder(Some(fixture.recorder.clone())).unwrap(),
            }
            step("目标:Owner Drop");
            drop(owner);
            for access in [&handle, &cloned] {
                assert!(matches!(access.admit_send(), Err(BindingError::Closed)));
                assert!(matches!(
                    access.submit_received(decoded(TRACE_BYTES, SOURCE, future_deadline())),
                    Err(BindingError::Closed)
                ));
            }
        }
        assert!(fixture.recorder.apply_remote_selection(
            TraceSelection {
                trace_id: TraceId::from_bytes(TRACE_BYTES),
                expires_at: future_deadline()
            },
            TraceNodeId::new(SOURCE)
        ));
        fixture.wait_selected(1);
    });
}

/// 发送准入已经返回后再关闭；通道只控制测试顺序，不提供发送或替代本库状态。
pub(super) fn test_binding_send_admitted_before_close_keeps_unit_success() {
    isolated(|| {
        step("前置:new/handle");
        let owner = CoordinationBinding::new();
        let handle = owner.handle();
        let (admitted_tx, admitted_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        step("目标:先发送准入后close");
        thread::scope(|scope| {
            let operation = scope.spawn(move || {
                let result: Result<(), BindingError> = handle.admit_send();
                admitted_tx.send(result.is_ok()).unwrap();
                release_rx
                    .recv_timeout(WAIT_LIMIT)
                    .expect("关闭后释放调用方");
                result
            });
            assert!(admitted_rx.recv_timeout(WAIT_LIMIT).expect("准入已返回"));
            owner.close();
            assert!(matches!(
                owner.handle().admit_send(),
                Err(BindingError::Closed)
            ));
            release_tx.send(()).unwrap();
            operation.join().expect("调用线程正常退出").unwrap();
        });
    });
}

/// 精确控制close返回先于另一线程进入API，不依赖sleep碰运气形成关闭先行。
pub(super) fn test_binding_close_before_concurrent_operations_rejects_both() {
    isolated(|| {
        let mut fixture = RecorderFixture::new(true, LOCAL);
        step("前置:new/绑定/两个handle");
        let owner = CoordinationBinding::new();
        owner.bind_recorder(Some(fixture.recorder.clone())).unwrap();
        let send = owner.handle();
        let receive = owner.handle();
        let gate = Barrier::new(3);
        step("目标:close先行/并发新操作");
        thread::scope(|scope| {
            let sender = scope.spawn(|| {
                gate.wait();
                send.admit_send()
            });
            let receiver = scope.spawn(|| {
                gate.wait();
                receive.submit_received(decoded(TRACE_BYTES, SOURCE, future_deadline()))
            });
            // close目标仍可能为TODO；先保留原panic并释放测试Barrier，避免scope回收
            // 等待未释放的测试线程，把目标panic误变成夹具超时。随后原样恢复panic。
            let closed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| owner.close()));
            gate.wait();
            if let Err(error) = closed {
                std::panic::resume_unwind(error);
            }
            assert!(matches!(sender.join().unwrap(), Err(BindingError::Closed)));
            assert!(matches!(
                receiver.join().unwrap(),
                Err(BindingError::Closed)
            ));
        });
        fixture.stop();
        assert_eq!(fixture.recorder.stats().selected_traces, 0);
    });
}

/// 一次真实关闭竞争只接受两种合法结果；不要求命中“准入后尚未发布”的不可见窗口。
pub(super) fn test_binding_bounded_receive_close_race_has_only_legal_outcomes() {
    isolated(|| {
        let mut fixture = RecorderFixture::new(true, LOCAL);
        step("前置:new/handle/绑定");
        let owner = CoordinationBinding::new();
        let handle = owner.handle();
        owner.bind_recorder(Some(fixture.recorder.clone())).unwrap();
        let gate = Barrier::new(2);
        step("目标:一次真实submit_received/close竞争");
        let result = thread::scope(|scope| {
            let receiver = scope.spawn(|| {
                gate.wait();
                handle.submit_received(decoded(TRACE_BYTES, SOURCE, future_deadline()))
            });
            gate.wait();
            owner.close();
            receiver.join().expect("接收线程正常退出")
        });
        let expected = match result {
            Ok(()) => 1,
            Err(BindingError::Closed) => 0,
            other => panic!("健康真实竞争只能成功或Closed：{other:?}"),
        };
        if expected == 1 {
            fixture.wait_selected(1);
        }
        fixture.stop();
        assert_eq!(fixture.recorder.stats().selected_traces, expected);
        assert!(matches!(handle.admit_send(), Err(BindingError::Closed)));
    });
}

/// 直接构造无资源错误只用于trait分类，不冒称已触发锁中毒等生产路径。
fn errors() -> [BindingError; 5] {
    [
        BindingError::Closed,
        BindingError::RecorderAlreadyBound,
        BindingError::RecorderUnavailable,
        BindingError::EventNotPublished,
        BindingError::StatePoisoned,
    ]
}

/// 不锁死标点或完整句子；每个分类必须包含正向语义，且五类不能混同。
pub(super) fn test_binding_error_display_classifies_five_semantic_errors() {
    step("目标:BindingError Display");
    let texts: Vec<String> = errors().iter().map(ToString::to_string).collect();
    let lower: Vec<String> = texts.iter().map(|text| text.to_lowercase()).collect();
    assert!(texts[0].contains("关闭") || lower[0].contains("closed"));
    assert!(
        (texts[1].contains("绑定") || lower[1].contains("bound"))
            && (texts[1].contains("已")
                || texts[1].contains("重复")
                || lower[1].contains("already"))
    );
    assert!(
        (texts[2].contains("记录器") || lower[2].contains("recorder"))
            && (texts[2].contains("不可用")
                || texts[2].contains("未绑定")
                || lower[2].contains("unavailable"))
    );
    assert!(
        (texts[3].contains("事件") || lower[3].contains("event"))
            && (texts[3].contains("未发布")
                || texts[3].contains("未能发布")
                || lower[3].contains("not published"))
    );
    assert!(texts[4].contains("中毒") || lower[4].contains("poison"));
    // EventNotPublished只保留未发布事实，不把依赖某个拒绝条件写成已知唯一原因。
    for claimed_reason in [
        "已关闭",
        "过期",
        "未安装",
        "未绑定",
        "closed",
        "expired",
        "not installed",
        "unbound",
    ] {
        let qualified = texts[3].contains("可能")
            || texts[3].contains("未知")
            || lower[3].contains("may")
            || lower[3].contains("possible")
            || lower[3].contains("unknown");
        assert!(
            !lower[3].contains(claimed_reason) || qualified,
            "不能把未知未发布原因断言为已知事实：{}",
            texts[3]
        );
    }
    for left in 0..texts.len() {
        for right in (left + 1)..texts.len() {
            assert_ne!(texts[left], texts[right]);
        }
    }
    for text in &texts {
        assert!(
            !text.contains("MutexGuard")
                && !text.contains("PoisonError")
                && !text.contains("BindingInner")
        );
    }
}

/// 真实格式化输出目标，可在任意不足容量处失败；失败后继续写会被独立断言捕获。
struct LimitedWriter {
    remaining: usize,
    failed: bool,
    output: String,
}
impl fmt::Write for LimitedWriter {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        assert!(!self.failed, "写入失败后不得继续写入");
        if text.len() > self.remaining {
            self.failed = true;
            return Err(fmt::Error);
        }
        self.remaining -= text.len();
        self.output.push_str(text);
        Ok(())
    }
}

/// 每种错误穷举不足输出容量，并提供足量容量成功对照；不把TODO panic当写入失败。
pub(super) fn test_binding_error_display_propagates_formatter_failure() {
    step("目标:BindingError Display格式化失败传播");
    for error in errors() {
        let expected = error.to_string();
        assert!(!expected.is_empty());
        for remaining in 0..expected.len() {
            let mut writer = LimitedWriter {
                remaining,
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
        assert!(!writer.failed);
        assert_eq!(writer.output, expected);
    }
}

/// 无cause不是吞PoisonError：五种错误本身均无源，重复查询也不改变结果。
pub(super) fn test_binding_error_source_is_none_for_all_five_errors() {
    step("目标:BindingError source");
    for error in errors() {
        assert!(error.source().is_none());
        assert!(error.source().is_none());
    }
}

/// 独立校准内容观测器，不经binding、不计为binding功能通过；没有手工推动Worker。
pub(super) fn test_binding_real_recorder_notification_fixture_control() {
    isolated(|| {
        let (notices_tx, notices_rx) = mpsc::channel();
        let fixture =
            RecorderFixture::with_notifier(true, LOCAL, Arc::new(ObservedNotice(notices_tx)));
        let observer = TraceNodeId::new("观测/节点-C");
        let seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let deadline = UNIX_EPOCH
            .checked_add(Duration::from_secs(seconds + 30))
            .unwrap();
        assert!(fixture
            .recorder
            .record_peer(TraceId::from_bytes(TRACE_BYTES), observer.clone()));
        assert!(fixture.recorder.apply_remote_selection(
            TraceSelection {
                trace_id: TraceId::from_bytes(TRACE_BYTES),
                expires_at: deadline
            },
            TraceNodeId::new(SOURCE)
        ));
        let (remote, actual) = notices_rx
            .recv_timeout(WAIT_LIMIT)
            .expect("真实通知应能观测业务内容");
        assert_eq!(remote.as_str(), observer.as_str());
        assert_eq!(actual.trace_id.to_bytes(), TRACE_BYTES);
        assert_eq!(actual.expires_at, deadline);
        fixture.wait_selected(1);
    });
}

/// 用真实依赖校准R2的不同Trace及最终观测，不经过binding、不计为目标实现绿色。
/// 同一Trace提交两次仅形成一个选择；不同Trace保证错误发布不会被成功对照的去重掩盖。
/// 只发布三条同步已完成事件，再由宿主停止Worker；不读取依赖内部状态或人工推进Worker。
pub(super) fn test_binding_real_recorder_distinct_final_effect_control() {
    isolated(|| {
        let mut fixture = RecorderFixture::new(true, LOCAL);
        assert_ne!(TRACE_BYTES, CONTROL_TRACE_BYTES);
        for bytes in [TRACE_BYTES, TRACE_BYTES, CONTROL_TRACE_BYTES] {
            assert!(fixture.recorder.apply_remote_selection(
                TraceSelection {
                    trace_id: TraceId::from_bytes(bytes),
                    expires_at: future_deadline(),
                },
                TraceNodeId::new(SOURCE),
            ));
        }
        // 没有以先读到0或先等到2代替完成观测；shutdown负责等待真实Worker退出。
        fixture.stop();
        assert_eq!(fixture.recorder.stats().selected_traces, 2);
        assert_eq!(fixture.recorder.stats().notification_attempts, 0);
    });
}

/// 独立校准两个公开start创建的真实实例、来源区分及关闭隔离；不计为binding绿色。
pub(super) fn test_binding_real_recorder_independent_instances_control() {
    isolated(|| {
        let mut original = StandaloneRecorderFixture::new(LOCAL);
        let mut candidate = StandaloneRecorderFixture::new(SOURCE);
        let selection = TraceSelection {
            trace_id: TraceId::from_bytes(TRACE_BYTES),
            expires_at: future_deadline(),
        };
        assert!(original
            .recorder
            .apply_remote_selection(selection, TraceNodeId::new(SOURCE)));
        assert!(!candidate
            .recorder
            .apply_remote_selection(selection, TraceNodeId::new(SOURCE)));
        // A关闭并排空不停止B；B仍接受不同来源、独立Trace的合法发布。
        original.stop();
        assert_eq!(original.recorder.stats().selected_traces, 1);
        assert!(candidate.recorder.apply_remote_selection(
            TraceSelection {
                trace_id: TraceId::from_bytes(CONTROL_TRACE_BYTES),
                expires_at: future_deadline(),
            },
            TraceNodeId::new(LOCAL),
        ));
        candidate.stop();
        assert_eq!(candidate.recorder.stats().selected_traces, 1);
        assert!(!original
            .recorder
            .apply_remote_selection(selection, TraceNodeId::new(SOURCE)));
        assert!(!candidate
            .recorder
            .apply_remote_selection(selection, TraceNodeId::new(LOCAL)));
        original.assert_stopped_without_export();
        candidate.assert_stopped_without_export();
    });
}
