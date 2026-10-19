// 设计：tracing-coordinator
// slice-binding十芯已实现；下列步骤注释保留冻结设计预期。
// 行为入口：tests/binding/binding_test.rs；内部属性独立审查已通过，验收见docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-binding。
// 取证：docs/SLICE_BINDING_TEST_EVIDENCE.md#slice-binding-test-evidence。

use super::{
    BindingError, BindingInner, BindingState, CoordinationBinding, CoordinationHandle,
    DecodedSelection,
};
use std::{
    error::Error,
    fmt,
    sync::{Arc, Mutex},
};

// 创建独立绑定：
// 1. 建立仅保存OpenUnbound状态互斥锁的私有容器。
// 2. 用Arc建立唯一生命周期所有者，交出CoordinationBinding。
// 后置：绑定机会未消耗，不保存协议资源，不安装记录器或服务、不创建线程。
pub(super) fn new() -> CoordinationBinding {
    // 每次独立分配唯一权威状态，不借用或缓存其它Owner的绑定机会。
    CoordinationBinding {
        inner: Arc::new(BindingInner {
            state: Mutex::new(BindingState::OpenUnbound),
        }),
    }
}

// 建立弱句柄：
// 从所有者强引用创建Weak并封装；不升级、不检查状态、不消耗绑定机会。
// 后置：不授予锁/记录器/关闭权限，不延长所有者生命周期。
pub(super) fn handle(binding: &CoordinationBinding) -> CoordinationHandle {
    // 仅建立Weak；即使Closed也不检查状态，不取得锁或临时强引用。
    CoordinationHandle {
        inner: Arc::downgrade(&binding.inner),
    }
}

// 一次绑定：
// 1. 接管新Option<FlightRecorder>，先不写入状态。
// 2. 获取私有状态锁。
//    中毒：仅为清理取得守卫，整体转Closed并移出旧状态；
//          先释放锁，再释放旧/新记录器，返回StatePoisoned，不清除中毒。
// 3. 健康锁下按状态裁决：
//    Closed：保持Closed，返回Closed，优先于重复绑定。
//    OpenBound：保持原值，返回RecorderAlreadyBound，不覆盖。
//    OpenUnbound：把输入原样移入OpenBound，None也消耗一次机会。
// 4. 释放锁，再释放失败时剩余新输入，返回结果。
// 后置：没有记录器调用、日志或锁内析构，没有替换或重开窗口。
pub(super) fn bind_recorder(
    binding: &CoordinationBinding,
    mut recorder: Option<pi_logger::FlightRecorder>,
) -> Result<(), BindingError> {
    // 结果只携带语义错误与移出的旧值；每个分支的守卫均先于这些值释放。
    // 失败输入一直留在锁外局部变量，不因重复绑定而替换原记录器。
    let (result, previous) = match binding.inner.state.lock() {
        Ok(mut state) => match &*state {
            BindingState::Closed => (Err(BindingError::Closed), None),
            BindingState::OpenBound(_) => (Err(BindingError::RecorderAlreadyBound), None),
            BindingState::OpenUnbound => {
                let previous =
                    std::mem::replace(&mut *state, BindingState::OpenBound(recorder.take()));
                (Ok(()), Some(previous))
            }
        },
        Err(poisoned) => {
            // 中毒只允许取得守卫完成终态清理；保留中毒标志，后续不再开放。
            let mut state = poisoned.into_inner();
            let previous = std::mem::replace(&mut *state, BindingState::Closed);
            (Err(BindingError::StatePoisoned), Some(previous))
        }
    };
    // match已结束，锁及包含守卫的LockResult均已释放；析构可以安全地发生在锁外。
    drop(previous);
    drop(recorder);
    result
}

// 关闭：
// 1. 获取私有状态锁；中毒时只取得守卫进行终态清理。
// 2. 整体移出旧状态，替换为Closed；已Closed也保持终态。
// 3. 释放状态锁。
// 4. 锁外释放旧状态中的记录器持有，返回。
// 后置：新准入拒绝，不等待已准入者，不清除中毒、不清理client/params的codec或宿主共享资源。
pub(super) fn close(binding: &CoordinationBinding) {
    // 不因中毒跳过关闭，也不清除中毒；整份旧状态只移动，不在持锁期间析构。
    let previous = {
        let mut state = match binding.inner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        std::mem::replace(&mut *state, BindingState::Closed)
    };
    // 同锁转终态即停止新准入；已准入调用持有自己的临时记录器，无需等待。
    drop(previous);
}

// 复制弱句柄：
// 只复制Weak并封装，不升级、不取状态锁、不复制生命周期所有者。
pub(super) fn clone_handle(handle: &CoordinationHandle) -> CoordinationHandle {
    CoordinationHandle {
        inner: handle.inner.clone(),
    }
}

// 所有者析构：
// 在释放自身Arc字段之前，采用已确认close政策主动转Closed并锁外释放记录器。
// 不因尚有临时强引用就推迟关闭；不等待在途调用或宿主worker。
// 复用已实现close出口，关闭不依赖最后一个Arc释放。
pub(super) fn drop_binding(binding: &mut CoordinationBinding) {
    // Owner析构在自身Arc字段释放前主动关闭；不能把最后Arc释放当作关闭条件。
    // 复用同一终态/中毒政策，临时保活状态的操作仍受Closed约束。
    close(binding);
}

// 发送准入：
// 1. 临时升级Weak；失效立即Closed。
// 2. 获取私有状态锁；中毒仅转Closed/移出旧值，解锁释放后返回StatePoisoned。
// 3. 健康锁下开放未绑定/已绑定均返回单位成功，Closed拒绝。
// 4. 释放锁，再释放临时强引用，返回结果。
// 后置：不交协议资源或token，不建在途计数；准入后close不能撤销。
// 不编码、发送或调用记录器。
pub(super) fn admit_send(handle: &CoordinationHandle) -> Result<(), BindingError> {
    let inner = handle.inner.upgrade().ok_or(BindingError::Closed)?;
    let (result, previous) = match inner.state.lock() {
        Ok(state) => {
            let result = match &*state {
                BindingState::OpenUnbound | BindingState::OpenBound(_) => Ok(()),
                BindingState::Closed => Err(BindingError::Closed),
            };
            (result, None)
        }
        Err(poisoned) => {
            let mut state = poisoned.into_inner();
            let previous = std::mem::replace(&mut *state, BindingState::Closed);
            (Err(BindingError::StatePoisoned), Some(previous))
        }
    };
    // 单位结果不携带状态或在途凭据；守卫先结束，再释放旧值及本次临时保活。
    // 返回后close不能撤回已判定的成功，client随后独立推进编码与唯一提交。
    drop(previous);
    drop(inner);
    result
}

// 接收交接：
// 前提：message已由protocol完整校验，调用者不能伪造私有字段。
// 1. 接管message并临时升级Weak；失效返回Closed，输入锁外释放。
// 2. 获取状态锁。
//    中毒：仅为清理取守卫，转Closed/移出旧状态，释放锁后清理资源及输入，
//          返回StatePoisoned，不清除中毒。
// 3. 健康锁下：
//    Closed：确定Closed错误。
//    OpenUnbound或OpenBound(None)：确定RecorderUnavailable。
//    OpenBound(Some)：只克隆共享记录器，本次准入成立。
// 4. 释放锁。拒绝则锁外释放输入及临时引用，不返还message、不调用记录器。
// 5. 准入成功才锁外消费DecodedSelection::into_parts，取得selection/source。
// 6. 锁外调用apply_remote_selection恰一次；true返回Ok，false返回EventNotPublished。
//    不重试、不猜false唯一原因、不调用record_peer、不重复协议校验。
// 7. 所有临时绑定/记录器/输入局部资源均锁外释放。
// 后置：准入后可以被close交错，仍允许完成该次已经获准的事件发布；
//       Ok仅证明本地事件发布，不证明处理、传播或导出完成。
pub(super) fn submit_received(
    handle: &CoordinationHandle,
    message: DecodedSelection,
) -> Result<(), BindingError> {
    let inner = handle.inner.upgrade().ok_or(BindingError::Closed)?;
    let (admission, previous) = match inner.state.lock() {
        Ok(state) => {
            let admission = match &*state {
                BindingState::Closed => Err(BindingError::Closed),
                BindingState::OpenUnbound | BindingState::OpenBound(None) => {
                    Err(BindingError::RecorderUnavailable)
                }
                // FlightRecorder的Clone只共享Arc；不在此锁内调用记录器或消费输入。
                BindingState::OpenBound(Some(recorder)) => Ok(recorder.clone()),
            };
            (admission, None)
        }
        Err(poisoned) => {
            let mut state = poisoned.into_inner();
            let previous = std::mem::replace(&mut *state, BindingState::Closed);
            (Err(BindingError::StatePoisoned), Some(previous))
        }
    };
    // 守卫和LockResult已结束；无论拒绝或放行，所有清理都发生在状态锁之外。
    drop(previous);
    drop(inner);
    let recorder = admission?;
    // 只在准入成功后拆解合法结果；不再检查Closed，close可先返回而本次继续收尾。
    // 精确零/一次调用与锁外释放的独立审查入口：SLICE_BINDING_TEST_EVIDENCE的R2/R4/R5。
    let (selection, source) = message.into_parts();
    let published = recorder.apply_remote_selection(selection, source);
    drop(recorder);
    if published {
        Ok(())
    } else {
        // false仅说明本地事件未发布，不能猜测为过期/自来源/关闭，也不重试或缓存。
        Err(BindingError::EventNotPublished)
    }
}

// 显示绑定错误：
// 分别描述关闭、重复绑定、记录器不可用、事件未发布及状态中毒；
// 不把事件未发布改写为已知唯一原因，不输出内部守卫或输入。
// 写出失败传播fmt::Error，不改变状态。
pub(super) fn fmt_binding_error(error: &BindingError, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    // 五类仅描述已知事实，尤其未发布不能推断关闭或过期；写出结果原样传播。
    // 验证：tests/binding/binding_test.rs 的两条 error_display 场景。
    f.write_str(match error {
        BindingError::Closed => "协调绑定已关闭",
        BindingError::RecorderAlreadyBound => "记录器已绑定，不能重复绑定",
        BindingError::RecorderUnavailable => "记录器不可用",
        BindingError::EventNotPublished => "记录器事件未发布",
        BindingError::StatePoisoned => "绑定状态锁中毒，已关闭",
    })
}

// 绑定错误没有底层源，五类均返回None，不分配或读取绑定状态。
pub(super) fn binding_error_source(_error: &BindingError) -> Option<&(dyn Error + 'static)> {
    None
}
