// 设计：tracing-coordinator
// slice-service四芯已实现；下列步骤保留冻结设计预期。
// 行为入口：tests/service/service_test.rs；独立审查已通过，验收见docs/IMPLEMENTATION_ACCEPTANCE.md#acceptance-service。
// 设计：design/tracing-coordinator/07-design/call-chain.md#slice-service。

use super::CoordinationServiceContext;
use crate::{
    binding::CoordinationHandle,
    protocol::{validate_selection_message, SelectionMessage},
};
use pi_service::{DefaultCodec, MultiLayerCodec, Service};

// new_context
//   接管弱binding，组成context
//   不升级、绑定、登记或发布上下文
pub(super) fn new_context(binding: CoordinationHandle) -> CoordinationServiceContext {
    // 只移动不透明弱句柄：关闭状态不影响组合，不升级、取锁或发布到宿主。
    // 验证：test_service_context_accepts_closed_handle_without_installing及params生命周期场景。
    CoordinationServiceContext { binding }
}

// default_service
//   交出DefaultCodec宏生成的静态服务借用
//   不注册或初始化
pub(super) fn default_service() -> &'static dyn Service<Codec = DefaultCodec> {
    // 借用标准宏生成的同一static；注册、bind_params及init仍由宿主框架推进。
    &super::__PI_RECEIVE_SELECTION_DEFAULT_SERVICE__
}

// multi_layer_service
//   交出MultiLayerCodec宏生成的静态服务借用
//   不注册或初始化
pub(super) fn multi_layer_service() -> &'static dyn Service<Codec = MultiLayerCodec> {
    // 只返回对应codec的宏static，不切层或安装；每进程择一的装配责任保持在宿主。
    &super::__PI_RECEIVE_SELECTION_MULTI_LAYER_SERVICE__
}

// receive_selection
//   消费消息并调用protocol校验
//   校验失败：尝试一次validate警告，然后结束
//   校验成功：把合法结果交binding一次
//   交接失败：在binding返回后尝试一次submit警告
//   结束；不重试、补偿、保存输入或返回远端应答
pub(super) fn receive_selection(context: &CoordinationServiceContext, message: SelectionMessage) {
    // 消费完整输入后才决定是否交接；即使绑定已关闭，也不能覆盖protocol首错。
    // 错误仅含静态分类/实际UTF-8原因，不把原消息、来源或Trace带入日志。
    let message = match validate_selection_message(message) {
        Ok(message) => message,
        Err(error) => {
            tracing::warn!(
                target: "pi_tracing_coordinator::service",
                service_port = crate::COORDINATION_SERVICE_PORT,
                stage = "validate",
                error = %error,
            );
            return;
        }
    };
    // 唯一交接：binding负责准入、锁外消费/记录器调用及临时资源释放。
    // 返回后才尝试警告，允许宿主subscriber重入；不再检查Closed或猜未发布原因。
    // 验证：两适配submit_log_runs_after_binding_return及拒绝/成功场景；全分支独立审查已归档。
    if let Err(error) = context.binding.submit_received(message) {
        tracing::warn!(
            target: "pi_tracing_coordinator::service",
            service_port = crate::COORDINATION_SERVICE_PORT,
            stage = "submit",
            error = %error,
        );
    }
}
