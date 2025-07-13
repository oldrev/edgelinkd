//! Trait for core-to-web decoupling: WebStateCore
//! Web crate 的 WebState 需实现本 trait，core handler 只依赖 trait，不依赖具体实现。

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::runtime::engine::Engine;
use crate::runtime::registry::RegistryHandle;
use crate::web::WebHandlerRegistry;

/// Trait for core-to-web decoupling: WebStateCore
pub trait WebStateCore: Send + Sync {
    /// 获取 Engine 实例
    fn engine(&self) -> &RwLock<Option<Arc<Engine>>>;
    /// 获取节点注册表
    fn registry(&self) -> &RwLock<Option<RegistryHandle>>;
    /// 获取静态文件目录
    fn static_dir(&self) -> &PathBuf;
    /// 获取 Web handler 注册表
    fn web_handlers(&self) -> &WebHandlerRegistry;
    /// 获取 flows.json 路径
    fn flows_file_path(&self) -> &RwLock<Option<PathBuf>>;
    /// 获取取消 token
    fn cancel_token(&self) -> &RwLock<Option<CancellationToken>>;
}
