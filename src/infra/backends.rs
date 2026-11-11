//! 存储路由：**未配置的客户走 default，配置过的客户走自己的后端**。
//!
//! 为什么需要这一层：`wist-field` 要对多个客户提供**物理隔离**的存储（每客户独立 PG +
//! 独立对象目录），而数据面投递只有一个入口、一份配置（`cust_id` 在请求体里，反代看不到）。
//! 所以"分流"只能发生在服务端，判据就是写请求里的 `cust_id`、读请求里的 `x-evidence-tenant`。
//!
//! 不配置 `[[storage.cell]]` 时，`cells` 为空 —— 行为与单后端版本完全一致（升级无感）。

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::infra::{Index, ObjectStore};

/// 一个完整的存储后端：索引 + 对象 + 桶名。
///
/// 桶名跟着后端走（不同客户可能落在不同 bucket），所以它不能留在 `EvidenceService` 上。
#[derive(Clone)]
pub struct Backend {
    pub index: Arc<dyn Index>,
    pub objects: Arc<dyn ObjectStore>,
    pub bucket: String,
}

/// 客户 → 后端的路由表。
pub struct Backends {
    default: Backend,
    cells: BTreeMap<String, Backend>,
}

impl Backends {
    pub fn new(default: Backend, cells: BTreeMap<String, Backend>) -> Self {
        Self { default, cells }
    }

    /// 单个后端的便捷构造（不配置 cells 时的默认形态）。
    pub fn single(default: Backend) -> Self {
        Self::new(default, BTreeMap::new())
    }

    /// 取某个客户的后端：**未登记的客户落 default**。
    ///
    /// 为什么不是拒绝：写侧来自数据面（wparse），它只负责把记录投出来；这里回 4xx
    /// 会让数据进引擎的 rescue 文件、并且和"客户还没在栈里登记"这种运维动作绑在一起。
    /// 落 default 至少盘住了数据，配合 `warn` 日志能在现场一眼看出"这个 cust_id 没配 cell"。
    pub fn for_tenant(&self, tenant: &str) -> &Backend {
        self.cells.get(tenant).unwrap_or(&self.default)
    }

    /// 这个客户是否有专属后端（用于日志/自述，不参与路由判断）。
    pub fn has_cell(&self, tenant: &str) -> bool {
        self.cells.contains_key(tenant)
    }

    pub fn default(&self) -> &Backend {
        &self.default
    }

    /// 已登记的客户 id（升序）。给 `/healthz` 与启动日志用。
    pub fn cell_ids(&self) -> Vec<&str> {
        self.cells.keys().map(String::as_str).collect()
    }

    pub fn is_multi(&self) -> bool {
        !self.cells.is_empty()
    }
}

/// 客户 id 出现在**未登记**位置时统一打这一行。
///
/// 现场最常见的两类原因：① 新客户还没在栈里登记 cell（数据全进 default）；
/// ② `cust_id` 拼错/与网关绑定的 `tenant_id` 不是同一个 ID 空间。
pub fn warn_unregistered(tenant: &str) {
    log::warn!(
        "storage: cust_id '{tenant}' has no [[storage.cell]] — falling back to the default backend \
         (check the cell list and that it matches the gateway-bound tenant_id)"
    );
}
