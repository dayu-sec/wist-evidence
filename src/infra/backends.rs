//! 存储路由：**每个客户一个 cell，没有默认后端**。
//!
//! 为什么需要这一层：`wist-field` 要对多个客户提供**物理隔离**的存储（每客户独立 PG +
//! 独立对象目录），而数据面投递只有一个入口、一份配置（`cust_id` 在请求体里，反代看不到）。
//! 所以"分流"只能发生在服务端，判据就是写请求里的 `cust_id`、读请求里的 `x-evidence-tenant`。
//!
//! 未登记的客户不再兜底：写路径直接 4xx，读路径按"不存在"处理。默认后端会让
//! 「数据落错库」成为可能，而那个库实际上没有任何客户在用。

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Serialize;

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

/// `/healthz` 的客户清单项：**只含后端类型**，绝不含连接串 / 口令。
#[derive(Debug, Clone, Serialize)]
pub struct CellManifest {
    pub cust_id: String,
    pub index: &'static str,
    pub objects: &'static str,
}

/// 客户 → 后端的路由表。
pub struct Backends {
    cells: BTreeMap<String, Backend>,
}

impl Backends {
    pub fn new(cells: BTreeMap<String, Backend>) -> Self {
        Self { cells }
    }

    /// 单客户便捷构造（测试与单客户站点）。
    pub fn from_one(cust_id: impl Into<String>, backend: Backend) -> Self {
        let mut cells = BTreeMap::new();
        cells.insert(cust_id.into(), backend);
        Self::new(cells)
    }

    /// 取某个客户的后端；未登记返回 `None`，由调用方决定是 4xx 还是当作不存在。
    pub fn for_tenant(&self, tenant: &str) -> Option<&Backend> {
        self.cells.get(tenant)
    }

    /// 这个客户是否登记过。
    pub fn has_cell(&self, tenant: &str) -> bool {
        self.cells.contains_key(tenant)
    }

    /// 已登记的客户 id（升序）。给 `/healthz` 与启动日志用。
    pub fn cell_ids(&self) -> Vec<&str> {
        self.cells.keys().map(String::as_str).collect()
    }

    pub fn len(&self) -> usize {
        self.cells.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// 所有 cell 的 objects 类型一致时返回该类型，否则 `"mixed"`。
    ///
    /// 读端用这个字段决定正文怎么打开：`s3` 可以走签名 URL，`file` / `mixed` 都会
    /// 保守地走代理读，避免先发一次必然 501 的 `:presign`。
    pub fn objects_kind(&self) -> &'static str {
        let mut kinds = self
            .cells
            .values()
            .map(|backend| backend.objects.backend().as_str());
        let Some(first) = kinds.next() else {
            return "none";
        };
        if kinds.all(|kind| kind == first) {
            first
        } else {
            "mixed"
        }
    }

    /// `/healthz` 的客户清单：cust_id + 索引 / 对象类型，逐客户自述。
    pub fn describe(&self) -> Vec<CellManifest> {
        self.cells
            .iter()
            .map(|(cust_id, backend)| CellManifest {
                cust_id: cust_id.clone(),
                index: backend.index.kind(),
                objects: backend.objects.backend().as_str(),
            })
            .collect()
    }
}
