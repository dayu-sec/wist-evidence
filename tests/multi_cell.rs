//! 多客户存储路由（`[[storage.cell]]`）的契约测试。
//!
//! 验的是「一条记录的落点由 `cust_id` 决定」这条规则，以及读的时候能不能跟着走。
//! 用内存后端，不需要 PG / 磁盘；HTTP 层不参与路由（它只搬运），所以直接调 service。

use std::collections::BTreeMap;
use std::sync::Arc;

use wist_evidence::domain::Kind;
use wist_evidence::domain::kind::Sensitivity;
use wist_evidence::infra::{Backend, Backends, MemoryIndex, MemoryObjectStore};
use wist_evidence::service::{EvidenceService, ReadPrincipal, SimpleWrite};

const A: &str = "1000000000000000001";
const B: &str = "1000000000000000002";
const UNKNOWN: &str = "1000000000000000003";

struct Harness {
    service: EvidenceService,
    default: Arc<MemoryIndex>,
    a: Arc<MemoryIndex>,
    b: Arc<MemoryIndex>,
}

fn backend(index: Arc<MemoryIndex>) -> Backend {
    Backend {
        index,
        objects: Arc::new(MemoryObjectStore::new()),
        bucket: "wist-evidence".to_string(),
    }
}

fn harness() -> Harness {
    let default = Arc::new(MemoryIndex::new());
    let a = Arc::new(MemoryIndex::new());
    let b = Arc::new(MemoryIndex::new());

    let mut cells = BTreeMap::new();
    cells.insert(A.to_string(), backend(a.clone()));
    cells.insert(B.to_string(), backend(b.clone()));

    let backends = Backends::new(backend(default.clone()), cells);
    let service = EvidenceService::with_backends(
        Arc::new(backends),
        vec![Kind::InvestigationReport],
        1024 * 1024,
    );
    Harness {
        service,
        default,
        a,
        b,
    }
}

fn write(service: &EvidenceService, cust: &str, cmd: &str, body: &str) -> String {
    let outcome = futures_lite_block_on(service.write_simple(SimpleWrite {
        customer_id: cust.to_string(),
        command_id: cmd.to_string(),
        subject_id: cust.to_string(),
        subject_type: "customer",
        kind: Kind::InvestigationReport,
        schema_id: "wist.evidence/v1".to_string(),
        sensitivity: Sensitivity::Paths,
        content: body.to_string(),
    }));
    outcome.expect("write must succeed").artifact_id
}

fn principal(tenants: &[&str]) -> ReadPrincipal {
    ReadPrincipal {
        tenant_ids: tenants.iter().map(|t| t.to_string()).collect(),
        scopes: vec!["evidence:read".to_string()],
        max_sensitivity: Sensitivity::Paths,
    }
}

/// 测试里不需要 runtime 的包装：tokio 的 current-thread runtime 足够了。
fn futures_lite_block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(future)
}

#[test]
fn writes_land_in_the_cell_of_their_cust_id() {
    let h = harness();
    write(&h.service, A, "cmd-a", "{\"a\":1}");
    write(&h.service, B, "cmd-b", "{\"b\":1}");

    assert_eq!(h.a.len(), 1, "A 的记录应落在 A 的索引");
    assert_eq!(h.b.len(), 1, "B 的记录应落在 B 的索引");
    assert_eq!(h.default.len(), 0, "default 不该被 A/B 的记录污染");
}

#[test]
fn unregistered_customer_falls_back_to_default() {
    let h = harness();
    write(&h.service, UNKNOWN, "cmd-x", "{\"x\":1}");

    assert_eq!(
        h.default.len(),
        1,
        "未登记客户必须落到 default（不能丢数据）"
    );
    assert_eq!(h.a.len(), 0);
    assert_eq!(h.b.len(), 0);
}

#[test]
fn reads_stay_inside_the_callers_tenant_set() {
    let h = harness();
    let artifact = write(&h.service, A, "cmd-a", "{\"a\":1}");

    // A 的调用方能按 id 与 cmd_id 两种 key 读到。
    futures_lite_block_on(h.service.metadata(&principal(&[A]), &artifact)).expect("A 读自己的记录");
    futures_lite_block_on(h.service.metadata(&principal(&[A]), "cmd-a")).expect("A 按 cmd_id 读");

    // B 的调用方即便拿到同一个 key 也读不到（跨客户 = 404，不泄露存在性）。
    assert!(
        futures_lite_block_on(h.service.metadata(&principal(&[B]), &artifact)).is_err(),
        "B 不能读到 A 的记录"
    );
    // 未登记客户更读不到。
    assert!(
        futures_lite_block_on(h.service.metadata(&principal(&[UNKNOWN]), &artifact)).is_err(),
        "未登记客户不能读到 A 的记录"
    );
}

#[test]
fn only_multi_cell_deployments_report_cells() {
    let h = harness();
    assert!(h.service.backends().is_multi());
    assert_eq!(h.service.backends().cell_ids(), vec![A, B]);
    assert!(h.service.backends().has_cell(A));
    assert!(!h.service.backends().has_cell(UNKNOWN));
}
