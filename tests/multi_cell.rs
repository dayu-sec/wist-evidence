//! 多客户存储路由（`[[storage.cell]]`）的契约测试。
//!
//! 验的是「一条记录的落点由 `cust_id` 决定」这条规则，以及读的时候能不能跟着走。
//! 用内存后端，不需要 PG / 磁盘；HTTP 层不参与路由（它只搬运），所以直接调 service。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use wist_evidence::domain::Kind;
use wist_evidence::domain::kind::Sensitivity;
use wist_evidence::infra::{
    Backend, Backends, FileObjectStore, ListFilter, MemoryIndex, MemoryObjectStore,
};
use wist_evidence::service::{EvidenceService, ReadPrincipal, SimpleWrite, WriteError};

const A: &str = "1000000000000000001";
const B: &str = "1000000000000000002";
const UNKNOWN: &str = "1000000000000000003";

struct Harness {
    service: EvidenceService,
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
    let a = Arc::new(MemoryIndex::new());
    let b = Arc::new(MemoryIndex::new());

    let mut cells = BTreeMap::new();
    cells.insert(A.to_string(), backend(a.clone()));
    cells.insert(B.to_string(), backend(b.clone()));

    let service = EvidenceService::with_backends(
        Arc::new(Backends::new(cells)),
        vec![Kind::InvestigationReport],
        1024 * 1024,
    );
    Harness { service, a, b }
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
}

#[test]
fn unregistered_customer_is_rejected_and_writes_nothing() {
    let h = harness();
    let result = futures_lite_block_on(h.service.write_simple(SimpleWrite {
        customer_id: UNKNOWN.to_string(),
        command_id: "cmd-x".to_string(),
        subject_id: UNKNOWN.to_string(),
        subject_type: "customer",
        kind: Kind::InvestigationReport,
        schema_id: "wist.evidence/v1".to_string(),
        sensitivity: Sensitivity::Paths,
        content: "{\"x\":1}".to_string(),
    }));

    match result {
        Err(WriteError::CustomerNotRegistered(cust_id)) => assert_eq!(cust_id, UNKNOWN),
        other => panic!("expected CustomerNotRegistered, got {other:?}"),
    }
    assert_eq!(h.a.len(), 0, "未登记客户不能落到别的客户的库");
    assert_eq!(h.b.len(), 0, "未登记客户不能落到别的客户的库");
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
fn unregistered_tenant_reads_look_empty() {
    let h = harness();
    write(&h.service, A, "cmd-a", "{\"a\":1}");

    let page = futures_lite_block_on(h.service.list(
        &principal(&[UNKNOWN]),
        UNKNOWN,
        &ListFilter::default(),
        50,
        None,
    ))
    .expect("list must not fail");
    assert!(page.artifacts.is_empty(), "未登记租户的列表必须为空");
    assert!(page.next_cursor.is_none());

    let purged = futures_lite_block_on(h.service.purge_probes(UNKNOWN, Duration::ZERO))
        .expect("purge must not fail");
    assert_eq!(purged, 0, "未登记租户没有可清理的探针");
    assert_eq!(h.a.len(), 1, "清理不能碰别的客户的库");
}

#[test]
fn registered_cells_are_reported_without_connection_details() {
    let h = harness();
    let backends = h.service.backends();
    assert_eq!(backends.len(), 2);
    assert!(!backends.is_empty());
    assert_eq!(backends.cell_ids(), vec![A, B]);
    assert!(backends.has_cell(A));
    assert!(!backends.has_cell(UNKNOWN));
    assert_eq!(backends.objects_kind(), "memory");

    let manifest = backends.describe();
    assert_eq!(manifest.len(), 2);
    assert_eq!(manifest[0].cust_id, A);
    assert_eq!(manifest[0].index, "memory");
    assert_eq!(manifest[0].objects, "memory");
}

#[test]
fn objects_kind_is_mixed_when_cells_differ() {
    let dir = std::env::temp_dir().join(format!("wist-evidence-mixed-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let file = Arc::new(FileObjectStore::open(&dir).expect("open file store"));

    let mut cells = BTreeMap::new();
    cells.insert(A.to_string(), backend(Arc::new(MemoryIndex::new())));
    cells.insert(
        B.to_string(),
        Backend {
            index: Arc::new(MemoryIndex::new()),
            objects: file,
            bucket: "wist-evidence".to_string(),
        },
    );

    // 各客户后端类型不同：读端要保守地走代理读，不能假设全都能签名。
    assert_eq!(Backends::new(cells).objects_kind(), "mixed");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_single_customer_can_be_built_from_one_cell() {
    let backends = Backends::from_one(UNKNOWN, backend(Arc::new(MemoryIndex::new())));
    assert_eq!(backends.len(), 1);
    assert!(backends.has_cell(UNKNOWN));
    assert!(backends.for_tenant(UNKNOWN).is_some());
    assert!(backends.for_tenant(A).is_none());
}
