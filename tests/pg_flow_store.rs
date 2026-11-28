//! `PgFlowStore` 的真机契约：**分区按需创建** + **ON CONFLICT 覆盖**。
//!
//! 为什么单独一条（默认 ignore）：它要一个真 PG。SQL 里有两处只有 PG 才能回答的问题：
//!
//! 1. 分区父表上的 `unique` 约束必须包含分区键（`window_start`）——写错了
//!    PG 会直接拒绝 `create table`；
//! 2. `on conflict (...) do update` 在**分区表**上是否按分区路由到正确的行。
//!
//! 跑法（`EVIDENCE_TEST_PG_URL` 指向一个**可清空的库**）：
//!
//! ```bash
//! docker run --rm -d --name flow-pg -e POSTGRES_PASSWORD=pg -e POSTGRES_DB=flowtest -p 55439:5432 postgres:16
//! EVIDENCE_TEST_PG_URL=postgres://postgres:pg@127.0.0.1:55439/flowtest \
//!   cargo test --features pg --test pg_flow_store -- --ignored --nocapture
//! ```

#![cfg(feature = "pg")]

use wist_evidence::domain::{FlowRecordIn, RECORD_SCHEMA_V1};
use wist_evidence::infra::{FlowStore, PgFlowStore};

fn database_url() -> Option<String> {
    std::env::var("EVIDENCE_TEST_PG_URL").ok()
}

fn record(start: i64, dst_port: &str) -> FlowRecordIn {
    FlowRecordIn {
        record_schema: RECORD_SCHEMA_V1.to_string(),
        source: "host_ebpf".to_string(),
        coverage: "host_partial".to_string(),
        capture: "tc_egress".to_string(),
        collector_instance_id: "agent-host-1".to_string(),
        host_id: "host-1".to_string(),
        host_name: Some("vm-web-01".to_string()),
        kernel: Some("6.8.0".to_string()),
        ifname: "eth0".to_string(),
        ifindex: 2,
        if_mac: None,
        local_addrs: vec!["10.118.0.14".to_string()],
        direction: "out".to_string(),
        protocol: "6".to_string(),
        srcaddr: "10.118.0.14".to_string(),
        dstaddr: "1.1.1.1".to_string(),
        srcport: "50000".to_string(),
        dstport: dst_port.to_string(),
        icmp_type: None,
        icmp_code: None,
        start,
        end: start + 60,
        window_seconds: 60,
        packets: 13,
        bytes: 1460,
        wire_bytes: None,
        wire_bytes_source: "unavailable".to_string(),
        skb_count: Some(4),
        gso_skb_count: Some(1),
        truncated: false,
        dropped: 0,
        partial: None,
        partial_reason: None,
    }
}

#[tokio::test]
#[ignore = "需要真 PG：设置 EVIDENCE_TEST_PG_URL"]
async fn partitions_are_created_on_demand_and_resends_overwrite() {
    let Some(url) = database_url() else {
        eprintln!("skip: EVIDENCE_TEST_PG_URL not set");
        return;
    };

    // 用真实的迁移建表（与部署同一条路径）。
    let index = wist_evidence::infra::PgIndex::connect(&url)
        .await
        .expect("connect");
    index.migrate().await.expect("migrate");
    let store = PgFlowStore::from_pool(index.pool().clone());

    // 两个不同的 UTC 日：第一次写要各建一个分区。
    let batch = vec![
        record(1_789_470_120, "443"), // 2026-09-15T11:02:00Z
        record(1_789_556_520, "80"),  // 次日（另一个 UTC 日）
    ];
    let first = store
        .insert_batch("c-001", &batch)
        .await
        .expect("first write");
    assert_eq!(first.accepted, 2);
    assert!(first.partitions >= 1, "首次写入应当创建分区");

    // 同键重发：不新增行（覆盖）。
    let second = store
        .insert_batch("c-001", &batch)
        .await
        .expect("second write");
    assert_eq!(second.accepted, 2);
    assert_eq!(second.partitions, 0, "分区已存在，不应再建");

    // 覆盖语义：同一窗口改大 `bytes`，行数不变、值被替换。
    let mut updated = batch.clone();
    updated[0].bytes = 9_999;
    store
        .insert_batch("c-001", &updated)
        .await
        .expect("third write");

    // 另一租户同一份记录：必须能写进去（唯一键含 tenant）。
    store
        .insert_batch("c-002", &batch)
        .await
        .expect("other tenant");
}
