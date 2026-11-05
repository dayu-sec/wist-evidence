//! wist 证据服务：动作产物的统一落点。
//!
//! 设计见 `wist-design/doc/design/center/evidence-service.md`。当前实现覆盖该文 **P0**：
//! 索引 + sink 写入 + 元数据读 + 短签名。
//!
//! 刻意不做（P0 之外）：清理作业、删除凭证、对账、索引重建、前缀级 STS、每客户 CMK。

pub mod api;
pub mod config;
pub mod domain;
pub mod infra;
pub mod probe;
pub mod service;
