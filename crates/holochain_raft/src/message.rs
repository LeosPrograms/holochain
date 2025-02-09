use holochain_types::prelude::*;
use openraft::raft::*;

use crate::memstore::TypeConfig;

#[derive(Debug, derive_more::From, serde::Serialize, serde::Deserialize, SerializedBytes)]
pub enum RaftMessage {
    Request(RaftRequest),
    Response(RaftResponse),
}

#[derive(
    Debug,
    derive_more::Unwrap,
    derive_more::From,
    serde::Serialize,
    serde::Deserialize,
    SerializedBytes,
)]
pub enum RaftRequest {
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
    Vote(VoteRequest<TypeConfig>),
}

#[derive(
    Debug,
    derive_more::Unwrap,
    derive_more::From,
    serde::Serialize,
    serde::Deserialize,
    SerializedBytes,
)]
pub enum RaftResponse {
    AppendEntries(AppendEntriesResponse<TypeConfig>),
    InstallSnapshot(InstallSnapshotResponse<TypeConfig>),
    Vote(VoteResponse<TypeConfig>),
}
