use holochain_types::prelude::*;
use openraft::raft::*;

use crate::{memstore::TypeConfig, RaftOp};

#[derive(Debug, derive_more::From, serde::Serialize, serde::Deserialize, SerializedBytes)]
pub enum RaftMessage {
    Request(RaftRequest),
    Response(RaftResponse),
}

#[derive(
    Debug,
    Clone,
    derive_more::Unwrap,
    derive_more::From,
    serde::Serialize,
    serde::Deserialize,
    SerializedBytes,
)]
pub enum RaftRequest {
    // Raft protocol
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
    Vote(VoteRequest<TypeConfig>),

    // Client protocol
    ProposeOp(RaftOp),
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
    // Raft protocol
    AppendEntries(AppendEntriesResponse<TypeConfig>),
    InstallSnapshot(InstallSnapshotResponse<TypeConfig>),
    Vote(VoteResponse<TypeConfig>),

    // Client protocol
    ProposeOp(ProposeOpResponse),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, SerializedBytes)]
pub enum ProposeOpResponse {
    Accepted,
    NoLeader,
    ForwardToLeader(AgentPubKey),
}
