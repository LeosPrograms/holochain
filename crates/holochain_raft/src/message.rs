use holochain_types::prelude::*;
use openraft::raft::*;

use crate::{memstore::TypeConfig, RaftOp};

#[derive(Debug, derive_more::From, serde::Serialize, serde::Deserialize, SerializedBytes)]
pub enum RaftRpc {
    Request(RaftRpcRequest),
    Response(RaftRpcResponse),
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
pub enum RaftRpcRequest {
    // Messages sent *from* the leader
    #[from]
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    #[from]
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
    #[from]
    Vote(VoteRequest<TypeConfig>),

    // Messages sent *to* the leader
    //
    /// Propose an operation to be added to the log
    ProposeOp(RaftOp),
    /// An agent wants to join the raft network
    Join(AgentPubKey),
    /// An agent wants to leave the raft network
    Leave(AgentPubKey),
}

#[derive(
    Debug,
    derive_more::Unwrap,
    derive_more::From,
    serde::Serialize,
    serde::Deserialize,
    SerializedBytes,
)]
pub enum RaftRpcResponse {
    // Messages sent *from* the leader
    AppendEntries(AppendEntriesResponse<TypeConfig>),
    InstallSnapshot(InstallSnapshotResponse<TypeConfig>),
    Vote(VoteResponse<TypeConfig>),

    // Messages sent *to* the leader
    Proposal(ProposalResponse),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, SerializedBytes)]
pub enum ProposalResponse {
    Accepted,
    NoLeader,
    ForwardToLeader(AgentPubKey),
}
