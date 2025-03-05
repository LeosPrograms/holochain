use holochain_types::prelude::*;

use crate::TypeConfig;

// #[derive(Clone, Debug, derive_more::From, serde::Serialize, serde::Deserialize)]
// pub enum RaftRpc {
//     Request(RaftRpcRequest),
//     Response(RaftRpcResponse),
// }

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RpcRequestEnvelope {
    pub raft_id: crate::RaftId,
    pub payload: RpcRequest,
}

pub type RpcRequest = p2p_raft::message::RpcRequest<TypeConfig>;
pub type RpcResponse = p2p_raft::message::RpcResponse<TypeConfig>;

pub type P2pRequest = p2p_raft::message::P2pRequest<TypeConfig>;
pub type P2pResponse = p2p_raft::message::P2pResponse<TypeConfig>;

pub type RaftRequest = p2p_raft::message::RaftRequest<TypeConfig>;
pub type RaftResponse = p2p_raft::message::RaftResponse<TypeConfig>;
