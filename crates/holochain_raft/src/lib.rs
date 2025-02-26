mod client;
mod memstore;
mod message;
mod network;
mod raft_mem;

pub use client::{Forker, HcClient};
pub use memstore::{ClientRequest, ClientResponse, MemLogStore, RaftOp, RaftSnap, TypeConfig};
pub use message::{ProposalResponse, RaftRpcRequest, RaftRpcRequestPayload, RaftRpcResponse};
pub use network::HcNetworkFactory;
pub use raft_mem::{handle_incoming_request, new_raft_mem};

pub use openraft::error;
pub use openraft::storage::RaftLogStorage;
pub use openraft::{EntryPayload, RaftLogReader};

pub type LogId = openraft::LogId<memstore::TypeConfig>;
pub type Entry = openraft::Entry<memstore::TypeConfig>;
pub type Raft = openraft::Raft<memstore::TypeConfig>;

pub type RaftForkId = u64;

pub const PRESENCE_WINDOW: std::time::Duration = std::time::Duration::from_secs(5);
pub const FORKING_WINDOW: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct RaftId {
    workspace: holo_hash::EntryHash,
    fork_id: Option<RaftForkId>,
}

impl From<holo_hash::EntryHash> for RaftId {
    fn from(workspace: holo_hash::EntryHash) -> Self {
        Self {
            workspace,
            fork_id: None,
        }
    }
}
