mod client;
mod forker;
mod memstore;
mod message;
mod network;
mod peer_tracker;
mod raft_mem;

use std::collections::BTreeSet;
use std::sync::Arc;

pub use client::HcClient;
pub use forker::Forker;
pub use memstore::{
    ClientRequest, ClientResponse, HcNode, MemLogStore, RaftOp, RaftSnap, TypeConfig,
};
pub use message::{ProposalResponse, RaftRpcRequest, RaftRpcRequestPayload, RaftRpcResponse};
pub use network::HcNetworkFactory;
use openraft::error::{InitializeError, RaftError};
pub use peer_tracker::PeerTracker;
pub use raft_mem::{handle_incoming_request, new_raft_mem};

pub use openraft::error;
pub use openraft::storage::RaftLogStorage;
pub use openraft::{EntryPayload, RaftLogReader};

pub type LogId = openraft::LogId<memstore::TypeConfig>;
pub type Entry = openraft::Entry<memstore::TypeConfig>;

/// State for a raft instance in the conductor
#[derive(Clone)]
pub struct HcRaft {
    /// The raft instance
    pub raft: Raft,
    /// The storage for the raft instance
    pub storage: Arc<MemLogStore>,
    /// The client for making remote calls to other conductors' rafts
    pub client: HcClient,
}

#[derive(Clone, derive_more::Deref, derive_more::From)]
pub struct Raft(openraft::Raft<memstore::TypeConfig>);

impl Raft {
    pub async fn initialize(
        &self,
        ids: impl IntoIterator<Item = holo_hash::AgentPubKey>,
    ) -> Result<(), RaftError<TypeConfig, InitializeError<TypeConfig>>> {
        let ids: BTreeSet<HcNode> = ids.into_iter().map(|a| a.into()).collect();
        dbg!();
        match dbg!(self.0.initialize(ids).await) {
            Ok(_) => Ok(()),
            // this error is ok, it means we got some network messages already
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => Ok(()),
            e => e,
        }
    }

    pub fn local_agent(&self) -> holo_hash::AgentPubKey {
        self.metrics().borrow().id.agent()
    }
}

pub type RaftForkId = u64;

pub const PRESENCE_WINDOW: std::time::Duration = std::time::Duration::from_secs(5);
pub const FORKING_WINDOW: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct RaftId {
    pub workspace: holo_hash::EntryHash,
    // pub fork_id: Option<RaftForkId>,
}

impl From<holo_hash::EntryHash> for RaftId {
    fn from(workspace: holo_hash::EntryHash) -> Self {
        Self {
            workspace,
            // fork_id: None,
        }
    }
}
