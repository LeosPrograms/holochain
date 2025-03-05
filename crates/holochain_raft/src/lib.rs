mod client;
pub mod message;
mod network;

use std::sync::Arc;

pub use client::HcClient;
pub use network::HcNetworkFactory;

pub use openraft::error;
pub use openraft::storage::RaftLogStorage;
pub use openraft::{Entry, EntryPayload, LogId, RaftLogReader};

/// State for a raft instance in the conductor
#[derive(Clone)]
pub struct HcRaft {
    /// The raft instance
    pub raft: p2p_raft::Dinghy<TypeConfig>,
    /// The storage for the raft instance
    pub storage: p2p_raft::LogStore<TypeConfig>,
    /// The client for making remote calls to other conductors' rafts
    pub client: HcClient,
}

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

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    derive_more::Deref,
    derive_more::Display,
)]
#[serde(transparent)]
pub struct HcNode(holo_hash::AgentPubKeyB64);

impl From<holo_hash::AgentPubKey> for HcNode {
    fn from(agent: holo_hash::AgentPubKey) -> Self {
        HcNode(agent.into())
    }
}

impl HcNode {
    pub fn agent(&self) -> holo_hash::AgentPubKey {
        self.0.clone().into()
    }
}

impl Default for HcNode {
    fn default() -> Self {
        HcNode(holo_hash::AgentPubKey::from_raw_32(vec![0; 32]).into())
    }
}

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    derive_more::Deref,
    derive_more::From,
    derive_more::Into,
)]
pub struct RaftOp(#[serde(with = "serde_bytes")] Vec<u8>);

openraft::declare_raft_types!(
    #[derive(serde::Serialize, serde::Deserialize)]
    pub TypeConfig:
        D = RaftOp,
        R = (),
        NodeId = HcNode,
        Node = (),
        SnapshotData = Box<Vec<u8>>,
);

impl p2p_raft::TypeConf for TypeConfig {}

impl openraft::NodeId for HcNode {}
