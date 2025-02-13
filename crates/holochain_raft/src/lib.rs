mod client;
mod memstore;
mod message;
mod network;
mod raft_mem;

pub use client::HcClient;
pub use memstore::{ClientRequest, ClientResponse, MemLogStore, RaftOp, RaftSnap, TypeConfig};
pub use message::{ProposalResponse, RaftRpcRequest, RaftRpcRequestPayload, RaftRpcResponse};
pub use network::HcNetworkFactory;
pub use raft_mem::{handle_incoming_request, new_raft_mem};

pub use openraft::error;
pub use openraft::storage::RaftLogStorage;
pub use openraft::RaftLogReader;

pub type LogId = openraft::LogId<memstore::TypeConfig>;
pub type Entry = openraft::Entry<memstore::TypeConfig>;
pub type Raft = openraft::Raft<memstore::TypeConfig>;
