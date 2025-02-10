mod memstore;
mod message;
mod network;
mod raft_mem;

pub use network::HcNetworkFactory;
pub use raft_mem::{handle_incoming_request, new_raft_mem};

pub type LogId = openraft::LogId<memstore::TypeConfig>;
pub type Entry = openraft::Entry<memstore::TypeConfig>;
pub type Raft = openraft::Raft<memstore::TypeConfig>;
