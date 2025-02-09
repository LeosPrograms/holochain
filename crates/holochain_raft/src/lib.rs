mod memstore;
mod message;
mod network;
mod raft_mem;

pub type LogId = openraft::LogId<memstore::TypeConfig>;
pub type Entry = openraft::Entry<memstore::TypeConfig>;
pub type Raft = openraft::Raft<memstore::TypeConfig>;
