use std::sync::Arc;

use crate::memstore::{new_mem_store, MemLogStore, TypeConfig};
use crate::message::{RaftRequest, RaftResponse};

use openraft::{Config, Raft, RaftNetworkFactory};

pub type RaftMem = Raft<TypeConfig>;
pub type NodeId = u64;

pub async fn new_raft_mem(
    id: NodeId,
    network: impl RaftNetworkFactory<TypeConfig>,
) -> anyhow::Result<(RaftMem, Arc<MemLogStore>)> {
    let config = Arc::new(
        Config {
            heartbeat_interval: 100,
            election_timeout_min: 250,
            election_timeout_max: 500,
            ..Default::default()
        }
        .validate()?,
    );
    let (storage, state_machine) = new_mem_store();
    let raft = Raft::new(id, config, network, storage.clone(), state_machine).await?;
    Ok((raft, storage))
}

/// TODO: handle errors
pub async fn handle_incoming_request(raft: &RaftMem, msg: RaftRequest) -> Option<RaftResponse> {
    let response = match msg {
        RaftRequest::AppendEntries(req) => raft.append_entries(req).await.ok()?.into(),
        RaftRequest::InstallSnapshot(req) => raft.install_snapshot(req).await.ok()?.into(),
        RaftRequest::Vote(req) => raft.vote(req).await.ok()?.into(),
    };
    Some(response)
}
