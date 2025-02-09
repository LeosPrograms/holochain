use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

// use log_store::LogStore;
use openraft::{
    Config, Raft, RaftNetwork, RaftNetworkFactory, StorageError,
    error::{RPCError, RaftError, RemoteError},
    network::RPCOption,
    raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse},
};
use openraft::{
    error::InstallSnapshotError,
    raft::{InstallSnapshotRequest, InstallSnapshotResponse},
};

use openraft_memstore::{MemStoreStateMachine, TypeConfig, new_mem_store};
use tokio::sync::Mutex;

pub struct MemNetworkNode {
    pub raft: Raft<TypeConfig>,
    pub half_delay: Arc<AtomicU64>,
}

#[derive(Default, Clone)]
pub struct MemNetworkFactory {
    pub nodes: Arc<Mutex<BTreeMap<u64, MemNetworkNode>>>,
}

impl MemNetworkFactory {
    pub async fn new_node(&self, id: u64) -> anyhow::Result<Raft<TypeConfig>> {
        let config = Arc::new(
            Config {
                heartbeat_interval: 100,
                election_timeout_min: 250,
                election_timeout_max: 500,
                ..Default::default()
            }
            .validate()?,
        );
        let network = self.clone();
        let (storage, state_machine) = new_mem_store();
        let raft = Raft::new(id, config, network, storage, state_machine).await?;
        self.nodes.lock().await.insert(id, MemNetworkNode {
            raft: raft.clone(),
            half_delay: Arc::new(AtomicU64::new(11)),
        });
        Ok(raft)
    }

    pub async fn set_half_delay(&self, a: u64, half_delay: tokio::time::Duration) {
        let nodes = self.nodes.lock().await;

        let node = nodes.get(&a).unwrap();
        node.half_delay
            .store(half_delay.as_millis() as u64, Ordering::SeqCst);
    }
}

pub struct MemNetwork {
    target: u64,
    raft: Raft<TypeConfig>,
    half_delay: Arc<AtomicU64>,
}

impl RaftNetworkFactory<TypeConfig> for MemNetworkFactory {
    type Network = MemNetwork;

    async fn new_client(&mut self, target: u64, (): &()) -> Self::Network {
        let nodes = self.nodes.lock().await;
        let node = nodes.get(&target).unwrap();
        MemNetwork {
            target,
            raft: node.raft.clone(),
            half_delay: node.half_delay.clone(),
        }
    }
}

impl MemNetwork {
    async fn delay(&self) {
        let ms = self.half_delay.load(Ordering::SeqCst);
        if ms > 0 {
            println!("Sleeping for {}ms", ms);
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(ms)).await;
    }
}

impl RaftNetwork<TypeConfig> for MemNetwork {
    /// Send an AppendEntries RPC to the target.
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>>
    {
        self.delay().await;
        let res = self
            .raft
            .append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)));
        self.delay().await;
        res
    }

    /// Send an InstallSnapshot RPC to the target.
    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<TypeConfig>,
        RPCError<TypeConfig, RaftError<TypeConfig, InstallSnapshotError>>,
    > {
        self.delay().await;
        let res = self
            .raft
            .install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)));
        self.delay().await;
        res
    }

    /// Send a RequestVote RPC to the target.
    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>> {
        self.delay().await;
        let res = self
            .raft
            .vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)));
        self.delay().await;
        res
    }
}
