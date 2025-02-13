use holochain_types::prelude::*;
// use log_store::LogStore;
use openraft::{
    error::InstallSnapshotError,
    raft::{InstallSnapshotRequest, InstallSnapshotResponse},
};
use openraft::{
    error::{RPCError, RaftError},
    network::RPCOption,
    raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse},
    RaftNetwork, RaftNetworkFactory,
};

use crate::{
    client::HcClient,
    memstore::{HcNode, TypeConfig},
};

#[derive(Clone)]
pub struct HcNetworkFactory {
    pub client: HcClient,
}

impl HcNetworkFactory {}

pub struct HcNetwork {
    target: HcNode,
    client: HcClient,
}

impl RaftNetworkFactory<TypeConfig> for HcNetworkFactory {
    type Network = HcNetwork;

    async fn new_client(&mut self, target: HcNode, _: &()) -> Self::Network {
        HcNetwork {
            target,
            client: self.client.clone(),
        }
    }
}

impl RaftNetwork<TypeConfig> for HcNetwork {
    /// Send an AppendEntries RPC to the target.
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>>
    {
        Ok(self
            .client
            .call(self.target.agent(), rpc.into())
            .await
            .unwrap()
            // .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target_id, e)))?
            .unwrap_append_entries())
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
        Ok(self
            .client
            .call(self.target.agent(), rpc.into())
            .await
            .unwrap()
            // .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target_id, e)))?
            .unwrap_install_snapshot())
    }

    /// Send a RequestVote RPC to the target.
    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>> {
        Ok(self
            .client
            .call(self.target.agent(), rpc.into())
            .await
            .unwrap()
            // .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target_id, e)))?
            .unwrap_vote())
    }
}
