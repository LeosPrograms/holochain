use holochain_types::prelude::*;
// use log_store::LogStore;
use openraft::{
    error::{Fatal, InstallSnapshotError, RemoteError},
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

#[derive(
    holochain_p2p::kitsune_p2p::dependencies::kitsune_p2p_types::dependencies::thiserror::Error,
    Debug,
    derive_more::Display,
    derive_more::From,
)]
pub struct RemoteErrorWrapper(anyhow::Error);

impl RaftNetwork<TypeConfig> for HcNetwork {
    /// Send an AppendEntries RPC to the target.
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>>
    {
        // println!("<RAFT> append_entries {rpc:?}");
        Ok(self
            .client
            .call(self.target.agent(), rpc.into())
            .await
            .map_err(|e| {
                tracing::error!("Error calling append entries: {:?}", e);
                RPCError::RemoteError(RemoteError::new(
                    self.target.clone(),
                    RaftError::Fatal(Fatal::Panicked),
                ))
            })?
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
        // println!("<RAFT> install_snapshot {rpc:?}");
        Ok(self
            .client
            .call(self.target.agent(), rpc.into())
            .await
            .map_err(|e| {
                tracing::error!("Error calling install snapshot: {:?}", e);
                RPCError::RemoteError(RemoteError::new(
                    self.target.clone(),
                    RaftError::Fatal(Fatal::Panicked),
                ))
            })?
            .unwrap_install_snapshot())
    }

    /// Send a RequestVote RPC to the target.
    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>> {
        // println!("<RAFT> vote {rpc:?}");
        Ok(self
            .client
            .call(self.target.agent(), rpc.into())
            .await
            // .unwrap()
            .map_err(|e| {
                tracing::error!("Error calling vote: {:?}", e);
                RPCError::RemoteError(RemoteError::new(
                    self.target.clone(),
                    RaftError::Fatal(Fatal::Panicked),
                ))
            })?
            .unwrap_vote())
    }
}
