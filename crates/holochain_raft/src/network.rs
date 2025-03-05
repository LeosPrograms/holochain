use std::future::Future;

use anyerror::AnyError;
use openraft::{
    alias::VoteOf,
    error::{
        Fatal, InstallSnapshotError, RemoteError, ReplicationClosed, StreamingError, Unreachable,
    },
    network::v2::RaftNetworkV2,
    raft::{InstallSnapshotRequest, InstallSnapshotResponse, SnapshotResponse},
    OptionalSend, Snapshot,
};
use openraft::{
    error::{RPCError, RaftError},
    network::RPCOption,
    raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse},
    RaftNetwork, RaftNetworkFactory,
};
use p2p_raft::message::RaftRequest;

use crate::{client::HcClient, HcNode, TypeConfig};

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

impl RaftNetworkV2<TypeConfig> for HcNetwork {
    /// Send an AppendEntries RPC to the target.
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        // println!("<RAFT> append_entries {rpc:?}");
        match self
            .client
            .call(self.target.agent(), RaftRequest::from(rpc).into())
            .await
        {
            Ok(resp) => Ok(resp.unwrap_raft().unwrap_append()),
            Err(e) => {
                tracing::error!("{e:?}");
                Err(RPCError::Unreachable(Unreachable::new(&AnyError::from(e))))
            }
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: Snapshot<TypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let rpc = RaftRequest::Snapshot {
            vote,
            snapshot_meta: snapshot.meta,
            snapshot_data: *snapshot.snapshot,
        };
        match self.client.call(self.target.agent(), rpc.into()).await {
            Ok(resp) => Ok(resp.unwrap_raft().unwrap_snapshot()),
            Err(e) => {
                tracing::error!("{e:?}");
                Err(StreamingError::Unreachable(Unreachable::new(
                    &AnyError::from(e),
                )))
            }
        }
    }

    /// Send a RequestVote RPC to the target.
    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        // println!("<RAFT> vote {rpc:?}");
        match self
            .client
            .call(self.target.agent(), RaftRequest::from(rpc).into())
            .await
        {
            Ok(resp) => Ok(resp.unwrap_raft().unwrap_vote()),
            Err(e) => {
                tracing::error!("{e:?}");
                Err(RPCError::Unreachable(Unreachable::new(&AnyError::from(e))))
            }
        }
    }
}
