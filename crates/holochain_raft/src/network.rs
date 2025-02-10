use std::sync::Arc;

use holochain_keystore::MetaLairClient;
use holochain_p2p::{HolochainP2pDna, HolochainP2pDnaT};
use holochain_types::prelude::*;
use once_cell::sync::Lazy;
// use log_store::LogStore;
use openraft::{
    error::InstallSnapshotError,
    raft::{InstallSnapshotRequest, InstallSnapshotResponse},
};
use openraft::{
    error::{RPCError, RaftError, RemoteError},
    network::RPCOption,
    raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse},
    RaftNetwork, RaftNetworkFactory,
};

use crate::{
    memstore::{HcNode, TypeConfig},
    message::{RaftMessage, RaftRequest, RaftResponse},
};

pub type NodeId = u64;

pub struct HcNetworkNode {}

pub static RAFT_DNA_HASH: Lazy<DnaHash> = Lazy::new(|| {
    DnaHash::from_raw_32(vec![
        173, 42, 198, 87, 14, 251, 109, 63, 220, 5, 134, 199, 76, 182, 91, 33, 248, 12, 157, 68,
        204, 39, 116, 143, 7, 225, 54, 187, 97, 29, 141, 210,
    ])
});

#[derive(Clone)]
pub struct HcNetworkFactory {
    pub network: HolochainP2pDna,
    pub provenance: AgentPubKey,
    pub keystore: Arc<MetaLairClient>,
}

impl HcNetworkFactory {}

pub struct HcNetwork {
    provenance: AgentPubKey,
    target_id: NodeId,
    target: HcNode,
    network: HolochainP2pDna,
    keystore: Arc<MetaLairClient>,
}

impl RaftNetworkFactory<TypeConfig> for HcNetworkFactory {
    type Network = HcNetwork;

    async fn new_client(&mut self, target_id: NodeId, node: &HcNode) -> Self::Network {
        HcNetwork {
            provenance: self.provenance.clone(),
            target_id,
            target: node.clone(),
            network: self.network.clone(),
            keystore: self.keystore.clone(),
        }
    }
}

impl HcNetwork {
    pub async fn call(&self, message: RaftRequest) -> anyhow::Result<RaftResponse> {
        let (nonce, expires_at) =
            holochain_nonce::fresh_nonce(Timestamp::now()).map_err(|e| anyhow::anyhow!(e))?;

        let zome_call_params = ZomeCallParams {
            provenance: self.provenance.clone(),
            cell_id: CellId::new(RAFT_DNA_HASH.clone(), self.target.agent.clone()),
            zome_name: "raft".into(),
            fn_name: "raft".into(),
            cap_secret: None,
            payload: ExternIO::encode(message)?,
            nonce,
            expires_at,
        };
        let zome_call_payload = holochain_types::ZomeCallParamsSigned::try_from_params(
            &self.keystore,
            zome_call_params.clone(),
        )
        .await?;

        let out = self
            .network
            .call_remote(
                self.target.agent.clone(),
                zome_call_payload.bytes,
                zome_call_payload.signature,
            )
            .await?;

        Ok(RaftResponse::try_from(out)?)
    }
}

impl RaftNetwork<TypeConfig> for HcNetwork {
    /// Send an AppendEntries RPC to the target.
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>>
    {
        Ok(self
            .call(rpc.into())
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
            .call(rpc.into())
            .await
            .unwrap()
            // .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target_id, e)))?
            .unwrap_install_snapshot())
    }

    /// Send a RequestVote RPC to the target.
    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>> {
        Ok(self
            .call(rpc.into())
            .await
            .unwrap()
            // .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target_id, e)))?
            .unwrap_vote())
    }
}
