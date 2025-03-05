use std::{sync::Arc, time::Duration};

use crate::message::*;
use holochain_keystore::MetaLairClient;
use holochain_p2p::{HolochainP2pDna, HolochainP2pDnaT};
use holochain_types::prelude::*;
use openraft::error::{ClientWriteError, RaftError};

use crate::RaftId;

#[derive(Clone)]
pub struct HcClient {
    pub provenance: AgentPubKey,
    pub network: HolochainP2pDna,
    pub raft_id: RaftId,
    pub keystore: MetaLairClient,
}

impl HcClient {
    pub async fn call_leader_with_retry(&self, message: RpcRequest) -> anyhow::Result<RpcResponse> {
        let retries = 3;
        let mut target = self.provenance.clone();
        let mut interval = tokio::time::interval(Duration::from_secs(3));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        for _ in 0..retries {
            interval.tick().await;
            let res = self.call(target.clone(), message.clone()).await?;
            match res {
                RpcResponse::P2p(P2pResponse::RaftError(RaftError::APIError(
                    ClientWriteError::ForwardToLeader(leader),
                ))) => {
                    if let Some(leader) = leader.leader_id {
                        target = leader.agent();
                    }
                }
                r => return Ok(r),
            }
        }
        anyhow::bail!("Failed to call leader after {} retries", retries);
    }

    pub async fn call(
        &self,
        target: AgentPubKey,
        message: RpcRequest,
    ) -> anyhow::Result<RpcResponse> {
        let zome_call_params = self.zome_call_params(target.clone(), message)?;
        let zome_call_payload = holochain_types::ZomeCallParamsSigned::try_from_params(
            &self.keystore,
            zome_call_params.clone(),
        )
        .await?;

        let out = self
            .network
            .call_remote(
                target.clone(),
                zome_call_payload.bytes,
                zome_call_payload.signature,
            )
            .await?;

        let zcr = ZomeCallResponse::try_from(out)?;
        match zcr {
            ZomeCallResponse::Ok(out) => Ok(out.decode()?),
            // ZomeCallResponse::Ok(out) => Ok(RaftRpcResponse::try_from(out)?),
            _ => anyhow::bail!("call: unexpected response: {:?}", zcr),
        }
    }

    pub fn zome_call_params(
        &self,
        target: AgentPubKey,
        message: RpcRequest,
    ) -> anyhow::Result<ZomeCallParams> {
        let (nonce, expires_at) =
            holochain_nonce::fresh_nonce(Timestamp::now()).map_err(|e| anyhow::anyhow!(e))?;

        let dna_hash = self.network.dna_hash();
        let cell_id = CellId::new(dna_hash, target.clone());

        let payload = ExternIO::encode(RpcRequestEnvelope {
            raft_id: self.raft_id.clone(),
            payload: message,
        })?;
        Ok(ZomeCallParams {
            provenance: self.provenance.clone(),
            cell_id,
            zome_name: "raft-hardwired-hack".into(),
            fn_name: "raft-hardwired-hack".into(),
            cap_secret: None,
            payload,
            nonce,
            expires_at,
        })
    }
}
