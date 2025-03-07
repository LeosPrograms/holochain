use std::{sync::Arc, time::Duration};

use crate::{message::*, HcNode, HcrTypes};
use holochain_keystore::MetaLairClient;
use holochain_p2p::{HolochainP2pDna, HolochainP2pDnaT};
use holochain_types::prelude::*;
use openraft::error::{ClientWriteError, RaftError};
use p2p_raft::Dinghy;
use tokio::sync::Mutex;

use crate::RaftId;

#[derive(Clone)]
pub struct HcClient {
    pub provenance: AgentPubKey,
    pub network: HolochainP2pDna,
    pub raft_id: RaftId,
    pub keystore: MetaLairClient,
    // XXX: circular reference, raft must be passed in after this is passed to raft
    pub raft: Arc<Mutex<Option<Dinghy<HcrTypes, HcClient>>>>,
}

impl HcClient {
    pub async fn call_leader_with_retry(&self, message: RpcRequest) -> anyhow::Result<RpcResponse> {
        let retries = 3;
        let mut target = self.provenance.clone();
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(3));
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
            ZomeCallResponse::Ok(out) => {
                let res = out.decode()?;
                println!(
                    "<CALL> {} -> {} ({}): {res:?}",
                    self.provenance.suffix(4),
                    target.suffix(4),
                    self.raft.lock().await.is_some(),
                );
                if let Some(raft) = self.raft.lock().await.as_ref() {
                    let mut t = raft.tracker.lock().await;

                    t.touch(&HcNode(target.into()));
                    t.handle_absentees(&raft, raft.config.p2p_config.responsive_interval)
                        .await;
                } else {
                    tracing::warn!("raft not yet set in client");
                }
                Ok(res)
            }
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
