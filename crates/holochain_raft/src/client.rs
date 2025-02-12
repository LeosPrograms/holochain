use holochain_keystore::MetaLairClient;
use holochain_p2p::{HolochainP2pDna, HolochainP2pDnaT};
use holochain_types::prelude::*;
use once_cell::sync::Lazy;

use crate::message::{ProposalResponse, RaftRpcRequest, RaftRpcResponse};

pub static RAFT_DNA_HASH: Lazy<DnaHash> = Lazy::new(|| {
    DnaHash::from_raw_32(vec![
        173, 42, 198, 87, 14, 251, 109, 63, 220, 5, 134, 199, 76, 182, 91, 33, 248, 12, 157, 68,
        204, 39, 116, 143, 7, 225, 54, 187, 97, 29, 141, 210,
    ])
});

#[derive(Clone)]
pub struct HcClient {
    pub provenance: AgentPubKey,
    pub network: HolochainP2pDna,
    pub keystore: MetaLairClient,
}

impl HcClient {
    pub async fn call_leader_with_retry(
        &self,
        mut target: AgentPubKey,
        message: RaftRpcRequest,
    ) -> anyhow::Result<RaftRpcResponse> {
        let retries = 3;
        for _ in 0..retries {
            let res = self.call(target, message.clone()).await;
            match res {
                Ok(RaftRpcResponse::Proposal(ProposalResponse::NoLeader)) => {
                    anyhow::bail!("call_leader_with_retry: No leader found")
                }
                Ok(RaftRpcResponse::Proposal(ProposalResponse::ForwardToLeader(leader))) => {
                    target = leader;
                }
                r => return Ok(r?),
            }
        }
        anyhow::bail!("Failed to call leader after {} retries", retries);
    }

    pub async fn call(
        &self,
        target: AgentPubKey,
        message: RaftRpcRequest,
    ) -> anyhow::Result<RaftRpcResponse> {
        let (nonce, expires_at) =
            holochain_nonce::fresh_nonce(Timestamp::now()).map_err(|e| anyhow::anyhow!(e))?;

        let zome_call_params = ZomeCallParams {
            provenance: self.provenance.clone(),
            cell_id: CellId::new(RAFT_DNA_HASH.clone(), target.clone()),
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
            .call_remote(target, zome_call_payload.bytes, zome_call_payload.signature)
            .await?;

        Ok(RaftRpcResponse::try_from(out)?)
    }
}
