use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use holochain_keystore::MetaLairClient;
use holochain_p2p::{HolochainP2pDna, HolochainP2pDnaT};
use holochain_types::prelude::*;
use openraft::QuorumSet;
use tokio::{sync::Mutex, time::Instant};

use crate::{
    handle_incoming_request,
    memstore::HcNode,
    message::{ProposalResponse, RaftRpcRequest, RaftRpcRequestPayload, RaftRpcResponse},
    Raft, RaftForkId, RaftId,
};

pub struct Forker {
    last_seen: BTreeMap<AgentPubKey, Instant>,
    first_instant_without_quorum: Option<Instant>,
    pub active_fork: Option<RaftForkId>,
}

impl Forker {
    pub fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            last_seen: Default::default(),
            first_instant_without_quorum: None,
            active_fork: None,
        }))
    }
    pub fn touch(&mut self, agent: AgentPubKey) {
        self.last_seen.insert(agent, Instant::now());
    }

    pub fn who_else_is_here(&self, interval: Duration) -> BTreeSet<AgentPubKey> {
        self.last_seen
            .iter()
            .filter(|(_, t)| t.elapsed() < interval)
            .map(|(to, _)| to)
            .cloned()
            .collect()
    }

    pub async fn its_forking_time(&mut self, raft: &Raft) -> bool {
        if self.active_fork.is_some() {
            return false;
        }

        let here: BTreeSet<HcNode> = self
            .who_else_is_here(crate::PRESENCE_WINDOW)
            .into_iter()
            .map(HcNode::from)
            .collect();

        let is_quorum = raft
            .with_raft_state(move |s| s.membership_state.effective().is_quorum(here.iter()))
            .await
            .unwrap_or(false);

        if is_quorum {
            self.first_instant_without_quorum = None;
        } else if self.first_instant_without_quorum.is_none() {
            self.first_instant_without_quorum = Some(Instant::now());
        }

        if let Some(t) = self.first_instant_without_quorum {
            t.elapsed() > crate::FORKING_WINDOW
        } else {
            false
        }
    }

    pub fn last_seen(&self) -> &BTreeMap<AgentPubKey, Instant> {
        &self.last_seen
    }
}

#[derive(Clone)]
pub struct HcClient {
    pub provenance: AgentPubKey,
    pub network: HolochainP2pDna,
    pub raft_id: RaftId,
    pub keystore: MetaLairClient,
    pub forker: Arc<Mutex<Forker>>,
}

impl HcClient {
    pub async fn call_leader_with_retry(
        &self,
        message: RaftRpcRequestPayload,
    ) -> anyhow::Result<RaftRpcResponse> {
        let retries = 3;
        let mut target = self.provenance.clone();
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
        message: RaftRpcRequestPayload,
    ) -> anyhow::Result<RaftRpcResponse> {
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

        self.forker.lock().await.touch(target);

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
        message: RaftRpcRequestPayload,
    ) -> anyhow::Result<ZomeCallParams> {
        let (nonce, expires_at) =
            holochain_nonce::fresh_nonce(Timestamp::now()).map_err(|e| anyhow::anyhow!(e))?;

        let dna_hash = self.network.dna_hash();
        let cell_id = CellId::new(dna_hash, target.clone());

        let payload = ExternIO::encode(RaftRpcRequest {
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
