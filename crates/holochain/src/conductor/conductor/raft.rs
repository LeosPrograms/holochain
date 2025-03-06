use std::collections::BTreeSet;

use holochain_conductor_api::{
    LogOp, RaftInterfaceRequest, RaftInterfaceRequestPayload, RaftInterfaceResponsePayload,
};
use holochain_raft::{message::*, *};

use super::*;

fn make_config() -> Config {
    Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        // max_in_snapshot_log_to_keep: 0,
        ..Default::default()
    }
}

impl Conductor {
    pub(crate) async fn get_raft(&self, dna_hash: DnaHash, raft_id: RaftId) -> HcRaft {
        let provenance = crate::core::workflow::sys_validation_workflow::get_representative_agent(
            self, &dna_hash,
        )
        .expect("TODO");

        self.lookup_raft(dna_hash, provenance, raft_id).await
    }

    pub(crate) async fn handle_raft_rpc_call(
        &self,
        dna_hash: DnaHash,
        request: RpcRequestEnvelope,
        remote_agent: AgentPubKey,
    ) -> ConductorResult<RpcResponse> {
        // TODO: the representative agent must change if this agent ever leaves the network (and there are other local agents)
        let local_agent = crate::core::workflow::sys_validation_workflow::get_representative_agent(
            self, &dna_hash,
        )
        .expect("TODO");

        let raft_id = request.raft_id;

        let data = self
            .lookup_raft(dna_hash.clone(), local_agent.clone(), raft_id.clone())
            .await;

        let res = data
            .raft
            .handle_request(remote_agent.clone().into(), request.payload)
            .await
            .map_err(|e| {
                ConductorError::other(format!("TODO handle_incoming_request error: {e:?}"))
            })?;

        Ok(res)
    }

    pub(crate) async fn handle_raft_interface_call(
        &self,
        raft_call: RaftInterfaceRequest,
    ) -> ConductorResult<RaftInterfaceResponsePayload> {
        let dna_hash = raft_call.dna_hash.clone();

        // TODO: the representative agent must change if this agent ever leaves the network (and there are other local agents)
        let local_agent = crate::core::workflow::sys_validation_workflow::get_representative_agent(
            self, &dna_hash,
        )
        .expect("TODO");

        let HcRaft { client, mut raft } = self
            .lookup_raft(dna_hash.clone(), local_agent.clone(), raft_call.raft_id)
            .await;

        match raft_call.payload {
            RaftInterfaceRequestPayload::Initialize(peers) => {
                raft.initialize(peers.into_iter().map(HcNode::from))
                    .await
                    .map_err(|e| ConductorError::other(format!("can't initialize: {e:?}")))?;

                Ok(RaftInterfaceResponsePayload::Ok)
            }
            RaftInterfaceRequestPayload::Join(peers) => {
                // Ask all known peers to join
                future::join_all(peers.into_iter().map(move |peer| {
                    let client = client.clone();
                    let msg = P2pRequest::Join;
                    async move {
                        let res = client
                            .call(peer.clone(), msg.into())
                            .await
                            .map_err(|e| ConductorError::other(format!("can't join raft: {e:?}")))?
                            .unwrap_p_2_p();
                        match res {
                            P2pResponse::Ok => {
                                ConductorResult::Ok(RaftInterfaceResponsePayload::Ok)
                            }
                            r => Ok(RaftInterfaceResponsePayload::Error(r)),
                        }
                    }
                }))
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;
                Ok(RaftInterfaceResponsePayload::Ok)
            }
            RaftInterfaceRequestPayload::Leave => {
                let res = client
                    .call_leader_with_retry(P2pRequest::Leave.into())
                    .await
                    .map_err(|e| ConductorError::other(format!("can't leave raft: {e:?}")))?
                    .unwrap_p_2_p();
                match res {
                    P2pResponse::Ok => Ok(RaftInterfaceResponsePayload::Ok),
                    r => Ok(RaftInterfaceResponsePayload::Error(r)),
                }
            }
            RaftInterfaceRequestPayload::Propose(op) => {
                // XXX: first call is to self. No need to use the client for this.
                let res = client
                    .call_leader_with_retry(P2pRequest::Propose(op).into())
                    .await
                    .map_err(|e| ConductorError::other(format!("can't propose op in raft: {e:?}")))?
                    .unwrap_p_2_p();
                match res {
                    P2pResponse::Ok => Ok(RaftInterfaceResponsePayload::Ok),
                    r => Ok(RaftInterfaceResponsePayload::Error(r)),
                }
            }
            RaftInterfaceRequestPayload::GetAllLogEntries(index) => {
                let mut reader = raft.store.get_log_reader().await;
                let entries = if let Some(index) = index {
                    reader.try_get_log_entries(index..).await
                } else {
                    reader.try_get_log_entries(..).await
                }
                .map_err(|e| ConductorError::other(e.to_string()))?;
                Ok(RaftInterfaceResponsePayload::AllLogEntries(entries))
            }
            RaftInterfaceRequestPayload::GetUserLogEntries(index) => {
                let mut reader = raft.store.get_log_reader().await;

                let entries = if let Some(index) = index {
                    reader.try_get_log_entries(index..).await
                } else {
                    reader.try_get_log_entries(..).await
                }
                .map_err(|e| ConductorError::other(e.to_string()))?
                .into_iter()
                .filter_map(|l| match l.payload {
                    EntryPayload::Normal(n) => Some(LogOp {
                        log_id: l.log_id,
                        op: n,
                    }),
                    _ => None,
                })
                .collect();

                Ok(RaftInterfaceResponsePayload::UserLogEntries(entries))
            }
        }
    }

    async fn lookup_raft(
        &self,
        dna_hash: DnaHash,
        local_agent: AgentPubKey,
        raft_id: RaftId,
    ) -> HcRaft {
        let mut rafts = self.rafts.lock().await;

        match rafts.entry((dna_hash.clone(), raft_id.clone())) {
            std::collections::hash_map::Entry::Vacant(v) => {
                let client = HcClient {
                    provenance: local_agent.clone(),
                    keystore: self.keystore().clone(),
                    raft_id: raft_id.clone(),
                    network: self.holochain_p2p().to_dna(dna_hash.clone(), None),
                };
                let network = HcNetworkFactory {
                    client: client.clone(),
                };
                let config = make_config();
                let raft =
                    holochain_raft::Dinghy::new_mem(local_agent.clone().into(), config, network)
                        .await;
                let hc_raft = HcRaft { raft, client };
                v.insert(hc_raft.clone());
                hc_raft
            }
            std::collections::hash_map::Entry::Occupied(o) => o.get().clone(),
        }
    }
}
