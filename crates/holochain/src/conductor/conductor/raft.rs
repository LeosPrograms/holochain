use holochain_conductor_api::{
    RaftInterfaceRequest, RaftInterfaceRequestPayload, RaftInterfaceResponsePayload,
};
use holochain_raft::{
    EntryPayload, HcClient, HcNetworkFactory, MemLogStore, Raft, RaftLogReader, RaftLogStorage,
    RaftRpcRequest, RaftRpcResponse,
};

use super::*;

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
        request: RaftRpcRequest,
        remote_agent: AgentPubKey,
    ) -> ConductorResult<RaftRpcResponse> {
        // TODO: the representative agent must change if this agent ever leaves the network (and there are other local agents)
        let local_agent = crate::core::workflow::sys_validation_workflow::get_representative_agent(
            self, &dna_hash,
        )
        .expect("TODO");

        let data = self
            .lookup_raft(dna_hash.clone(), local_agent.clone(), request.raft_id)
            .await;

        let res = holochain_raft::handle_incoming_request(
            &data.raft,
            request.payload,
            remote_agent.clone(),
        )
        .await
        .map_err(|e| ConductorError::other(format!("TODO handle_incoming_request error: {e:?}")))?;

        {
            let mut forker = data.client.forker.lock().await;
            forker.touch(remote_agent);
            if forker.its_forking_time(&data.raft).await {
                // TODO: fork
            }
        }

        Ok(res)
    }

    pub(crate) async fn handle_raft_interface_call(
        &self,
        raft_call: RaftInterfaceRequest,
    ) -> ConductorResult<RaftInterfaceResponsePayload> {
        let dna_hash = raft_call.dna_hash.clone();

        // TODO: the representative agent must change if this agent ever leaves the network (and there are other local agents)
        let provenance = crate::core::workflow::sys_validation_workflow::get_representative_agent(
            self, &dna_hash,
        )
        .expect("TODO");

        let HcRaft {
            mut storage,
            client,
            ..
        } = self
            .lookup_raft(dna_hash.clone(), provenance.clone(), raft_call.raft_id)
            .await;

        match raft_call.payload {
            RaftInterfaceRequestPayload::Initialize(peers) => {
                let zome_call_params = client
                    .zome_call_params(
                        provenance.clone(),
                        holochain_raft::RaftRpcRequestPayload::Initialize(peers),
                    )
                    .map_err(|e| {
                        ConductorError::other(format!("couldn't format zome call params: {e:?}"))
                    })?;

                self.call_zome(zome_call_params)
                    .await
                    .map_err(|e| ConductorError::other(format!("can't initialize: {e:?}")))??;

                Ok(RaftInterfaceResponsePayload::Ok)
            }
            RaftInterfaceRequestPayload::Join(peers) => {
                // Ask all known peers to join
                future::join_all(peers.into_iter().map(move |peer| {
                    let client = client.clone();
                    let provenance = provenance.clone();
                    async move {
                        let res = client
                            .call(
                                peer.clone(),
                                holochain_raft::RaftRpcRequestPayload::Join(provenance.clone()),
                            )
                            .await;
                        match res {
                            Ok(holochain_raft::RaftRpcResponse::Proposal(
                                holochain_raft::ProposalResponse::Accepted,
                            )) => Ok(()),
                            Ok(holochain_raft::RaftRpcResponse::Proposal(e)) => Err(
                                ConductorError::other(format!("Can't connect to leader: {e:?}",)),
                            ),
                            res => Err(ConductorError::other(format!(
                                "Error from leader while joining: {res:?}",
                            ))),
                        }
                    }
                }))
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;
                Ok(RaftInterfaceResponsePayload::Ok)
            }
            RaftInterfaceRequestPayload::Leave => {
                client
                    .call_leader_with_retry(holochain_raft::RaftRpcRequestPayload::Leave(
                        provenance.clone(),
                    ))
                    .await
                    .map_err(|_| ConductorError::other("can't leave"))?;
                Ok(RaftInterfaceResponsePayload::Ok)
            }
            RaftInterfaceRequestPayload::Propose(op) => {
                // XXX: first call is to self. No need to use the client for this.
                match client
                    .call_leader_with_retry(holochain_raft::RaftRpcRequestPayload::ProposeOp(op))
                    .await
                {
                    Ok(holochain_raft::RaftRpcResponse::Proposal(_res)) => {
                        Ok(RaftInterfaceResponsePayload::Ok)
                    }
                    Ok(res) => Err(ConductorError::other(format!(
                        "Unexpected response from leader during proposal: {res:?}",
                    ))),
                    Err(e) => Err(ConductorError::other(e.to_string())),
                }
            }
            RaftInterfaceRequestPayload::GetAllLogEntries(index) => {
                let mut reader = storage.get_log_reader().await;
                let entries = if let Some(index) = index {
                    reader.try_get_log_entries(index..).await
                } else {
                    reader.try_get_log_entries(..).await
                }
                .map_err(|e| ConductorError::other(e.to_string()))?;
                Ok(RaftInterfaceResponsePayload::AllLogEntries(entries))
            }
            RaftInterfaceRequestPayload::GetUserLogEntries(index) => {
                let mut reader = storage.get_log_reader().await;

                let entries = if let Some(index) = index {
                    reader.try_get_log_entries(index..).await
                } else {
                    reader.try_get_log_entries(..).await
                }
                .map_err(|e| ConductorError::other(e.to_string()))?
                .into_iter()
                .filter_map(|l| match l.payload {
                    EntryPayload::Normal(n) => Some(n),
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
                    forker: holochain_raft::Forker::new(),
                };
                let network = HcNetworkFactory {
                    client: client.clone(),
                };
                let (raft, storage) =
                    holochain_raft::new_raft_mem(local_agent.clone().into(), network)
                        .await
                        .map_err(|e| ConductorError::other(e.to_string()))
                        .expect("TODO");
                let hc_raft = HcRaft {
                    raft,
                    storage,
                    client,
                };
                v.insert(hc_raft.clone());
                hc_raft
            }
            std::collections::hash_map::Entry::Occupied(o) => o.get().clone(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HcRaftError<RE> {
    #[error("No current leader. Must wait for a new leader to be elected.")]
    NoLeader,

    #[error(transparent)]
    RaftError(#[from] RE),
}
