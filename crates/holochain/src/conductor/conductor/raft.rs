use holochain_conductor_api::{
    RaftInterfaceRequest, RaftInterfaceRequestPayload, RaftInterfaceResponsePayload,
};
use holochain_raft::{
    error::{ClientWriteError, RaftError},
    HcClient, HcNetworkFactory, MemLogStore, ProposalResponse, Raft, RaftLogReader, RaftLogStorage,
    RaftRpcRequest, RaftRpcResponse, TypeConfig,
};

use super::*;

impl Conductor {
    pub(crate) async fn handle_raft_rpc_call(
        &self,
        dna_hash: DnaHash,
        request: RaftRpcRequest,
    ) -> ConductorResult<RaftRpcResponse> {
        // TODO: the representative agent must change if this agent ever leaves the network (and there are other local agents)
        let provenance = crate::core::workflow::sys_validation_workflow::get_representative_agent(
            self, &dna_hash,
        )
        .expect("TODO");

        let (raft, _, _) = self
            .lookup_raft(dna_hash.clone(), provenance.clone(), request.workspace)
            .await;

        holochain_raft::handle_incoming_request(&raft, request.payload)
            .await
            .map_err(|e| {
                ConductorError::other(format!("TODO handle_incoming_request error: {e:?}"))
            })
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

        let (_, mut storage, client) = self
            .lookup_raft(dna_hash.clone(), provenance.clone(), raft_call.workspace)
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
            RaftInterfaceRequestPayload::GetLogEntries(log_id) => {
                let mut reader = storage.get_log_reader().await;
                let entries = reader
                    .try_get_log_entries(log_id.index..)
                    .await
                    .map_err(|e| ConductorError::other(e.to_string()))?;
                Ok(RaftInterfaceResponsePayload::LogEntries(entries))
            }
        }
    }

    async fn lookup_raft(
        &self,
        dna_hash: DnaHash,
        provenance: AgentPubKey,
        workspace: EntryHash,
    ) -> (Raft, Arc<MemLogStore>, HcClient) {
        let mut rafts = self.rafts.lock().await;

        match rafts.entry((dna_hash.clone(), workspace.clone())) {
            std::collections::hash_map::Entry::Vacant(v) => {
                let client = HcClient {
                    provenance: provenance.clone(),
                    keystore: self.keystore().clone(),
                    workspace: workspace.clone(),
                    network: self.holochain_p2p().to_dna(dna_hash.clone(), None),
                };
                let network = HcNetworkFactory {
                    client: client.clone(),
                };
                let (raft, storage) =
                    holochain_raft::new_raft_mem(provenance.clone().into(), network)
                        .await
                        .map_err(|e| ConductorError::other(e.to_string()))
                        .expect("TODO");
                let tup = (raft, storage, client);
                v.insert(tup.clone());
                tup
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

pub type HcRaftResult<T, RE> = Result<T, HcRaftError<RE>>;

#[cfg(test)]
mod tests {
    use holochain_raft::RaftOp;
    use holochain_wasm_test_utils::TestWasm;

    use super::*;
    use crate::sweettest::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_raft() {
        let num = 5;
        let workspace = EntryHash::from_raw_32(vec![55; 32]);
        let config = SweetConductorConfig::standard();
        let mut conductors = SweetConductorBatch::from_config(num, config).await;

        let (dna_file, _, _) = SweetDnaFile::unique_from_test_wasms(vec![TestWasm::Anchor]).await;
        let dna_hash = dna_file.dna_hash().clone();

        let apps = conductors.setup_app("app", &[dna_file]).await.unwrap();
        let cells = apps.cells_flattened();
        conductors.exchange_peer_info().await;

        let mk_request = |i: usize, payload: RaftInterfaceRequestPayload| {
            conductors[i].handle_raft_interface_call(RaftInterfaceRequest {
                dna_hash: dna_hash.clone(),
                workspace: workspace.clone(),
                payload,
            })
        };

        for i in 0..num {
            let response = mk_request(
                i,
                RaftInterfaceRequestPayload::Initialize(
                    cells.iter().map(|c| c.agent_pubkey().clone()).collect(),
                ),
            )
            .await
            .unwrap();

            assert_eq!(response, RaftInterfaceResponsePayload::Ok);
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

        mk_request(0, RaftInterfaceRequestPayload::Propose(RaftOp(vec![0])))
            .await
            .unwrap();

        mk_request(1, RaftInterfaceRequestPayload::Propose(RaftOp(vec![1])))
            .await
            .unwrap();

        mk_request(2, RaftInterfaceRequestPayload::Propose(RaftOp(vec![2])))
            .await
            .unwrap();
    }
}
