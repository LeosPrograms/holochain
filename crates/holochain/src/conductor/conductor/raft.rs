use holochain_conductor_api::{
    RaftInterfaceRequest, RaftInterfaceRequestPayload, RaftInterfaceResponsePayload,
};
use holochain_raft::{
    EntryPayload, HcClient, HcNetworkFactory, MemLogStore, Raft, RaftLogReader, RaftLogStorage,
    RaftRpcRequest, RaftRpcResponse,
};

use super::*;

impl Conductor {
    pub(crate) async fn get_raft(&self, dna_hash: DnaHash, workspace_hash: EntryHash) -> Raft {
        let provenance = crate::core::workflow::sys_validation_workflow::get_representative_agent(
            self, &dna_hash,
        )
        .expect("TODO");

        let (raft, _, _) = self.lookup_raft(dna_hash, provenance, workspace_hash).await;
        raft
    }

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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use holochain_raft::RaftOp;
    use holochain_wasm_test_utils::TestWasm;

    use super::*;
    use crate::sweettest::*;

    /// Wait for the cluster to settle on an elected leader. Only returns when all running conductors agree.
    async fn await_leader(
        batch: impl IntoIterator<Item = &SweetConductor>,
        cells: impl IntoIterator<Item = &SweetCell>,
        workspace: &EntryHash,
        not_this_one: Option<usize>,
    ) -> usize {
        let batch = batch.into_iter().collect_vec();
        let cells = cells.into_iter().collect_vec();
        let dna_hash = cells[0].dna_hash();
        let start = std::time::Instant::now();
        loop {
            let mut leaders = BTreeSet::new();
            for c in batch.iter() {
                if c.is_running() {
                    let raft = c.get_raft(dna_hash.clone(), workspace.clone()).await;
                    let leader = raft.current_leader().await;
                    leaders.insert(leader.map(|l| l.agent()));
                }
            }
            if leaders.len() == 1 {
                if let Some(agent) = leaders.pop_first().unwrap() {
                    let (leader_index, _) = cells
                        .iter()
                        .find_position(|c| c.agent_pubkey() == &agent)
                        .unwrap();

                    // Skip the one we're not interested in
                    if Some(leader_index) != not_this_one {
                        println!("leader {leader_index} found in {:?}", start.elapsed());
                        return leader_index;
                    }
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

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

        for (i, c) in cells.iter().enumerate() {
            println!("cell {}: {}", i, c.agent_pubkey());
        }
        conductors.exchange_peer_info().await;

        let mk_payload = |payload| RaftInterfaceRequest {
            dna_hash: dna_hash.clone(),
            workspace: workspace.clone(),
            payload,
        };

        // Initialize the first conductor with a raft with only itself
        conductors[0]
            .handle_raft_interface_call(mk_payload(RaftInterfaceRequestPayload::Initialize(vec![
                cells[0].agent_pubkey().clone(),
            ])))
            .await
            .unwrap();

        // wait for self-election
        let leader_index = await_leader([&conductors[0]], [&cells[0]], &workspace, None).await;
        assert_eq!(leader_index, 0);

        for i in 1..num {
            // All known peers up to this point
            let peers = cells
                .iter()
                .take(i + 1)
                .map(|c| c.agent_pubkey().clone())
                .collect_vec();

            // Set up the raft with all known nodes up to this point
            //
            // This may error with NotAllowed if a raft message was already sent from another initialized node.
            // If so it's safe to ignore.
            let _ = conductors[i]
                .handle_raft_interface_call(mk_payload(RaftInterfaceRequestPayload::Initialize(
                    peers.clone(),
                )))
                .await;

            // Broadcast a request to all known peers to be added to their raft cluster.
            // In reality the message only needs to be sent to the leader, and in fact only the leader
            // can process the request. Broadcasting is just a quicker way to get the message out to the leader.
            // If the cluster has no elected leader at the time of the request, this will fail and need to be retried.
            // TODO: test the above.
            // TODO: Join and Initialize will pretty much always go together, so maybe they should be combined.
            let res = conductors[i]
                .handle_raft_interface_call(mk_payload(RaftInterfaceRequestPayload::Join(peers)))
                .await;
            let _ = dbg!(res);
        }

        // Wait for all clusters to agree on a leader
        let leader_index = await_leader(conductors.iter(), &cells, &workspace, None).await;
        dbg!(leader_index);

        // Let each node propose an op
        for i in 0..num {
            conductors[i]
                .handle_raft_interface_call(mk_payload(RaftInterfaceRequestPayload::Propose(
                    RaftOp(vec![i as u8]),
                )))
                .await
                .unwrap();
        }

        // Make the leader crash
        conductors[leader_index].shutdown().await;

        // TODO: there will be errors about not being able to connect to the leader.
        // Need to make a good UX for that.

        // for i in 0..num {
        //     if i == leader_index {
        //         continue;
        //     }

        //     conductors[i]
        //         .handle_raft_interface_call(mk_payload(RaftInterfaceRequestPayload::Propose(
        //             RaftOp(vec![i as u8]),
        //         )))
        //         .await
        //         .unwrap();
        // }

        // Wait for the survivors to agree on a new leader
        let leader2 = await_leader(conductors.iter(), &cells, &workspace, Some(leader_index)).await;
        dbg!(leader2);
        assert_ne!(leader_index, leader2);

        // Check that all ops are still retrievable by the remaining voters
        for i in 0..num {
            if i == leader_index {
                continue;
            }

            let ops = conductors[i]
                .handle_raft_interface_call(mk_payload(
                    RaftInterfaceRequestPayload::GetUserLogEntries(None),
                ))
                .await
                .unwrap();

            assert_eq!(
                ops.unwrap_user_log_entries().len(),
                num,
                "agent {i} can't get all the ops"
            );
        }
    }
}
