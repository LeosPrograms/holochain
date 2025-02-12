use holochain_conductor_api::{
    RaftInterfaceRequest, RaftInterfaceRequestPayload, RaftInterfaceResponsePayload,
};
use holochain_raft::{
    error::{ClientWriteError, RaftError},
    HcClient, HcNetworkFactory, RaftLogReader, RaftLogStorage, TypeConfig,
};

use super::*;

impl Conductor {
    pub(crate) async fn handle_raft_call(
        &self,
        raft_call: RaftInterfaceRequest,
    ) -> ConductorResult<RaftInterfaceResponsePayload> {
        let (raft, mut storage, client) = {
            let mut rafts = self.rafts.lock().await;
            let num = rafts.len();
            let dna_hash = raft_call.dna_hash.clone();

            // TODO: the representative agent must change if this agent ever leaves the network (and there are other local agents)
            let provenance =
                crate::core::workflow::sys_validation_workflow::get_representative_agent(
                    self, &dna_hash,
                )
                .expect("TODO");

            match rafts.entry(raft_call.workspace) {
                std::collections::hash_map::Entry::Vacant(v) => {
                    let client = HcClient {
                        provenance,
                        keystore: self.keystore().clone(),
                        network: self.holochain_p2p().to_dna(dna_hash.clone(), None),
                    };
                    let network = HcNetworkFactory {
                        client: client.clone(),
                    };
                    let (raft, storage) = holochain_raft::new_raft_mem(num as u64, network)
                        .await
                        .map_err(|e| ConductorError::other(e.to_string()))?;
                    let tup = (raft, storage, client);
                    v.insert(tup.clone());
                    tup
                }
                std::collections::hash_map::Entry::Occupied(o) => o.get().clone(),
            }
        };
        match raft_call.payload {
            RaftInterfaceRequestPayload::Join(peers) => {
                future::join_all(peers.into_iter().map(|peer| async move {
                    let res = client
                        .call(peer, holochain_raft::RaftRpcRequest::Join)
                        .await;
                    match res {
                        Ok(holochain_raft::RaftRpcResponse::Proposal(
                            holochain_raft::ProposalResponse::Accepted,
                        )) => Ok(RaftInterfaceResponsePayload::Ok),
                        Ok(holochain_raft::RaftRpcResponse::Proposal(_)) => {
                            Err(ConductorError::other("Can't connect to leader"))
                        }
                        _ => Err(ConductorError::other("Unexpected response from leader")),
                    }
                }))
                .await
                .collect::<Result<Vec<_>, _>>()
            }
            RaftInterfaceRequestPayload::Leave => {
                todo!("raft")
            }
            RaftInterfaceRequestPayload::Propose(op) => {
                // XXX: first call is to self. No need to use the client for this.
                match client
                    .call_leader_with_retry(
                        client.provenance.clone(),
                        holochain_raft::RaftRpcRequest::ProposeOp(op),
                    )
                    .await
                {
                    Ok(holochain_raft::RaftRpcResponse::Proposal(res)) => {
                        Ok(RaftInterfaceResponsePayload::Ok)
                    }
                    Ok(_) => Err(ConductorError::other("Unexpected response from leader")),
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
}

#[derive(Debug, thiserror::Error)]
pub enum HcRaftError<RE> {
    #[error("No current leader. Must wait for a new leader to be elected.")]
    NoLeader,

    #[error(transparent)]
    RaftError(#[from] RE),
}

pub type HcRaftResult<T, RE> = Result<T, HcRaftError<RE>>;
