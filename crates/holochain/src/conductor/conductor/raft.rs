use holochain_conductor_api::{RaftRequest, RaftRequestPayload, RaftResponsePayload};
use holochain_raft::{
    error::{ClientWriteError, RaftError},
    HcNetworkFactory, Raft, RaftLogReader, RaftLogStorage,
};

use super::*;

impl Conductor {
    pub(crate) async fn handle_raft_call(
        &self,
        raft_call: RaftRequest,
    ) -> ConductorResult<RaftResponsePayload> {
        let (raft, mut storage) = {
            let mut rafts = self.rafts.lock().await;
            let num = rafts.len();
            match rafts.entry(raft_call.workspace) {
                std::collections::hash_map::Entry::Vacant(v) => {
                    let network = HcNetworkFactory {
                            provenance: crate::core::workflow::sys_validation_workflow::get_representative_agent(self, &raft_call.dna_hash).expect("TODO"),
                            keystore: Arc::new(self.keystore().clone()),
                            network: self.holochain_p2p().to_dna(raft_call.dna_hash, None),
                        };
                    let raft = holochain_raft::new_raft_mem(num as u64, network)
                        .await
                        .map_err(|e| ConductorError::other(e.to_string()))?;
                    v.insert(raft.clone());
                    raft
                }
                std::collections::hash_map::Entry::Occupied(o) => o.get().clone(),
            }
        };
        match raft_call.payload {
            RaftRequestPayload::Join => {
                todo!("raft")
            }
            RaftRequestPayload::Leave => {
                todo!("raft")
            }
            RaftRequestPayload::Propose(op) => {
                match raft
                    .client_write(holochain_raft::ClientRequest::Op(op))
                    .await
                {
                    Ok(_) => Ok(RaftResponsePayload::Ok),
                    Err(RaftError::APIError(ClientWriteError::ForwardToLeader(e))) => {
                        if let Some(leader) = e.leader_node {
                            todo!()
                        } else {
                            todo!()
                        }
                    }
                    Err(e) => Err(ConductorError::other(e.to_string())),
                }
            }
            RaftRequestPayload::GetLogEntries(log_id) => {
                let mut reader = storage.get_log_reader().await;
                let entries = reader
                    .try_get_log_entries(log_id.index..)
                    .await
                    .map_err(|e| ConductorError::other(e.to_string()))?;
                Ok(RaftResponsePayload::LogEntries(entries))
            }
        }
    }
}
