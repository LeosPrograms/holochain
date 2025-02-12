use std::sync::Arc;

use crate::memstore::{new_mem_store, MemLogStore, TypeConfig};
use crate::message::{ProposalResponse, RaftRpcRequest, RaftRpcResponse};
use crate::ClientRequest;

use openraft::{Config, Raft, RaftNetworkFactory};

pub type RaftMem = Raft<TypeConfig>;
pub type NodeId = u64;

pub async fn new_raft_mem(
    id: NodeId,
    network: impl RaftNetworkFactory<TypeConfig>,
) -> anyhow::Result<(RaftMem, Arc<MemLogStore>)> {
    let config = Arc::new(
        Config {
            heartbeat_interval: 100,
            election_timeout_min: 250,
            election_timeout_max: 500,
            ..Default::default()
        }
        .validate()?,
    );
    let (storage, state_machine) = new_mem_store();
    let raft = Raft::new(id, config, network, storage.clone(), state_machine).await?;
    Ok((raft, storage))
}

/// TODO: handle errors
pub async fn handle_incoming_request(
    raft: &RaftMem,
    msg: RaftRpcRequest,
) -> Option<RaftRpcResponse> {
    let response = match msg {
        RaftRpcRequest::AppendEntries(req) => raft.append_entries(req).await.ok()?.into(),
        RaftRpcRequest::InstallSnapshot(req) => raft.install_snapshot(req).await.ok()?.into(),
        RaftRpcRequest::Vote(req) => raft.vote(req).await.ok()?.into(),

        RaftRpcRequest::ProposeOp(op) => match raft.client_write(ClientRequest::Op(op)).await {
            Err(e) => {
                if let Some(maybe_leader) = e.forward_to_leader() {
                    if let Some(leader) = maybe_leader.leader_node.as_ref() {
                        RaftRpcResponse::Proposal(ProposalResponse::ForwardToLeader(
                            leader.agent.clone(),
                        ));
                    } else {
                        RaftRpcResponse::Proposal(ProposalResponse::NoLeader);
                    }
                }
                return None;
            }
            Ok(_) => RaftRpcResponse::Proposal(ProposalResponse::Accepted),
        },
        RaftRpcRequest::Join(agent) => raft
            .change_membership(vec![agent], false)
            .await
            .ok()?
            .into(),
        RaftRpcRequest::Leave => todo!(),
    };
    Some(response)
}
