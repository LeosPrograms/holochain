use std::collections::BTreeSet;
use std::sync::Arc;

use crate::memstore::{new_mem_store, HcNode, MemLogStore, TypeConfig};
use crate::message::{ProposalResponse, RaftRpcRequest, RaftRpcRequestPayload, RaftRpcResponse};
use crate::ClientRequest;

use holo_hash::AgentPubKey;
use maplit::{btreemap, btreeset};
use openraft::{ChangeMembers, Config, Raft, RaftNetworkFactory};

pub type RaftMem = Raft<TypeConfig>;

pub async fn new_raft_mem(
    id: HcNode,
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
    msg: RaftRpcRequestPayload,
    remote_agent: AgentPubKey,
) -> anyhow::Result<RaftRpcResponse> {
    let response = match msg {
        RaftRpcRequestPayload::AppendEntries(req) => raft.append_entries(req).await?.into(),
        RaftRpcRequestPayload::InstallSnapshot(req) => raft.install_snapshot(req).await?.into(),
        RaftRpcRequestPayload::Vote(req) => raft.vote(req).await?.into(),

        RaftRpcRequestPayload::ProposeOp(op) => {
            match raft.client_write(ClientRequest::Op(op)).await {
                Err(e) => {
                    if let Some(maybe_leader) = e.forward_to_leader() {
                        if let Some(leader) = maybe_leader.leader_id.as_ref() {
                            RaftRpcResponse::Proposal(ProposalResponse::ForwardToLeader(
                                leader.agent(),
                            ))
                        } else {
                            RaftRpcResponse::Proposal(ProposalResponse::NoLeader)
                        }
                    } else {
                        anyhow::bail!("handle_incoming_request: unexpected error: {e:?}");
                    }
                }
                Ok(_) => RaftRpcResponse::Proposal(ProposalResponse::Accepted),
            }
        }

        RaftRpcRequestPayload::Initialize(peers) => {
            raft.initialize(
                peers
                    .into_iter()
                    .map(|p| HcNode::from(p))
                    .collect::<BTreeSet<_>>(),
            )
            .await?;
            RaftRpcResponse::Proposal(ProposalResponse::Accepted)
        }

        RaftRpcRequestPayload::Join(agent) => raft
            .change_membership(
                ChangeMembers::AddVoters(btreemap![agent.into() => ()]),
                false,
            )
            .await
            .map(|_| RaftRpcResponse::Proposal(ProposalResponse::Accepted))?,

        RaftRpcRequestPayload::Leave(agent) => raft
            .change_membership(ChangeMembers::RemoveVoters(btreeset![agent.into()]), false)
            .await
            .map(|_| RaftRpcResponse::Proposal(ProposalResponse::Accepted))?,
    };
    Ok(response)
}
