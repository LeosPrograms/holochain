use holochain_raft::{ClientRequest, ClientResponse};

use super::*;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, SerializedBytes)]
pub struct RaftInterfaceRequest {
    /// Hash of the network which contains the peers to sync with, e.g. `syn`
    pub dna_hash: DnaHash,
    /// A new raft instance is created for each workspace
    pub workspace: EntryHash,
    /// The actual request
    pub payload: RaftInterfaceRequestPayload,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, SerializedBytes)]
pub struct RaftInterfaceResponse {
    /// The workspace the request was made for
    pub workspace: EntryHash,
    /// The actual response
    pub payload: RaftInterfaceResponsePayload,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, SerializedBytes)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum RaftInterfaceRequestPayload {
    /// Initialize the raft network with the provided peers
    Initialize(Vec<AgentPubKey>),
    /// Add the provided peers to the raft network
    Join(Vec<AgentPubKey>),
    /// Leave the raft network
    Leave,
    /// Propose an operation to the raft network
    Propose(holochain_raft::RaftOp),
    /// Get log entries after the given log id
    GetAllLogEntries(Option<u64>),
    /// Get user-created log entries after the given log id
    GetUserLogEntries(Option<u64>),
}

#[derive(
    Clone,
    Debug,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    SerializedBytes,
    derive_more::Unwrap,
)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum RaftInterfaceResponsePayload {
    AllLogEntries(Vec<holochain_raft::Entry>),
    UserLogEntries(Vec<ClientRequest>),
    Ok,
    NoLeader,
}
