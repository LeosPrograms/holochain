use super::*;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, SerializedBytes)]
pub struct RaftRequest {
    pub workspace: EntryHash,
    pub payload: RaftRequestPayload,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, SerializedBytes)]
pub struct RaftResponse {
    pub workspace: EntryHash,
    pub payload: RaftResponsePayload,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, SerializedBytes)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum RaftRequestPayload {
    Join,
    Leave,
    Propose(holochain_raft::Entry),
    GetLogEntries(holochain_raft::LogId),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, SerializedBytes)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum RaftResponsePayload {
    LogEntries(Vec<holochain_raft::Entry>),
    Ok,
}
