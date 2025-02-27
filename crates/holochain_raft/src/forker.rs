use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use holochain_types::prelude::*;
use openraft::QuorumSet;
use tokio::{sync::Mutex, time::Instant};

use crate::{memstore::HcNode, Raft, RaftForkId};

pub struct Forker {
    last_seen: BTreeMap<AgentPubKey, Instant>,
    first_instant_without_quorum: Option<Instant>,
    pub active_fork: Option<RaftForkId>,
}

impl Forker {
    pub fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            last_seen: Default::default(),
            first_instant_without_quorum: None,
            active_fork: None,
        }))
    }
    pub fn touch(&mut self, agent: AgentPubKey) {
        self.last_seen.insert(agent, Instant::now());
    }

    pub fn who_else_is_here(&self, interval: Duration) -> BTreeSet<AgentPubKey> {
        self.last_seen
            .iter()
            .filter(|(_, t)| t.elapsed() < interval)
            .map(|(to, _)| to)
            .cloned()
            .collect()
    }

    pub fn last_seen(&self) -> &BTreeMap<AgentPubKey, Instant> {
        &self.last_seen
    }

    pub async fn its_forking_time(&mut self, raft: &Raft) -> bool {
        if self.active_fork.is_some() {
            return false;
        }

        let here: BTreeSet<HcNode> = self
            .who_else_is_here(crate::PRESENCE_WINDOW)
            .into_iter()
            .map(HcNode::from)
            .collect();

        let is_quorum = raft
            .with_raft_state(move |s| s.membership_state.effective().is_quorum(here.iter()))
            .await
            .unwrap_or(false);

        if is_quorum {
            self.first_instant_without_quorum = None;
        } else if self.first_instant_without_quorum.is_none() {
            self.first_instant_without_quorum = Some(Instant::now());
        }

        if let Some(t) = self.first_instant_without_quorum {
            t.elapsed() > crate::FORKING_WINDOW
        } else {
            false
        }
    }
}
