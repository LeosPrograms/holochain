use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use holochain_types::prelude::*;
use openraft::QuorumSet;
use tokio::{sync::Mutex, time::Instant};

pub struct PeerTracker {
    last_seen: BTreeMap<AgentPubKey, Instant>,
    first_instant_without_quorum: Option<Instant>,
}

impl PeerTracker {
    pub fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            last_seen: Default::default(),
            first_instant_without_quorum: None,
            // active_fork: None,
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
}
