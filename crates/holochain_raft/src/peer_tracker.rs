use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use holochain_types::prelude::*;
use openraft::{error::RaftError, ChangeMembers, QuorumSet};
use tokio::{sync::Mutex, time::Instant};

use crate::{HcNode, Raft};

pub struct PeerTracker {
    last_seen: BTreeMap<AgentPubKey, Instant>,
}

impl PeerTracker {
    pub fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            last_seen: Default::default(),
        }))
    }

    pub fn touch(&mut self, agent: AgentPubKey) {
        self.last_seen.insert(agent, Instant::now());
    }

    pub async fn handle_absentees(&mut self, raft: &Raft) {
        let unresponsive = self
            .unresponsive_members(raft, crate::PRESENCE_WINDOW)
            .await;

        // XXX: this hack ensures that we only attempt removing nodes once per PRESENCE_WINDOW.
        //      if they are removed by the next time the interval expires, they won't show up
        //      in the next unresponsive set.
        for p in unresponsive.iter() {
            self.touch(p.agent());
        }

        dbg!(&unresponsive);

        if let Err(e) = raft
            .change_membership(ChangeMembers::RemoveVoters(unresponsive), true)
            .await
        {
            tracing::error!("Failed to remove absentees: {e:?}");
        }
    }

    /// Returns the set of peers that have been seen in the last `interval` seconds.
    /// NOTE, this does not include the local node.
    pub fn responsive_peers(&self, interval: Duration) -> BTreeSet<HcNode> {
        self.last_seen
            .iter()
            .filter(|(_, t)| t.elapsed() < interval)
            .map(|(to, _)| HcNode::from(to.clone()))
            .collect()
    }

    async fn unresponsive_members(&self, raft: &Raft, interval: Duration) -> BTreeSet<HcNode> {
        let here = self.responsive_peers(interval);

        let all_members = raft
            .with_raft_state(move |s| {
                s.membership_state
                    .effective()
                    .voter_ids()
                    .collect::<BTreeSet<_>>()
            })
            .await
            .unwrap_or_default();

        all_members.difference(&here).cloned().collect()
    }

    pub fn last_seen(&self) -> &BTreeMap<AgentPubKey, Instant> {
        &self.last_seen
    }
}
