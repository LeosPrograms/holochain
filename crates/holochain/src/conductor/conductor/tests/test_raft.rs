use std::collections::BTreeSet;

use holochain_conductor_api::{RaftInterfaceRequest, RaftInterfaceRequestPayload};
use holochain_raft::RaftOp;
use holochain_wasm_test_utils::TestWasm;

use super::raft::*;
use super::*;
use crate::sweettest::*;

#[tokio::test(flavor = "multi_thread")]
async fn test_raft() {
    let num = 5;
    let raft_id: RaftId = EntryHash::from_raw_32(vec![55; 32]).into();
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
        raft_id: raft_id.clone(),
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
    let leader_index = await_leader([&conductors[0]], [&cells[0]], &raft_id, None).await;
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
        let _ = res;
    }

    // Wait for all clusters to agree on a leader
    let leader_index = await_leader(conductors.iter(), &cells, &raft_id, None).await;
    dbg!(leader_index);

    // Let each node propose an op
    for i in 0..num {
        conductors[i]
            .handle_raft_interface_call(mk_payload(RaftInterfaceRequestPayload::Propose(RaftOp(
                vec![i as u8],
            ))))
            .await
            .unwrap();
    }

    // Make over half of the conductors crash
    for i in 0..(num + 1) / 2 {
        conductors[i].shutdown().await;
        println!("SHUTDOWN {i}");
    }

    // // Make the leader crash
    // conductors[leader_index].shutdown().await;

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
    let leader2 = await_leader(conductors.iter(), &cells, &raft_id, Some(leader_index)).await;
    dbg!(leader2);
    assert_ne!(leader_index, leader2);

    // Check that all ops are still retrievable by the remaining voters
    for i in num / 2..num {
        if i == leader_index {
            continue;
        }

        let ops = conductors[i]
            .handle_raft_interface_call(mk_payload(RaftInterfaceRequestPayload::GetUserLogEntries(
                None,
            )))
            .await
            .unwrap();

        assert_eq!(
            ops.unwrap_user_log_entries().len(),
            num,
            "agent {i} can't get all the ops"
        );
    }
}

/// Wait for the cluster to settle on an elected leader. Only returns when all running conductors agree.
async fn await_leader(
    batch: impl IntoIterator<Item = &SweetConductor>,
    cells: impl IntoIterator<Item = &SweetCell>,
    raft_id: &RaftId,
    not_this_one: Option<usize>,
) -> usize {
    let batch = batch.into_iter().collect_vec();
    let cells = cells.into_iter().collect_vec();
    let dna_hash = cells[0].dna_hash();
    let start = std::time::Instant::now();
    loop {
        let mut leaders = BTreeSet::new();
        for (cond, cell) in batch.iter().zip(cells.iter()) {
            if cond.is_running() {
                let data = cond.get_raft(dna_hash.clone(), raft_id.clone()).await;
                let leader = data.raft.current_leader().await;
                leaders.insert(leader.map(|l| l.agent()));

                let mut forker = data.client.forker.lock().await;
                let forking_time = forker.its_forking_time(&data.raft).await;
                let present: BTreeSet<String> = forker
                    .whos_here(holochain_raft::PRESENCE_WINDOW)
                    .into_iter()
                    .map(|a| a.suffix(4))
                    .collect();
                println!(
                    "{}: {} {:?}",
                    cell.agent_pubkey().suffix(4),
                    forking_time,
                    present
                );

                // for (a, t) in data.client.forker.lock().await.last_seen().iter() {
                //     println!("{}->{}: {:?}", cell.agent_pubkey(), a, t.elapsed());
                // }
            }
        }
        println!("-----------");
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
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    }
}
