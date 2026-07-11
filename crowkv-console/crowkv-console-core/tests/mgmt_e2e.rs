//! C5 end-to-end: spawn a real `crowkv-server`, drive the full
//! store → group → remote lifecycle through the `mgmt` client, and
//! verify each call's side effect via the corresponding `GET`.
//!
//! Skips silently when the `crowkv-server` binary is not built.

use std::time::Duration;

use crowkv_console_core::clients::http::ServerClient;
use crowkv_console_core::config::{NodeEntry, RackEntry};
use crowkv_console_core::lifecycle::{self, crowkv_server_bin, DeployRequest};
use crowkv_console_core::mgmt::{AddGroupRequest, AddStoreRequest, RemoteReplicaInfo};

fn pick_free_port() -> u16 {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

async fn spawn_server() -> Option<(u32, ServerClient)> {
    let bin = crowkv_server_bin()?;
    if !bin.exists() {
        return None;
    }
    let node = NodeEntry {
        id: "n1".into(),
        rack_id: "r1".into(),
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
    };
    let _ = RackEntry {
        id: "r1".into(),
        name: "r1".into(),
    };
    let req = DeployRequest {
        server_id: "s1".into(),
        mgmt_port: pick_free_port(),
        grpc_port: pick_free_port(),
        binary: Some(bin),
    };
    let deployed = lifecycle::deploy_local(&req, &node).await.expect("deploy_local");
    let client = ServerClient::new(deployed.mgmt_url.clone()).unwrap();
    Some((deployed.pid, client))
}

#[tokio::test]
async fn full_store_group_remote_cycle() {
    let Some((pid, client)) = spawn_server().await else {
        eprintln!("skipping: crowkv-server binary not built");
        return;
    };

    // The server bootstraps with `--stores 1 --groups 1 --replica 1` by
    // default, so pick IDs outside that range.
    let store_id: u64 = 5;
    let group_id: u64 = 50;
    let replica_id: u64 = 500;

    client
        .add_store(&AddStoreRequest {
            store_id,
            group_id,
            replica_id,
            port: None,
        })
        .await
        .expect("add_store");

    // 2. list_stores sees it.
    let stores = client.list_stores().await.expect("list_stores");
    assert!(stores.iter().any(|s| s.store_id == store_id), "add_store not reflected in list_stores: {stores:?}");

    // 3. get_store returns the bootstrap group.
    let detail = client.get_store(store_id).await.expect("get_store");
    assert_eq!(detail.store_id, store_id);
    assert_eq!(detail.groups.len(), 1);
    assert_eq!(detail.groups[0].group_id, group_id);
    assert_eq!(detail.groups[0].local_replica_id, replica_id);

    // 4. Add a second group.
    let group_id_2: u64 = 60;
    let replica_id_2: u64 = 600;
    client
        .add_group(
            store_id,
            &AddGroupRequest {
                group_id: group_id_2,
                replica_id: replica_id_2,
            },
        )
        .await
        .expect("add_group");

    let groups = client.list_groups(store_id).await.expect("list_groups");
    assert_eq!(groups.len(), 2, "expected both groups, got {groups:?}");

    // 5. Add a remote replica on the second group.
    let remotes = vec![RemoteReplicaInfo {
        replica_id: 601,
        endpoint: "127.0.0.1:39999".into(),
    }];
    client.add_remotes(store_id, group_id_2, &remotes).await.expect("add_remotes");
    let got = client.list_remotes(store_id, group_id_2).await.expect("list_remotes");
    assert_eq!(got, remotes, "add_remotes round trip");

    // 6. Delete the remote.
    client.remove_remote(store_id, group_id_2, 601).await.expect("remove_remote");
    let got = client.list_remotes(store_id, group_id_2).await.expect("list_remotes after delete");
    assert!(got.is_empty(), "remote should be gone: {got:?}");

    // 7. Delete the second group.
    client.remove_group(store_id, group_id_2).await.expect("remove_group");
    let groups = client.list_groups(store_id).await.expect("list_groups after delete");
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].group_id, group_id);

    // 8. Tear the whole store down. The server currently lacks a
    //    DELETE /stores/{sid} handler — so this is allowed to fail; we
    //    assert the call completes without a transport panic.
    let _ = client.remove_store(store_id).await;

    // Cleanup: kill the server we spawned.
    let _ = lifecycle::stop_pid(pid);
    tokio::time::sleep(Duration::from_millis(50)).await;
}
