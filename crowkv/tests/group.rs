use crowkv::group::group::{PxGroup, PxGroupConfig, PxGroupMember};

fn sample_group() -> PxGroup {
    PxGroup::new(
        PxGroupConfig {
            group_id: 1,
            members: vec![
                PxGroupMember {
                    node_id: 1,
                    endpoint: "127.0.0.1:1".to_string(),
                    voting: true,
                },
                PxGroupMember {
                    node_id: 2,
                    endpoint: "127.0.0.1:2".to_string(),
                    voting: true,
                },
            ],
            quorum_size: 2,
            config_version: 1,
        },
        1,
        1,
    )
}

#[test]
fn endpoint_cache_refresh_and_update() {
    let mut group = sample_group();
    assert_eq!(group.leader_endpoint().as_deref(), Some("127.0.0.1:1"));
    assert_eq!(group.member_endpoint(2), Some("127.0.0.1:2"));

    group.update_member_endpoint(1, "127.0.0.1:11");
    assert_eq!(group.leader_endpoint().as_deref(), Some("127.0.0.1:11"));

    group.update_leader_id(2);
    assert_eq!(group.leader_endpoint().as_deref(), Some("127.0.0.1:2"));

    group.leader_id = 1;
    group.group_config.members[0].endpoint = "127.0.0.1:111".to_string();
    group.refresh_endpoint_cache();
    assert_eq!(group.leader_endpoint().as_deref(), Some("127.0.0.1:111"));
}
