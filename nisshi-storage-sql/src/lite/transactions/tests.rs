// Copyright ⓒ 2026 Nisshi contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use nisshi_sans_io::{
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    txn_offset_commit_request::{TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic},
};

async fn storage(directory: &tempfile::TempDir) -> ArcDynStorage {
    let filename = directory.path().file_name().unwrap().to_str().unwrap();
    let storage = Engine::builder()
        .cluster("atomic")
        .node(111)
        .advertised_listener("tcp://localhost:9092".parse().unwrap())
        .storage(format!("sqlite:///{filename}/broker.db").parse().unwrap())
        .build()
        .await
        .unwrap();
    storage
        .register_broker(BrokerRegistrationRequest {
            broker_id: 111,
            cluster_id: "atomic".into(),
            incarnation_id: Uuid::now_v7(),
            rack: None,
        })
        .await
        .unwrap();
    for name in ["events", "output"] {
        let _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(name.into())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some(vec![]))
                    .configs(Some(vec![])),
                false,
            )
            .await
            .unwrap();
    }
    storage
}

fn batch(producer: ProducerIdResponse) -> deflated::Batch {
    inflated::Batch::builder()
        .producer_id(producer.id)
        .producer_epoch(producer.epoch)
        .base_sequence(0)
        .attributes(BatchAttribute::default().transaction(true).into())
        .record(Record::builder().value(Some(Bytes::from_static(b"result"))))
        .build()
        .unwrap()
        .try_into()
        .unwrap()
}

#[tokio::test]
async fn failed_offset_publication_rolls_back_transaction_decision_and_marker() {
    let directory = tempfile::tempdir_in(".").unwrap();
    let storage = storage(&directory).await;
    let input = Topition::new("events", 0);
    let output = Topition::new("output", 0);
    let _ = storage
        .offset_commit(
            "projection",
            None,
            &[(input.clone(), OffsetCommitRequest::default().offset(0))],
        )
        .await
        .unwrap();
    let producer = storage
        .init_producer(Some("projection"), 30_000, None, None)
        .await
        .unwrap();
    let _ = storage
        .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: "projection".into(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            topics: vec![
                AddPartitionsToTxnTopic::default()
                    .name("output".into())
                    .partitions(Some(vec![0])),
            ],
        })
        .await
        .unwrap();
    let _ = storage
        .produce(Some("projection"), &output, batch(producer))
        .await
        .unwrap();
    assert_eq!(
        ErrorCode::None,
        storage
            .txn_add_offsets("projection", producer.id, producer.epoch, "projection")
            .await
            .unwrap()
    );
    let _ = storage
        .txn_offset_commit(TxnOffsetCommitRequest {
            transaction_id: "projection".into(),
            group_id: "projection".into(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            generation_id: None,
            member_id: None,
            group_instance_id: None,
            topics: vec![
                TxnOffsetCommitRequestTopic::default()
                    .name("events".into())
                    .partitions(Some(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .partition_index(0)
                            .committed_offset(1),
                    ])),
            ],
        })
        .await
        .unwrap();
    let database = libsql::Builder::new_local(directory.path().join("broker.db"))
        .build()
        .await
        .unwrap();
    let connection = database.connect().unwrap();
    let _ = connection
        .execute(
            "create trigger fail_offset before insert on consumer_offset
         when new.committed_offset = 1 begin select raise(ABORT, 'commit fault'); end",
            (),
        )
        .await
        .unwrap();
    assert!(
        storage
            .txn_end("projection", producer.id, producer.epoch, true)
            .await
            .is_err()
    );
    assert_eq!(
        Some(&0),
        storage
            .offset_fetch(Some("projection"), std::slice::from_ref(&input), Some(true))
            .await
            .unwrap()
            .get(&input)
    );
    let stage = storage.offset_stage(&output).await.unwrap();
    assert_eq!(0, stage.last_stable);
    assert_eq!(1, stage.high_watermark);
    let _ = connection
        .execute("drop trigger fail_offset", ())
        .await
        .unwrap();
    assert_eq!(
        ErrorCode::None,
        storage
            .txn_end("projection", producer.id, producer.epoch, true)
            .await
            .unwrap()
    );
    assert_eq!(
        Some(&1),
        storage
            .offset_fetch(Some("projection"), std::slice::from_ref(&input), Some(true))
            .await
            .unwrap()
            .get(&input)
    );
    let stage = storage.offset_stage(&output).await.unwrap();
    assert_eq!(2, stage.last_stable);
    assert_eq!(2, stage.high_watermark);
}

#[tokio::test]
async fn transactional_produce_requires_an_enrolled_partition() {
    let directory = tempfile::tempdir_in(".").unwrap();
    let storage = storage(&directory).await;
    let producer = storage
        .init_producer(Some("projection"), 30_000, None, None)
        .await
        .unwrap();
    assert_eq!(
        ErrorCode::None,
        storage
            .txn_add_offsets("projection", producer.id, producer.epoch, "projection")
            .await
            .unwrap()
    );
    let output = Topition::new("output", 0);
    assert!(
        storage
            .produce(Some("projection"), &output, batch(producer))
            .await
            .is_err()
    );
    assert_eq!(
        0,
        storage.offset_stage(&output).await.unwrap().high_watermark
    );
}

#[tokio::test]
async fn initialization_rejects_transaction_history_without_abort_tracking() {
    let directory = tempfile::tempdir().unwrap();
    let database = libsql::Builder::new_local(directory.path().join("old.db"))
        .build()
        .await
        .unwrap();
    let connection = database.connect().unwrap();
    let _ = connection
        .execute("create table txn_detail (status text)", ())
        .await
        .unwrap();
    let _ = connection
        .execute("insert into txn_detail values ('ABORTED')", ())
        .await
        .unwrap();
    assert!(
        initialize_history(&connection)
            .await
            .unwrap_err()
            .to_string()
            .contains("fresh database")
    );
}
