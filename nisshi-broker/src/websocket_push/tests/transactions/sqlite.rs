// Copyright ⓒ 2026 Nisshi contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use nisshi_storage::BrokerRegistrationRequest;

async fn reopen(cluster: &str, filename: &str) -> ArcDynStorage {
    let mut builder = StorageContainer::builder();
    builder.with_factory(Arc::new(nisshi_storage_sql::LiteEngineFactory));
    let storage = builder
        .cluster_id(cluster)
        .node_id(111)
        .advertised_listener("tcp://localhost:9092".parse::<Url>().unwrap())
        .storage(format!("sqlite:///{filename}").parse().unwrap())
        .build()
        .await
        .unwrap();
    storage
        .register_broker(BrokerRegistrationRequest {
            broker_id: 111,
            cluster_id: cluster.into(),
            incarnation_id: Uuid::now_v7(),
            rack: None,
        })
        .await
        .unwrap();
    assert!(storage.supports_transactional_consumption().await.unwrap());
    storage
}

async fn create_topics(storage: &impl Storage) {
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
}

fn filename(directory: &tempfile::TempDir) -> String {
    format!(
        "{}/broker.db",
        directory.path().file_name().unwrap().to_str().unwrap()
    )
}

#[tokio::test]
async fn sqlite_same_socket_commit_abort_and_lost_result_survive_reopening() {
    let directory = tempfile::tempdir_in(".").unwrap();
    let filename = filename(&directory);
    let storage = reopen("atomic", &filename).await;
    create_topics(&storage).await;
    produce(&storage, 1).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = configuration(transactional(format!(
        "ws://{}/",
        listener.local_addr().unwrap()
    )));
    let mut tasks = JoinSet::new();
    let cancellation = CancellationToken::new();
    config
        .spawn(&mut tasks, storage.clone(), cancellation.clone())
        .await
        .unwrap();
    let mut peer = accept(&listener).await;
    let delivery = receive(&mut peer).await;
    let producer = begin(&mut peer).await;
    output(&mut peer, &producer, "aborted").await;
    stage(&mut peer, &producer, &delivery).await;
    assert_eq!(0, committed(&storage).await);
    assert!(visible_output(&storage).await.is_null());
    assert_eq!(0, end(&mut peer, &producer, false).await["error_code"]);
    assert_eq!(0, committed(&storage).await);
    assert!(visible_output(&storage).await.is_null());

    let producer = begin(&mut peer).await;
    output(&mut peer, &producer, "committed").await;
    stage(&mut peer, &producer, &delivery).await;
    let mut args = producer;
    args["committed"] = json!(true);
    peer.send(Message::Text(
        json!({"version":1,"type":"request","request_id":"lost",
        "method":"end_txn","params":args})
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    wait_committed(&storage, 1).await;
    // The client loses the response after the database committed.
    drop(peer);
    let mut peer = accept(&listener).await;
    assert_eq!(
        "1",
        ok(&mut peer, "offset_fetch", json!({})).await["next_offset"]
    );
    assert!(
        timeout(Duration::from_millis(100), peer.next())
            .await
            .is_err()
    );
    cancellation.cancel();
    assert!(tasks.join_next().await.unwrap().is_ok());
    drop(peer);
    drop(storage);

    let storage = reopen("atomic", &filename).await;
    assert_eq!(1, committed(&storage).await);
    let visible = visible_output(&storage).await;
    assert_eq!(1, visible["records"].as_array().unwrap().len());
    assert_eq!(STANDARD.encode("committed"), visible["records"][0]["value"]);
}

#[tokio::test]
async fn sqlite_restart_before_commit_replays_input_and_fences_interrupted_producer() {
    let directory = tempfile::tempdir_in(".").unwrap();
    let filename = filename(&directory);
    let storage = reopen("interrupted", &filename).await;
    create_topics(&storage).await;
    produce(&storage, 1).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = configuration(transactional(format!(
        "ws://{}/",
        listener.local_addr().unwrap()
    )));
    let mut tasks = JoinSet::new();
    let cancellation = CancellationToken::new();
    config
        .spawn(&mut tasks, storage.clone(), cancellation.clone())
        .await
        .unwrap();
    let mut peer = accept(&listener).await;
    let delivery = receive(&mut peer).await;
    let interrupted = begin(&mut peer).await;
    output(&mut peer, &interrupted, "interrupted").await;
    stage(&mut peer, &interrupted, &delivery).await;
    assert_eq!(0, committed(&storage).await);
    cancellation.cancel();
    assert!(tasks.join_next().await.unwrap().is_ok());
    drop(peer);
    drop(storage);

    let storage = reopen("interrupted", &filename).await;
    let cancellation = CancellationToken::new();
    config
        .spawn(&mut tasks, storage.clone(), cancellation.clone())
        .await
        .unwrap();
    let mut peer = accept(&listener).await;
    let replay = receive(&mut peer).await;
    assert_eq!(delivery["records"], replay["records"]);
    let producer = begin(&mut peer).await;
    assert!(visible_output(&storage).await.is_null());
    output(&mut peer, &producer, "recovered").await;
    stage(&mut peer, &producer, &replay).await;
    assert_eq!(0, end(&mut peer, &producer, true).await["error_code"]);
    assert_eq!(1, committed(&storage).await);
    let old_id = interrupted["producer_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let old_epoch = interrupted["producer_epoch"].as_i64().unwrap() as i16;
    assert_eq!(
        ErrorCode::ProducerFenced,
        storage
            .txn_end("actor-transaction", old_id, old_epoch, true)
            .await
            .unwrap()
    );
    let visible = visible_output(&storage).await;
    assert_eq!(1, visible["records"].as_array().unwrap().len());
    assert_eq!(STANDARD.encode("recovered"), visible["records"][0]["value"]);
    cancellation.cancel();
    assert!(tasks.join_next().await.unwrap().is_ok());
}

#[tokio::test]
async fn sqlite_timeout_aborts_records_and_fences_old_producer() {
    let directory = tempfile::tempdir_in(".").unwrap();
    let storage = reopen("timeout", &filename(&directory)).await;
    create_topics(&storage).await;
    let producer = storage
        .init_producer(Some("filter-test"), 30_000, None, None)
        .await
        .unwrap();
    add(&storage, producer).await;
    transactional_batch(&storage, producer, 0, 1).await;
    assert!(
        subscription("ws://localhost/".into())
            .fetch(&storage, 0)
            .await
            .unwrap()
            .0
            .is_none()
    );
    storage
        .maintain_transactions(std::time::SystemTime::now() + Duration::from_secs(60))
        .await
        .unwrap();
    let stage = storage
        .offset_stage(&Topition::new("events", 0))
        .await
        .unwrap();
    assert_eq!(stage.high_watermark, stage.last_stable);
    let (delivery, cursor) = subscription("ws://localhost/".into())
        .fetch(&storage, 0)
        .await
        .unwrap();
    assert!(delivery.is_none());
    assert_eq!(stage.last_stable, cursor);
    assert_eq!(
        ErrorCode::ProducerFenced,
        storage
            .txn_end("filter-test", producer.id, producer.epoch, true)
            .await
            .unwrap()
    );
    let aborted = storage
        .aborted_transactions(&Topition::new("events", 0), 0, stage.last_stable)
        .await
        .unwrap();
    assert_eq!(1, aborted.len());
}

#[tokio::test]
async fn sqlite_abort_history_survives_reusing_the_same_producer_epoch() {
    let directory = tempfile::tempdir_in(".").unwrap();
    let storage = reopen("reuse", &filename(&directory)).await;
    create_topics(&storage).await;
    let producer = storage
        .init_producer(Some("filter-test"), 30_000, None, None)
        .await
        .unwrap();
    add(&storage, producer).await;
    transactional_batch(&storage, producer, 0, 105).await;
    assert_eq!(
        ErrorCode::None,
        storage
            .txn_end("filter-test", producer.id, producer.epoch, false)
            .await
            .unwrap()
    );
    let watermark = storage
        .offset_stage(&Topition::new("events", 0))
        .await
        .unwrap()
        .high_watermark;
    assert_eq!(
        ErrorCode::None,
        storage
            .txn_end("filter-test", producer.id, producer.epoch, false)
            .await
            .unwrap()
    );
    assert_eq!(
        ErrorCode::InvalidTxnState,
        storage
            .txn_end("filter-test", producer.id, producer.epoch, true)
            .await
            .unwrap()
    );
    assert_eq!(
        watermark,
        storage
            .offset_stage(&Topition::new("events", 0))
            .await
            .unwrap()
            .high_watermark
    );
    add(&storage, producer).await;
    transactional_batch(&storage, producer, 105, 105).await;
    assert_eq!(
        ErrorCode::None,
        storage
            .txn_end("filter-test", producer.id, producer.epoch, true)
            .await
            .unwrap()
    );
    let subscription = subscription("ws://localhost/".into());
    let (delivery, cursor) = subscription.fetch(&storage, 50).await.unwrap();
    let delivery: Value = serde_json::from_str(&delivery.unwrap().json).unwrap();
    assert_eq!(100, delivery["records"].as_array().unwrap().len());
    assert_eq!(watermark.to_string(), delivery["records"][0]["offset"]);
    let (delivery, _) = subscription.fetch(&storage, cursor).await.unwrap();
    let delivery: Value = serde_json::from_str(&delivery.unwrap().json).unwrap();
    assert_eq!(5, delivery["records"].as_array().unwrap().len());
}
