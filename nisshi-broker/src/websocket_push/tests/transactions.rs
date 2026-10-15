// Copyright ⓒ 2026 Nisshi contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use nisshi_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
use nisshi_storage::{ProducerIdResponse, TxnAddPartitionsRequest};

fn transactional(endpoint: String) -> Subscription {
    let mut subscription = subscription(endpoint);
    subscription.transactional_id = Some("actor-transaction".into());
    subscription.output_topics = ["output".into()].into();
    subscription
}

async fn with_output() -> ArcDynStorage {
    let storage = storage().await;
    let _ = storage
        .create_topic(
            CreatableTopic::default()
                .name("output".into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some(vec![]))
                .configs(Some(vec![])),
            false,
        )
        .await
        .unwrap();
    storage
}

async fn request(peer: &mut Peer, method: &str, params: Value) -> Value {
    let id = Uuid::now_v7().to_string();
    peer.send(Message::Text(
        json!({"version":1,"type":"request","request_id":id,"method":method,"params":params})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let response = receive(peer).await;
    assert_eq!("response", response["type"]);
    assert_eq!(id, response["request_id"]);
    response
}

async fn ok(peer: &mut Peer, method: &str, params: Value) -> Value {
    let response = request(peer, method, params).await;
    assert_eq!(0, response["error_code"], "{method}: {response}");
    response["result"].clone()
}

async fn begin(peer: &mut Peer) -> Value {
    let producer = ok(peer, "init_producer_id", json!({})).await;
    let mut args = producer.clone();
    args["topic"] = json!("output");
    args["partition"] = json!(0);
    let _ = ok(peer, "add_partitions_to_txn", args).await;
    producer
}

async fn output(peer: &mut Peer, producer: &Value, value: &str) {
    let mut args = producer.clone();
    args["topic"] = json!("output");
    args["partition"] = json!(0);
    args["base_sequence"] = json!(0);
    args["records"] = json!([{"timestamp":"9007199254740993", "key":"AP8=", "value":STANDARD.encode(value),
        "headers":[{"key":"aw==","value":null},{"key":"aw==","value":""}]}]);
    let _ = ok(peer, "produce", args).await;
}

async fn stage(peer: &mut Peer, producer: &Value, delivery: &Value) {
    let _ = ok(peer, "add_offsets_to_txn", producer.clone()).await;
    let mut args = producer.clone();
    args["delivery_id"] = delivery["delivery_id"].clone();
    args["next_offset"] = delivery["next_offset"].clone();
    let _ = ok(peer, "txn_offset_commit", args).await;
}

async fn end(peer: &mut Peer, producer: &Value, committed: bool) -> Value {
    let mut args = producer.clone();
    args["committed"] = json!(committed);
    request(peer, "end_txn", args).await
}

async fn visible_output(storage: &impl Storage) -> Value {
    let mut subscription = subscription("ws://localhost/".into());
    subscription.topic = "output".into();
    subscription
        .fetch(storage, 0)
        .await
        .unwrap()
        .0
        .map_or(Value::Null, |delivery| {
            serde_json::from_str(&delivery.json).unwrap()
        })
}

#[tokio::test]
async fn same_socket_transactions_commit_output_and_offset_together_and_abort_is_invisible() {
    let storage = with_output().await;
    produce(&storage, 1).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = configuration(transactional(format!(
        "ws://{}/",
        listener.local_addr().unwrap()
    )));
    let cancellation = CancellationToken::new();
    let mut tasks = JoinSet::new();
    config
        .spawn(&mut tasks, storage.clone(), cancellation.clone())
        .await
        .unwrap();
    let mut peer = accept(&listener).await;
    let delivery = receive(&mut peer).await;
    assert_eq!(
        "0",
        ok(&mut peer, "offset_fetch", json!({})).await["next_offset"]
    );
    let producer = begin(&mut peer).await;
    output(&mut peer, &producer, "aborted").await;
    stage(&mut peer, &producer, &delivery).await;
    assert_eq!(0, committed(&storage).await);
    assert!(visible_output(&storage).await.is_null());
    assert_eq!(0, end(&mut peer, &producer, false).await["error_code"]);
    assert_eq!(0, committed(&storage).await);
    assert!(visible_output(&storage).await.is_null());

    let producer = begin(&mut peer).await;
    let mut forbidden = producer.clone();
    forbidden["topic"] = json!("events");
    forbidden["partition"] = json!(0);
    assert_eq!(
        29,
        request(&mut peer, "add_partitions_to_txn", forbidden).await["error_code"]
    );
    let mut wrong = producer.clone();
    wrong["delivery_id"] = delivery["delivery_id"].clone();
    wrong["next_offset"] = json!("2");
    assert_eq!(
        42,
        request(&mut peer, "txn_offset_commit", wrong).await["error_code"]
    );
    output(&mut peer, &producer, "committed").await;
    stage(&mut peer, &producer, &delivery).await;
    assert_eq!(0, committed(&storage).await);
    assert!(visible_output(&storage).await.is_null());
    assert_eq!(0, end(&mut peer, &producer, true).await["error_code"]);
    wait_committed(&storage, 1).await;
    let visible = visible_output(&storage).await;
    assert_eq!(1, visible["records"].as_array().unwrap().len());
    assert_eq!(STANDARD.encode("committed"), visible["records"][0]["value"]);
    assert_eq!("9007199254740993", visible["records"][0]["timestamp"]);
    assert_eq!("AP8=", visible["records"][0]["key"]);
    assert_eq!(
        json!([{"key":"aw==","value":null},{"key":"aw==","value":""}]),
        visible["records"][0]["headers"]
    );
    // RPC remains available after the delivery has completed, without another socket.
    assert_eq!(
        "1",
        ok(&mut peer, "offset_fetch", json!({})).await["next_offset"]
    );
    cancellation.cancel();
    assert!(tasks.join_next().await.unwrap().is_ok());
}

async fn add(storage: &impl Storage, producer: ProducerIdResponse) {
    let result = storage
        .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: "filter-test".into(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            topics: vec![
                AddPartitionsToTxnTopic::default()
                    .name("events".into())
                    .partitions(Some(vec![0])),
            ],
        })
        .await
        .unwrap();
    assert_eq!(
        0,
        result.zero_to_three()[0]
            .results_by_partition
            .as_ref()
            .unwrap()[0]
            .partition_error_code
    );
}

async fn transactional_batch(
    storage: &impl Storage,
    producer: ProducerIdResponse,
    sequence: i32,
    count: i32,
) {
    let mut batch = inflated::Batch::builder()
        .last_offset_delta(count - 1)
        .producer_id(producer.id)
        .producer_epoch(producer.epoch)
        .base_sequence(sequence)
        .attributes(BatchAttribute::default().transaction(true).into());
    for offset in 0..count {
        batch = batch.record(
            KafkaRecord::builder()
                .offset_delta(offset)
                .value(Some(Bytes::from(format!("txn-{sequence}-{offset}")))),
        );
    }
    let _ = storage
        .produce(
            Some("filter-test"),
            &Topition::new("events", 0),
            batch.build().unwrap().try_into().unwrap(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn committed_filter_handles_open_aborted_partial_batches_and_reused_producer() {
    let storage = storage().await;
    let producer = storage
        .init_producer(Some("filter-test"), 30_000, Some(-1), Some(-1))
        .await
        .unwrap();
    add(&storage, producer).await;
    transactional_batch(&storage, producer, 0, 105).await;
    let subscription = subscription("ws://localhost/".into());
    assert!(subscription.fetch(&storage, 0).await.unwrap().0.is_none());
    assert_eq!(
        ErrorCode::None,
        storage
            .txn_end("filter-test", producer.id, producer.epoch, false)
            .await
            .unwrap()
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
    add(&storage, producer).await;
    transactional_batch(&storage, producer, 210, 1).await;

    // Resume in the middle of the aborted batch. Its marker must clear the
    // producer before the next committed transaction, and partial delivery
    // must not expose the open transaction behind it.
    let (delivery, cursor) = subscription.fetch(&storage, 50).await.unwrap();
    let delivery: Value = serde_json::from_str(&delivery.unwrap().json).unwrap();
    assert_eq!(100, delivery["records"].as_array().unwrap().len());
    assert_eq!("106", delivery["records"][0]["offset"]);
    assert_eq!(206, cursor);
    let (delivery, cursor) = subscription.fetch(&storage, cursor).await.unwrap();
    let delivery: Value = serde_json::from_str(&delivery.unwrap().json).unwrap();
    assert_eq!(5, delivery["records"].as_array().unwrap().len());
    assert_eq!("206", delivery["records"][0]["offset"]);
    assert_eq!(212, cursor);
    assert!(
        subscription
            .fetch(&storage, cursor)
            .await
            .unwrap()
            .0
            .is_none()
    );
}

#[tokio::test]
async fn transactional_mode_rejects_plain_ack_and_fences_disconnected_producer() {
    let storage = with_output().await;
    produce(&storage, 1).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = configuration(transactional(format!(
        "ws://{}/",
        listener.local_addr().unwrap()
    )));
    let cancellation = CancellationToken::new();
    let mut tasks = JoinSet::new();
    config
        .spawn(&mut tasks, storage.clone(), cancellation.clone())
        .await
        .unwrap();
    let mut peer = accept(&listener).await;
    let delivery = receive(&mut peer).await;
    let producer = begin(&mut peer).await;
    output(&mut peer, &producer, "interrupted").await;
    stage(&mut peer, &producer, &delivery).await;
    ack(&mut peer, &delivery).await;
    let mut peer = accept(&listener).await;
    assert_eq!(delivery, receive(&mut peer).await);
    assert_eq!(0, committed(&storage).await);
    let producer = begin(&mut peer).await;
    output(&mut peer, &producer, "after-reconnect").await;
    stage(&mut peer, &producer, &delivery).await;
    assert_eq!(0, end(&mut peer, &producer, true).await["error_code"]);
    let visible = visible_output(&storage).await;
    assert_eq!(1, visible["records"].as_array().unwrap().len());
    assert_eq!(
        STANDARD.encode("after-reconnect"),
        visible["records"][0]["value"]
    );
    cancellation.cancel();
    assert!(tasks.join_next().await.unwrap().is_ok());
}

#[tokio::test]
async fn lost_commit_result_reconciles_offset_before_replaying_output() {
    uncertain_commit(false).await;
}

#[tokio::test]
async fn initialization_completing_an_uncertain_commit_reconciles_before_accepting_more_output() {
    uncertain_commit(true).await;
}

async fn uncertain_commit(recover_on_init: bool) {
    use nisshi_storage::{Request, RequestChannelService, RequestStorageService, Response};
    use rama::Service as _;
    let storage = with_output().await;
    produce(&storage, 1).await;
    let (sender, mut receiver) =
        tokio::sync::mpsc::channel::<(Request, tokio::sync::oneshot::Sender<Response>)>(8);
    let service = RequestStorageService::new(storage.clone());
    let storage_task = tokio::spawn(async move {
        let mut hide_commit = true;
        let mut delayed_commit = None;
        while let Some((request, response)) = receiver.recv().await {
            // Model a prepared decision finishing inside InitProducerId. The
            // backend fault tests exercise the actual interrupted metadata CAS;
            // this test isolates the WebSocket reconciliation boundary.
            if matches!(&request, Request::InitProducer { .. })
                && let Some(commit) = delayed_commit.take()
            {
                assert!(matches!(
                    service.serve(commit).await.unwrap(),
                    Response::TxnEnd(Ok(ErrorCode::None))
                ));
            }
            let hide = hide_commit
                && matches!(
                    &request,
                    Request::TxnEnd {
                        committed: true,
                        ..
                    }
                );
            if hide {
                hide_commit = false;
                if recover_on_init {
                    delayed_commit = Some(request);
                } else {
                    assert!(matches!(
                        service.serve(request).await.unwrap(),
                        Response::TxnEnd(Ok(ErrorCode::None))
                    ));
                }
                response
                    .send(Response::TxnEnd(Ok(ErrorCode::UnknownServerError)))
                    .unwrap();
            } else {
                response
                    .send(service.serve(request).await.unwrap())
                    .unwrap();
            }
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = configuration(transactional(format!(
        "ws://{}/",
        listener.local_addr().unwrap()
    )));
    let cancellation = CancellationToken::new();
    let mut tasks = JoinSet::new();
    config
        .spawn(
            &mut tasks,
            RequestChannelService::new(sender),
            cancellation.clone(),
        )
        .await
        .unwrap();
    let mut peer = accept(&listener).await;
    let delivery = receive(&mut peer).await;
    let producer = begin(&mut peer).await;
    output(&mut peer, &producer, "once").await;
    stage(&mut peer, &producer, &delivery).await;
    assert_eq!(-1, end(&mut peer, &producer, true).await["error_code"]);
    let mut peer = accept(&listener).await;
    if recover_on_init {
        assert_eq!(delivery, receive(&mut peer).await);
        assert_eq!(0, committed(&storage).await);
        assert!(visible_output(&storage).await.is_null());
        let _ = ok(&mut peer, "init_producer_id", json!({})).await;
        // Initialization published the previous decision. The broker closes
        // this incarnation before accepting any output for the stale delivery.
        peer = accept(&listener).await;
    }
    assert_eq!(
        "1",
        ok(&mut peer, "offset_fetch", json!({})).await["next_offset"]
    );
    assert!(
        timeout(Duration::from_millis(100), peer.next())
            .await
            .is_err()
    );
    produce(&storage, 1).await;
    let next = receive(&mut peer).await;
    assert_eq!("1", next["records"][0]["offset"]);
    assert_eq!("2", next["next_offset"]);
    let visible = visible_output(&storage).await;
    assert_eq!(1, visible["records"].as_array().unwrap().len());
    assert_eq!(STANDARD.encode("once"), visible["records"][0]["value"]);
    cancellation.cancel();
    assert!(tasks.join_next().await.unwrap().is_ok());
    storage_task.await.unwrap();
}

#[tokio::test]
async fn retention_between_batch_and_abort_metadata_reads_cannot_release_records() {
    use nisshi_sans_io::delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic};
    use nisshi_storage::{Request, RequestChannelService, RequestStorageService, Response};
    use rama::Service as _;
    let storage = storage().await;
    produce(&storage, 2).await;
    let (sender, mut receiver) =
        tokio::sync::mpsc::channel::<(Request, tokio::sync::oneshot::Sender<Response>)>(8);
    let service = RequestStorageService::new(storage.clone());
    let task = tokio::spawn(async move {
        while let Some((request, response)) = receiver.recv().await {
            if matches!(&request, Request::AbortedTransactions { .. }) {
                let _ = storage
                    .delete_records(&[DeleteRecordsTopic::default()
                        .name("events".into())
                        .partitions(Some(vec![
                            DeleteRecordsPartition::default()
                                .partition_index(0)
                                .offset(1),
                        ]))])
                    .await
                    .unwrap();
            }
            response
                .send(service.serve(request).await.unwrap())
                .unwrap();
        }
    });
    let storage = RequestChannelService::new(sender);
    let result = subscription("ws://localhost/".into())
        .fetch(&storage, 0)
        .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("retention advanced")
    );
    drop(storage);
    task.await.unwrap();
}
