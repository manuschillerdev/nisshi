// Copyright ⓒ 2026 Nisshi contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use bytes::Bytes;
use nisshi_sans_io::{
    create_topics_request::CreatableTopic,
    record::{Record as KafkaRecord, header},
};
use nisshi_storage::{ArcDynStorage, StorageContainer};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;

type Peer = WebSocketStream<TcpStream>;

fn subscription(endpoint: String) -> Subscription {
    Subscription {
        group_id: "actor-consumer".into(),
        topic: "events".into(),
        partition: 0,
        endpoint: endpoint.parse().unwrap(),
        bearer_token_env: None,
        authorization: None,
    }
}

async fn storage() -> ArcDynStorage {
    let mut builder = StorageContainer::builder();
    builder.with_factory(Arc::new(nisshi_storage_dynostore::MemoryEngineFactory));
    let storage = builder
        .cluster_id(Uuid::now_v7().to_string())
        .node_id(111)
        .advertised_listener("tcp://localhost:9092".parse::<Url>().unwrap())
        .storage("memory://".parse().unwrap())
        .build()
        .await
        .unwrap();
    let _ = storage
        .create_topic(
            CreatableTopic::default()
                .name("events".into())
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

async fn produce(storage: &impl Storage, count: i32) {
    let mut batch = inflated::Batch::builder()
        .base_timestamp(1000)
        .last_offset_delta(count - 1);
    for offset in 0..count {
        batch = batch.record(
            KafkaRecord::builder()
                .offset_delta(offset)
                .timestamp_delta(i64::from(offset))
                .value(Some(Bytes::from(format!("record-{offset}"))))
                .header(
                    header::Builder::default()
                        .key(Bytes::from_static(b"duplicate"))
                        .value(Bytes::from_static(&[0, 255])),
                )
                .header(header::Builder::default().key(Bytes::from_static(b"duplicate"))),
        );
    }
    let _ = storage
        .produce(
            None,
            &Topition::new("events", 0),
            batch.build().unwrap().try_into().unwrap(),
        )
        .await
        .unwrap();
}

fn configuration(subscription: Subscription) -> Config {
    let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(rustls::RootCertStore::empty())
    .with_no_client_auth();
    Config {
        subscriptions: vec![subscription],
        tls: Arc::new(tls),
    }
}

async fn accept(listener: &TcpListener) -> Peer {
    timeout(Duration::from_secs(5), async {
        accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap()
    })
    .await
    .unwrap()
}

async fn receive(socket: &mut Peer) -> Value {
    let message = timeout(Duration::from_secs(5), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}

async fn ack(socket: &mut Peer, delivery: &Value) {
    let ack = json!({"version":1,"type":"ack","delivery_id":delivery["delivery_id"],"group_id":delivery["group_id"],"topic":delivery["topic"],"partition":delivery["partition"],"next_offset":delivery["next_offset"]});
    socket
        .send(Message::Text(ack.to_string().into()))
        .await
        .unwrap();
}

async fn committed(storage: &impl Storage) -> i64 {
    storage
        .offset_fetch(
            Some("actor-consumer"),
            &[Topition::new("events", 0)],
            Some(false),
        )
        .await
        .unwrap()
        .get(&Topition::new("events", 0))
        .copied()
        .unwrap_or(-1)
}

async fn wait_committed(storage: &impl Storage, expected: i64) {
    timeout(Duration::from_secs(5), async {
        while committed(storage).await != expected {
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn exact_ack_commits_and_restart_resumes_after_processed_records() {
    let storage = storage().await;
    produce(&storage, 2).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = configuration(subscription(format!(
        "ws://{}/consumer",
        listener.local_addr().unwrap()
    )));
    let mut tasks = JoinSet::new();
    let cancellation = CancellationToken::new();
    config.spawn(&mut tasks, storage.clone(), cancellation.clone());
    let mut peer = accept(&listener).await;
    let delivery = receive(&mut peer).await;
    assert_eq!("2", delivery["next_offset"]);
    assert_eq!(0, committed(&storage).await);
    ack(&mut peer, &delivery).await;
    wait_committed(&storage, 2).await;
    cancellation.cancel();
    assert!(
        timeout(Duration::from_secs(1), tasks.join_next())
            .await
            .unwrap()
            .unwrap()
            .is_ok()
    );

    produce(&storage, 1).await;
    let cancellation = CancellationToken::new();
    config.spawn(&mut tasks, storage.clone(), cancellation.clone());
    let mut peer = accept(&listener).await;
    let delivery = receive(&mut peer).await;
    assert_eq!("2", delivery["records"][0]["offset"]);
    assert_eq!("3", delivery["next_offset"]);
    ack(&mut peer, &delivery).await;
    wait_committed(&storage, 3).await;
    cancellation.cancel();
    assert!(tasks.join_next().await.unwrap().is_ok());
}

#[tokio::test]
async fn wrong_ack_and_disconnect_replay_the_same_pending_delivery() {
    let storage = storage().await;
    produce(&storage, 1).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = configuration(subscription(format!(
        "ws://{}/",
        listener.local_addr().unwrap()
    )));
    let mut tasks = JoinSet::new();
    let cancellation = CancellationToken::new();
    config.spawn(&mut tasks, storage.clone(), cancellation.clone());
    let mut peer = accept(&listener).await;
    let delivery = receive(&mut peer).await;
    let mut incorrect = delivery.clone();
    incorrect["next_offset"] = json!("999");
    ack(&mut peer, &incorrect).await;
    let mut peer = accept(&listener).await;
    assert_eq!(delivery, receive(&mut peer).await);
    assert_eq!(0, committed(&storage).await);
    drop(peer);
    let mut peer = accept(&listener).await;
    assert_eq!(delivery, receive(&mut peer).await);
    assert_eq!(0, committed(&storage).await);
    ack(&mut peer, &delivery).await;
    wait_committed(&storage, 1).await;
    cancellation.cancel();
    assert!(tasks.join_next().await.unwrap().is_ok());
}

#[tokio::test]
async fn empty_topics_send_no_data_frames_and_cancel_without_waiting() {
    let storage = storage().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = configuration(subscription(format!(
        "ws://{}/",
        listener.local_addr().unwrap()
    )));
    let mut tasks = JoinSet::new();
    let cancellation = CancellationToken::new();
    config.spawn(&mut tasks, storage.clone(), cancellation.clone());
    let mut peer = accept(&listener).await;
    assert!(
        timeout(Duration::from_millis(700), peer.next())
            .await
            .is_err()
    );
    produce(&storage, 1).await;
    let delivery = receive(&mut peer).await;
    assert_eq!("0", delivery["records"][0]["offset"]);
    ack(&mut peer, &delivery).await;
    wait_committed(&storage, 1).await;
    cancellation.cancel();
    assert!(
        timeout(Duration::from_secs(1), tasks.join_next())
            .await
            .unwrap()
            .unwrap()
            .is_ok()
    );
}

#[tokio::test]
async fn overlapping_kafka_batches_are_split_without_repeating_offsets() {
    let storage = storage().await;
    produce(&storage, 105).await;
    let subscription = subscription("ws://localhost/".into());
    let (delivery, cursor) = subscription.fetch(&storage, 0).await.unwrap();
    let delivery: Value = serde_json::from_str(&delivery.unwrap().json).unwrap();
    assert_eq!(100, cursor);
    assert_eq!(100, delivery["records"].as_array().unwrap().len());
    let record = &delivery["records"][0];
    assert_eq!(Value::Null, record["key"]);
    assert_eq!("cmVjb3JkLTA=", record["value"]);
    assert_eq!("1000", record["timestamp"]);
    assert_eq!(
        json!([{"key":"ZHVwbGljYXRl","value":"AP8="},{"key":"ZHVwbGljYXRl","value":null}]),
        record["headers"]
    );
    let (delivery, cursor) = subscription.fetch(&storage, cursor).await.unwrap();
    let delivery: Value = serde_json::from_str(&delivery.unwrap().json).unwrap();
    assert_eq!(105, cursor);
    assert_eq!(5, delivery["records"].as_array().unwrap().len());
    assert_eq!("100", delivery["records"][0]["offset"]);
}

#[tokio::test]
async fn oversized_first_kafka_batch_is_split_but_oversized_record_is_rejected() {
    let storage = storage().await;
    let mut batch = inflated::Batch::builder().last_offset_delta(1);
    for offset in 0..2 {
        batch = batch.record(
            KafkaRecord::builder()
                .offset_delta(offset)
                .value(Some(Bytes::from(vec![b'x'; 600_000]))),
        );
    }
    let _ = storage
        .produce(
            None,
            &Topition::new("events", 0),
            batch.build().unwrap().try_into().unwrap(),
        )
        .await
        .unwrap();
    let subscription = subscription("ws://localhost/".into());
    let (delivery, cursor) = subscription.fetch(&storage, 0).await.unwrap();
    assert_eq!(1, cursor);
    assert!(delivery.unwrap().json.len() <= MAX_PAYLOAD_BYTES);
    let (delivery, cursor) = subscription.fetch(&storage, cursor).await.unwrap();
    assert_eq!(2, cursor);
    assert!(delivery.unwrap().json.len() <= MAX_PAYLOAD_BYTES);
    let batch = inflated::Batch::builder()
        .record(KafkaRecord::builder().value(Some(Bytes::from(vec![b'x'; MAX_PAYLOAD_BYTES]))))
        .build()
        .unwrap();
    let _ = storage
        .produce(None, &Topition::new("events", 0), batch.try_into().unwrap())
        .await
        .unwrap();
    assert!(
        subscription
            .fetch(&storage, cursor)
            .await
            .unwrap_err()
            .to_string()
            .contains("record exceeds")
    );
}

#[tokio::test]
async fn control_batches_advance_cursor_without_delivering_records() {
    let storage = storage().await;
    let batch = inflated::Batch::builder()
        .attributes(BatchAttribute::default().control(true).into())
        .record(
            KafkaRecord::builder()
                .key(Some(Bytes::from_static(&[0, 0, 0, 1])))
                .value(Some(Bytes::from_static(&[0, 0, 0, 0, 0, 0]))),
        )
        .build()
        .unwrap();
    let _ = storage
        .produce(None, &Topition::new("events", 0), batch.try_into().unwrap())
        .await
        .unwrap();
    let (delivery, cursor) = subscription("ws://localhost/".into())
        .fetch(&storage, 0)
        .await
        .unwrap();
    assert!(delivery.is_none());
    assert_eq!(1, cursor);
}

#[tokio::test]
async fn retained_offset_gaps_fail_without_silently_skipping_records() {
    use nisshi_sans_io::delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic};
    let storage = storage().await;
    produce(&storage, 2).await;
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
    assert!(
        subscription("ws://localhost/".into())
            .fetch(&storage, 0)
            .await
            .unwrap_err()
            .to_string()
            .contains("outside retained log")
    );
    assert_eq!(-1, committed(&storage).await);
}

#[test]
fn configuration_rejects_invalid_endpoints_duplicates_unknown_fields_and_missing_secrets() {
    let valid = json!({"group_id":"actor-consumer","topic":"events","partition":0,"endpoint":"ws://localhost/"});
    let mut cases = vec![
        json!({"subscriptions":[]}),
        json!({"subscriptions":[valid.clone(),valid.clone()]}),
    ];
    for endpoint in [
        "https://localhost/",
        "ws://user:secret@localhost/",
        "ws://localhost/?secret=abc",
        "ws://localhost/#fragment",
    ] {
        let mut subscription = valid.clone();
        subscription["endpoint"] = json!(endpoint);
        cases.push(json!({"subscriptions":[subscription]}));
    }
    for (field, value) in [
        ("group_id", json!("")),
        ("partition", json!(-1)),
        (
            "bearer_token_env",
            json!("NISSHI_MISSING_WS_TEST_SECRET_8BB6"),
        ),
        ("typo", json!(true)),
    ] {
        let mut subscription = valid.clone();
        subscription[field] = value;
        cases.push(json!({"subscriptions":[subscription]}));
    }
    for config in cases {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), config.to_string()).unwrap();
        assert!(Config::load(file.path()).is_err(), "accepted {config}");
    }
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), json!({"subscriptions":[valid]}).to_string()).unwrap();
    assert!(Config::load(file.path()).is_ok());
}

#[tokio::test]
#[allow(
    clippy::result_large_err,
    reason = "tungstenite fixes the handshake callback error response type"
)]
async fn wss_uses_verified_tls_and_bearer_authentication() {
    let storage = storage().await;
    produce(&storage, 1).await;
    let certificate = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let server_config = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der())
                .into(),
        )
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate.cert.der().clone()).unwrap();
    let client_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut subscription = subscription(format!("wss://{}/", listener.local_addr().unwrap()));
    subscription.authorization = Some(HeaderValue::from_static("Bearer test-token"));
    let config = Config {
        subscriptions: vec![subscription],
        tls: Arc::new(client_config),
    };
    let mut tasks = JoinSet::new();
    let cancellation = CancellationToken::new();
    config.spawn(&mut tasks, storage.clone(), cancellation.clone());
    let stream = timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap()
        .0;
    let tls = tokio_rustls::TlsAcceptor::from(Arc::new(server_config))
        .accept(stream)
        .await
        .unwrap();
    let mut peer = tokio_tungstenite::accept_hdr_async(
        tls,
        |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
            assert_eq!("Bearer test-token", request.headers()[AUTHORIZATION]);
            Ok(response)
        },
    )
    .await
    .unwrap();
    let message = timeout(Duration::from_secs(5), peer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let mut delivery: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    delivery["type"] = json!("ack");
    let _ = delivery.as_object_mut().unwrap().remove("records");
    peer.send(Message::Text(delivery.to_string().into()))
        .await
        .unwrap();
    wait_committed(&storage, 1).await;
    cancellation.cancel();
    assert!(tasks.join_next().await.unwrap().is_ok());
}

#[tokio::test]
async fn partition_commit_errors_replay_acknowledged_records() {
    use nisshi_storage::{Request, RequestChannelService, RequestStorageService, Response};
    use rama::Service as _;
    let storage = storage().await;
    produce(&storage, 1).await;
    let (sender, mut receiver) =
        tokio::sync::mpsc::channel::<(Request, tokio::sync::oneshot::Sender<Response>)>(8);
    let service = RequestStorageService::new(storage.clone());
    let storage_task = tokio::spawn(async move {
        let mut fail_commit = true;
        while let Some((request, response)) = receiver.recv().await {
            if let Request::OffsetCommit { ref offsets, .. } = request
                && offsets[0].1.offset > 0
                && fail_commit
            {
                fail_commit = false;
                response
                    .send(Response::OffsetCommit(Ok(vec![(
                        offsets[0].0.clone(),
                        ErrorCode::UnknownServerError,
                    )])))
                    .unwrap();
            } else {
                response
                    .send(service.serve(request).await.unwrap())
                    .unwrap();
            }
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = configuration(subscription(format!(
        "ws://{}/",
        listener.local_addr().unwrap()
    )));
    let mut tasks = JoinSet::new();
    let cancellation = CancellationToken::new();
    config.spawn(
        &mut tasks,
        RequestChannelService::new(sender),
        cancellation.clone(),
    );
    let mut peer = accept(&listener).await;
    let delivery = receive(&mut peer).await;
    ack(&mut peer, &delivery).await;
    let mut peer = accept(&listener).await;
    assert_eq!(delivery, receive(&mut peer).await);
    assert_eq!(0, committed(&storage).await);
    ack(&mut peer, &delivery).await;
    wait_committed(&storage, 1).await;
    cancellation.cancel();
    assert!(tasks.join_next().await.unwrap().is_ok());
    storage_task.await.unwrap();
}

#[tokio::test]
async fn cancellation_during_ack_wait_leaves_progress_uncommitted() {
    let storage = storage().await;
    produce(&storage, 1).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = configuration(subscription(format!(
        "ws://{}/",
        listener.local_addr().unwrap()
    )));
    let mut tasks = JoinSet::new();
    let cancellation = CancellationToken::new();
    config.spawn(&mut tasks, storage.clone(), cancellation.clone());
    let mut peer = accept(&listener).await;
    let _ = receive(&mut peer).await;
    cancellation.cancel();
    assert!(
        timeout(Duration::from_secs(1), tasks.join_next())
            .await
            .unwrap()
            .unwrap()
            .is_ok()
    );
    assert_eq!(0, committed(&storage).await);
}
