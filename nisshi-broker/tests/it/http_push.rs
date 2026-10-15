// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![cfg(feature = "dynostore")]

use crate::common::memory_storage;
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::{
    Request, Response, StatusCode, body::Incoming, server::conn::http1, service::service_fn,
};
use hyper_util::rt::TokioIo;
use nisshi_broker::http_push::HttpPush;
use nisshi_sans_io::{
    create_topics_request::CreatableTopic,
    record::{Header, Record, inflated},
};
use nisshi_storage::{ArcDynStorage, OffsetCommitRequest, Storage, Topition};
use serde_json::{Value, json};
use std::{convert::Infallible, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::{JoinHandle, JoinSet},
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;

struct Received {
    path: String,
    body: Value,
    response: oneshot::Sender<StatusCode>,
}

struct Endpoint {
    url: String,
    requests: mpsc::UnboundedReceiver<Received>,
    task: JoinHandle<()>,
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Endpoint {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/consume", listener.local_addr().unwrap());
        let (sender, requests) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    stream = listener.accept() => {
                        let (stream, _) = stream.unwrap();
                        let sender = sender.clone();
                        _ = connections.spawn(async move {
                            let service = service_fn(move |request: Request<Incoming>| {
                                let sender = sender.clone();
                                async move {
                                    assert_eq!(request.method(), "POST");
                                    assert_eq!(request.headers()["content-type"], "application/json");
                                    let path = request.uri().path().to_owned();
                                    let body = request.into_body().collect().await.unwrap().to_bytes();
                                    let (response, receive) = oneshot::channel();
                                    sender.send(Received { path, body: serde_json::from_slice(&body).unwrap(), response }).unwrap();
                                    let status = receive.await.unwrap_or(StatusCode::SERVICE_UNAVAILABLE);
                                    Ok::<_, Infallible>(Response::builder().status(status)
                                        .header("location", "/redirected")
                                        .body(Full::new(Bytes::new())).unwrap())
                                }
                            });
                            _ = http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Self {
            url,
            requests,
            task,
        }
    }

    async fn receive(&mut self) -> Received {
        let request = timeout(Duration::from_secs(10), self.requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.path, "/consume");
        request
    }

    fn configuration(&self, max_records: usize) -> String {
        json!({
            "poll_interval_ms": 10,
            "request_timeout_ms": 1000,
            "max_records": max_records,
            "subscriptions": [{ "group_id": "push-test", "topic": "events", "partition": 0, "endpoint": self.url }]
        }).to_string()
    }

    fn start(
        &self,
        storage: ArcDynStorage,
        max_records: usize,
    ) -> (CancellationToken, JoinHandle<()>) {
        let push = HttpPush::from_json(&self.configuration(max_records)).unwrap();
        let cancellation = CancellationToken::new();
        let token = cancellation.clone();
        let task = tokio::spawn(async move { push.run(storage, token).await.unwrap() });
        (cancellation, task)
    }
}

async fn storage() -> ArcDynStorage {
    let storage = memory_storage(uuid::Uuid::new_v4().to_string(), 111)
        .await
        .unwrap();
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name("events".into())
                .num_partitions(1)
                .replication_factor(1),
            false,
        )
        .await
        .unwrap();
    storage
}

async fn produce(storage: &ArcDynStorage, count: i32) {
    let mut batch = inflated::Batch::builder()
        .base_timestamp(9_007_199_254_740_993)
        .max_timestamp(9_007_199_254_740_993)
        .last_offset_delta(count - 1);
    for offset in 0..count {
        batch = batch.record(
            Record::builder()
                .offset_delta(offset)
                .value(Some(Bytes::from_static(&[0, 255])))
                .header(Header::builder().key(Bytes::from_static(b"duplicate")))
                .header(
                    Header::builder()
                        .key(Bytes::from_static(b"duplicate"))
                        .value(Bytes::new()),
                ),
        );
    }
    _ = storage
        .produce(
            None,
            &Topition::new("events", 0),
            batch.build().unwrap().try_into().unwrap(),
        )
        .await
        .unwrap();
}

async fn committed(storage: &ArcDynStorage) -> i64 {
    storage
        .offset_fetch(
            Some("push-test"),
            &[Topition::new("events", 0)],
            Some(false),
        )
        .await
        .unwrap()
        .get(&Topition::new("events", 0))
        .copied()
        .unwrap_or(-1)
}

async fn wait_committed(storage: &ArcDynStorage, offset: i64) {
    timeout(Duration::from_secs(5), async {
        while committed(storage).await != offset {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

async fn stop(cancellation: CancellationToken, task: JoinHandle<()>) {
    cancellation.cancel();
    timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn acknowledges_whole_batch_before_committing_and_preserves_binary_records() {
    let storage = storage().await;
    produce(&storage, 2).await;
    let mut endpoint = Endpoint::new().await;
    let (cancellation, task) = endpoint.start(storage.clone(), 100);
    let request = endpoint.receive().await;
    assert_eq!(committed(&storage).await, 0);
    assert_eq!(request.body["next_offset"], "2");
    assert_eq!(request.body["version"], 1);
    assert_eq!(
        request.body["records"][0],
        json!({
            "offset": "0", "timestamp": "9007199254740993", "key": null, "value": "AP8=",
            "headers": [{"key":"ZHVwbGljYXRl", "value":null}, {"key":"ZHVwbGljYXRl", "value":""}]
        })
    );
    request.response.send(StatusCode::NO_CONTENT).unwrap();
    wait_committed(&storage, 2).await;
    assert!(
        timeout(Duration::from_millis(100), endpoint.requests.recv())
            .await
            .is_err()
    );
    stop(cancellation, task).await;
}

#[tokio::test]
async fn failures_and_redirects_retry_identical_payload_without_committing() {
    let storage = storage().await;
    produce(&storage, 1).await;
    let mut endpoint = Endpoint::new().await;
    let (cancellation, task) = endpoint.start(storage.clone(), 100);
    let first = endpoint.receive().await;
    first
        .response
        .send(StatusCode::SERVICE_UNAVAILABLE)
        .unwrap();
    let retry = endpoint.receive().await;
    assert_eq!(first.body, retry.body);
    assert_eq!(committed(&storage).await, 0);
    retry.response.send(StatusCode::TEMPORARY_REDIRECT).unwrap();
    let retry = endpoint.receive().await;
    assert_eq!(first.body, retry.body);
    assert_eq!(committed(&storage).await, 0);
    retry.response.send(StatusCode::OK).unwrap();
    wait_committed(&storage, 1).await;
    stop(cancellation, task).await;
}

#[tokio::test]
async fn restart_replays_unacknowledged_records_and_resumes_inside_a_kafka_batch() {
    let storage = storage().await;
    produce(&storage, 3).await;
    let mut endpoint = Endpoint::new().await;
    let (cancellation, task) = endpoint.start(storage.clone(), 1);
    let first = endpoint.receive().await;
    assert_eq!(first.body["records"].as_array().unwrap().len(), 1);
    assert_eq!(first.body["next_offset"], "1");
    first.response.send(StatusCode::OK).unwrap();
    wait_committed(&storage, 1).await;
    let pending = endpoint.receive().await;
    assert_eq!(pending.body["records"][0]["offset"], "1");
    stop(cancellation, task).await;
    drop(pending.response);
    let (cancellation, task) = endpoint.start(storage.clone(), 1);
    let replay = endpoint.receive().await;
    assert_eq!(pending.body, replay.body);
    replay.response.send(StatusCode::OK).unwrap();
    let last = endpoint.receive().await;
    assert_eq!(last.body["records"][0]["offset"], "2");
    last.response.send(StatusCode::OK).unwrap();
    wait_committed(&storage, 3).await;
    stop(cancellation, task).await;
}

#[tokio::test]
async fn idle_subscription_sends_only_when_records_arrive_and_cancels_promptly() {
    let storage = storage().await;
    let mut endpoint = Endpoint::new().await;
    let (cancellation, task) = endpoint.start(storage.clone(), 100);
    assert!(
        timeout(Duration::from_millis(100), endpoint.requests.recv())
            .await
            .is_err()
    );
    assert_eq!(committed(&storage).await, 0);
    produce(&storage, 1).await;
    let request = endpoint.receive().await;
    assert_eq!(request.body["records"][0]["offset"], "0");
    request.response.send(StatusCode::OK).unwrap();
    wait_committed(&storage, 1).await;
    stop(cancellation, task).await;
}

#[tokio::test]
async fn timeout_redelivers_and_cancellation_interrupts_an_unresponsive_receiver() {
    let storage = storage().await;
    produce(&storage, 1).await;
    let mut endpoint = Endpoint::new().await;
    let (cancellation, task) = endpoint.start(storage.clone(), 100);
    let first = endpoint.receive().await;
    let retry = endpoint.receive().await;
    assert_eq!(first.body, retry.body);
    assert_eq!(committed(&storage).await, 0);
    stop(cancellation, task).await;
}

#[tokio::test]
async fn out_of_range_cursor_stalls_without_resetting_or_delivering() {
    let storage = storage().await;
    produce(&storage, 1).await;
    _ = storage
        .offset_commit(
            "push-test",
            None,
            &[(
                Topition::new("events", 0),
                OffsetCommitRequest::default().offset(10),
            )],
        )
        .await
        .unwrap();
    let mut endpoint = Endpoint::new().await;
    let (cancellation, task) = endpoint.start(storage.clone(), 100);
    assert!(
        timeout(Duration::from_millis(400), endpoint.requests.recv())
            .await
            .is_err()
    );
    assert_eq!(committed(&storage).await, 10);
    stop(cancellation, task).await;
}

#[tokio::test]
async fn invalid_configuration_is_rejected_before_delivery() {
    let endpoint = Endpoint::new().await;
    let valid: Value = serde_json::from_str(&endpoint.configuration(100)).unwrap();
    for (field, invalid) in [
        ("max_records", json!(0)),
        ("max_batch_bytes", json!(0)),
        ("poll_interval_ms", json!(0)),
        ("request_timeout_ms", json!(0)),
        ("unknown", json!(1)),
    ] {
        let mut configuration = valid.clone();
        configuration[field] = invalid;
        assert!(
            HttpPush::from_json(&configuration.to_string()).is_err(),
            "{field}"
        );
    }
    for endpoint in [
        "ws://localhost/",
        "https://user:secret@localhost/",
        "https://localhost/?token=secret",
        "https://localhost/#fragment",
    ] {
        let mut configuration = valid.clone();
        configuration["subscriptions"][0]["endpoint"] = json!(endpoint);
        assert!(HttpPush::from_json(&configuration.to_string()).is_err());
    }
    let mut duplicate = valid.clone();
    let subscription = duplicate["subscriptions"][0].clone();
    duplicate["subscriptions"]
        .as_array_mut()
        .unwrap()
        .push(subscription);
    assert!(HttpPush::from_json(&duplicate.to_string()).is_err());
    let mut missing_token = valid;
    missing_token["subscriptions"][0]["bearer_token_env"] =
        json!("NISSHI_HTTP_PUSH_TEST_MISSING_CREDENTIAL_8A64");
    assert!(HttpPush::from_json(&missing_token.to_string()).is_err());
}

#[tokio::test]
async fn control_only_batch_advances_without_waking_receiver() {
    let storage = storage().await;
    let batch = inflated::Batch::builder()
        .attributes(0x20)
        .record(Record::builder().value(Some(Bytes::from_static(&[0, 0, 0, 0]))))
        .build()
        .unwrap();
    _ = storage
        .produce(None, &Topition::new("events", 0), batch.try_into().unwrap())
        .await
        .unwrap();
    let mut endpoint = Endpoint::new().await;
    let (cancellation, task) = endpoint.start(storage.clone(), 100);
    wait_committed(&storage, 1).await;
    assert!(
        timeout(Duration::from_millis(100), endpoint.requests.recv())
            .await
            .is_err()
    );
    stop(cancellation, task).await;
}

#[tokio::test]
async fn oversized_record_stalls_instead_of_skipping_or_exceeding_payload_bound() {
    let storage = storage().await;
    let batch = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from(vec![42; 2_048]))))
        .build()
        .unwrap();
    _ = storage
        .produce(None, &Topition::new("events", 0), batch.try_into().unwrap())
        .await
        .unwrap();
    let mut endpoint = Endpoint::new().await;
    let mut configuration: Value = serde_json::from_str(&endpoint.configuration(100)).unwrap();
    configuration["max_batch_bytes"] = json!(1024);
    let push = HttpPush::from_json(&configuration.to_string()).unwrap();
    let cancellation = CancellationToken::new();
    let token = cancellation.clone();
    let task_storage = storage.clone();
    let task = tokio::spawn(async move { push.run(task_storage, token).await.unwrap() });
    wait_committed(&storage, 0).await;
    assert!(
        timeout(Duration::from_millis(400), endpoint.requests.recv())
            .await
            .is_err()
    );
    assert_eq!(committed(&storage).await, 0);
    stop(cancellation, task).await;
}

#[tokio::test]
async fn restart_preserves_initial_cursor_when_retention_removes_unacknowledged_data() {
    use nisshi_sans_io::delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic};
    let storage = storage().await;
    produce(&storage, 1).await;
    let mut endpoint = Endpoint::new().await;
    let (cancellation, task) = endpoint.start(storage.clone(), 100);
    let pending = endpoint.receive().await;
    stop(cancellation, task).await;
    drop(pending.response);
    let deleted = storage
        .delete_records(&[DeleteRecordsTopic::default()
            .name("events".into())
            .partitions(Some(vec![
                DeleteRecordsPartition::default()
                    .partition_index(0)
                    .offset(1),
            ]))])
        .await
        .unwrap();
    assert_eq!(deleted[0].partitions.as_ref().unwrap()[0].error_code, 0);
    produce(&storage, 1).await;
    let (cancellation, task) = endpoint.start(storage.clone(), 100);
    assert!(
        timeout(Duration::from_millis(400), endpoint.requests.recv())
            .await
            .is_err()
    );
    assert_eq!(committed(&storage).await, 0);
    stop(cancellation, task).await;
}

#[tokio::test]
async fn byte_limit_splits_a_kafka_batch_without_losing_or_repeating_offsets() {
    let storage = storage().await;
    let mut batch = inflated::Batch::builder().last_offset_delta(4);
    for offset in 0..5 {
        batch = batch.record(
            Record::builder()
                .offset_delta(offset)
                .value(Some(Bytes::from(vec![42; 256]))),
        );
    }
    _ = storage
        .produce(
            None,
            &Topition::new("events", 0),
            batch.build().unwrap().try_into().unwrap(),
        )
        .await
        .unwrap();
    let mut endpoint = Endpoint::new().await;
    let mut configuration: Value = serde_json::from_str(&endpoint.configuration(100)).unwrap();
    configuration["max_batch_bytes"] = json!(1024);
    let push = HttpPush::from_json(&configuration.to_string()).unwrap();
    let cancellation = CancellationToken::new();
    let token = cancellation.clone();
    let task_storage = storage.clone();
    let task = tokio::spawn(async move { push.run(task_storage, token).await.unwrap() });
    let mut offsets = Vec::new();
    while offsets.len() < 5 {
        let request = endpoint.receive().await;
        assert!(serde_json::to_vec(&request.body).unwrap().len() <= 1024);
        offsets.extend(
            request.body["records"]
                .as_array()
                .unwrap()
                .iter()
                .map(|record| record["offset"].as_str().unwrap().to_owned()),
        );
        request.response.send(StatusCode::OK).unwrap();
    }
    assert_eq!(offsets, ["0", "1", "2", "3", "4"]);
    wait_committed(&storage, 5).await;
    stop(cancellation, task).await;
}

#[tokio::test]
async fn rejected_partition_commit_retries_without_resending_or_advancing_delivery() {
    use nisshi_sans_io::ErrorCode;
    use nisshi_storage::{
        Request as StorageRequest, RequestChannelService, RequestSender, RequestStorageService,
        Response as StorageResponse,
    };
    use rama::Service as _;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    let storage = storage().await;
    produce(&storage, 2).await;
    let allow_commit = Arc::new(AtomicBool::new(false));
    let allowed = Arc::clone(&allow_commit);
    let (sender, mut requests): (RequestSender, _) = mpsc::channel(8);
    let (failed, mut failures) = mpsc::unbounded_channel();
    let service = RequestStorageService::new(storage.clone());
    let forwarding = tokio::spawn(async move {
        while let Some((request, response)) = requests.recv().await {
            if let StorageRequest::OffsetCommit { offsets, .. } = &request
                && offsets[0].1.offset == 1
                && !allowed.load(Ordering::SeqCst)
            {
                _ = response.send(StorageResponse::OffsetCommit(Ok(vec![(
                    offsets[0].0.clone(),
                    ErrorCode::KafkaStorageError,
                )])));
                failed.send(()).unwrap();
                continue;
            }
            _ = response.send(service.serve(request).await.unwrap());
        }
    });
    let mut endpoint = Endpoint::new().await;
    let (cancellation, task) = endpoint.start(Arc::new(RequestChannelService::new(sender)), 1);
    let first = endpoint.receive().await;
    assert_eq!(first.body["records"][0]["offset"], "0");
    first.response.send(StatusCode::OK).unwrap();
    timeout(Duration::from_secs(5), failures.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        timeout(Duration::from_millis(350), endpoint.requests.recv())
            .await
            .is_err()
    );
    assert_eq!(committed(&storage).await, 0);
    allow_commit.store(true, Ordering::SeqCst);
    let second = endpoint.receive().await;
    assert_eq!(second.body["records"][0]["offset"], "1");
    second.response.send(StatusCode::OK).unwrap();
    wait_committed(&storage, 2).await;
    stop(cancellation, task).await;
    timeout(Duration::from_secs(1), forwarding)
        .await
        .unwrap()
        .unwrap();
}
