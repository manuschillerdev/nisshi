// Copyright ⓒ 2026 Nisshi contributors
// SPDX-License-Identifier: Apache-2.0

//! A bounded JSON mapping of Kafka's producer transaction operations.

use super::*;
use bytes::Bytes;
use nisshi_sans_io::{
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    record::{Record as KafkaRecord, header},
    txn_offset_commit_request::{TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic},
};
use nisshi_storage::{ProducerIdResponse, TxnAddPartitionsRequest, TxnOffsetCommitRequest};
use serde_json::{Value, json};

type RpcResult<T> = nisshi_storage::Result<T>;

#[derive(Default)]
pub(super) struct Session {
    producer: Option<ProducerIdResponse>,
    staged_delivery: Option<String>,
}

pub(super) struct Response {
    pub json: String,
    pub committed: bool,
    pub reconnect: bool,
    pub initialized: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    version: u8,
    r#type: String,
    request_id: String,
    method: String,
    params: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Producer {
    producer_id: String,
    producer_epoch: i16,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Partition {
    producer_id: String,
    producer_epoch: i16,
    topic: String,
    partition: i32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Produce {
    producer_id: String,
    producer_epoch: i16,
    topic: String,
    partition: i32,
    base_sequence: i32,
    records: Vec<ProduceRecord>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProduceRecord {
    timestamp: String,
    key: Option<String>,
    value: Option<String>,
    headers: Vec<ProduceHeader>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProduceHeader {
    key: Option<String>,
    value: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Offset {
    producer_id: String,
    producer_epoch: i16,
    delivery_id: String,
    next_offset: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct End {
    producer_id: String,
    producer_epoch: i16,
    committed: bool,
}

fn api(code: ErrorCode) -> nisshi_storage::Error {
    nisshi_storage::Error::Api(code)
}

fn parameters<T: serde::de::DeserializeOwned>(value: Value) -> RpcResult<T> {
    serde_json::from_value(value).map_err(|_| api(ErrorCode::InvalidRequest))
}

fn checked(code: ErrorCode) -> RpcResult<()> {
    if code == ErrorCode::None {
        Ok(())
    } else {
        Err(api(code))
    }
}

fn decimal(value: &str) -> RpcResult<i64> {
    let number = value
        .parse::<i64>()
        .map_err(|_| api(ErrorCode::InvalidRequest))?;
    if number.to_string() != value {
        return Err(api(ErrorCode::InvalidRequest));
    }
    Ok(number)
}

fn bytes(value: Option<String>) -> RpcResult<Option<Bytes>> {
    value
        .map(|value| {
            STANDARD
                .decode(value)
                .map(Bytes::from)
                .map_err(|_| api(ErrorCode::InvalidRequest))
        })
        .transpose()
}

impl Session {
    fn producer(&self, id: &str, epoch: i16) -> RpcResult<ProducerIdResponse> {
        let id = decimal(id)?;
        self.producer
            .filter(|producer| producer.id == id && producer.epoch == epoch)
            .ok_or_else(|| api(ErrorCode::InvalidProducerEpoch))
    }
}

impl Subscription {
    pub(super) async fn rpc(
        &self,
        storage: &impl Storage,
        session: &mut Session,
        pending: Option<&Delivery>,
        message: &str,
    ) -> Result<Response> {
        let request: Request =
            serde_json::from_str(message).map_err(|_| invalid("invalid transaction request"))?;
        if request.version != 1
            || request.r#type != "request"
            || request.request_id.is_empty()
            || request.request_id.len() > 128
        {
            return Err(invalid("invalid request envelope"));
        }
        let commit = request.method == "end_txn"
            && request.params.get("committed") == Some(&Value::Bool(true));
        let result = self
            .operation(storage, session, pending, &request.method, request.params)
            .await;
        let (error_code, result) = match result {
            Ok(result) => (ErrorCode::None, result),
            Err(nisshi_storage::Error::Api(code)) => (code, json!({})),
            Err(_) => (ErrorCode::UnknownServerError, json!({})),
        };
        Ok(Response {
            committed: commit && error_code == ErrorCode::None,
            initialized: request.method == "init_producer_id" && error_code == ErrorCode::None,
            // A failed commit response may hide a successful durable decision.
            // Reconnect and reload the group offset before accepting more output.
            reconnect: error_code == ErrorCode::UnknownServerError
                || (request.method == "end_txn" && error_code != ErrorCode::None),
            json: serde_json::to_string(&json!({
                "version": 1, "type": "response", "request_id": request.request_id,
                "error_code": i16::from(error_code), "result": result,
            }))?,
        })
    }

    fn output(&self, topic: &str, partition: i32) -> RpcResult<Topition> {
        if !self.output_topics.contains(topic) {
            return Err(api(ErrorCode::TopicAuthorizationFailed));
        }
        if partition < 0 {
            return Err(api(ErrorCode::InvalidRequest));
        }
        Ok(Topition::new(topic, partition))
    }

    async fn operation(
        &self,
        storage: &impl Storage,
        session: &mut Session,
        pending: Option<&Delivery>,
        method: &str,
        params: Value,
    ) -> RpcResult<Value> {
        let id = self
            .transactional_id
            .as_deref()
            .ok_or_else(|| api(ErrorCode::TransactionalIdAuthorizationFailed))?;
        match method {
            "init_producer_id" => {
                let _: Empty = parameters(params)?;
                let producer = storage
                    .init_producer(Some(id), 30_000, Some(-1), Some(-1))
                    .await?;
                checked(producer.error)?;
                session.producer = Some(producer);
                session.staged_delivery = None;
                return Ok(
                    json!({"producer_id": producer.id.to_string(), "producer_epoch": producer.epoch}),
                );
            }
            "add_partitions_to_txn" => {
                let args: Partition = parameters(params)?;
                let producer = session.producer(&args.producer_id, args.producer_epoch)?;
                let _ = self.output(&args.topic, args.partition)?;
                let result = storage
                    .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                        transaction_id: id.to_owned(),
                        producer_id: producer.id,
                        producer_epoch: producer.epoch,
                        topics: vec![
                            AddPartitionsToTxnTopic::default()
                                .name(args.topic.clone())
                                .partitions(Some(vec![args.partition])),
                        ],
                    })
                    .await?;
                match result.zero_to_three() {
                    [topic] if topic.name == args.topic => {
                        match topic.results_by_partition.as_deref() {
                            Some([partition]) if partition.partition_index == args.partition => {
                                checked(ErrorCode::try_from(partition.partition_error_code)?)?
                            }
                            _ => return Err(api(ErrorCode::UnknownServerError)),
                        }
                    }
                    _ => return Err(api(ErrorCode::UnknownServerError)),
                }
            }
            "produce" => {
                let args: Produce = parameters(params)?;
                let producer = session.producer(&args.producer_id, args.producer_epoch)?;
                let topition = self.output(&args.topic, args.partition)?;
                if args.records.is_empty()
                    || args.records.len() > MAX_RECORDS
                    || args.base_sequence < 0
                    || args
                        .base_sequence
                        .checked_add(args.records.len() as i32)
                        .is_none()
                {
                    return Err(api(ErrorCode::InvalidRequest));
                }
                let base_timestamp = decimal(&args.records[0].timestamp)?;
                let mut maximum = base_timestamp;
                let mut batch = inflated::Batch::builder()
                    .base_timestamp(base_timestamp)
                    .last_offset_delta(args.records.len() as i32 - 1)
                    .attributes(BatchAttribute::default().transaction(true).into())
                    .producer_id(producer.id)
                    .producer_epoch(producer.epoch)
                    .base_sequence(args.base_sequence);
                for (index, record) in args.records.into_iter().enumerate() {
                    let timestamp = decimal(&record.timestamp)?;
                    maximum = maximum.max(timestamp);
                    let delta = timestamp
                        .checked_sub(base_timestamp)
                        .ok_or_else(|| api(ErrorCode::InvalidRequest))?;
                    let mut record_builder = KafkaRecord::builder()
                        .offset_delta(index as i32)
                        .timestamp_delta(delta)
                        .key(bytes(record.key)?)
                        .value(bytes(record.value)?);
                    for header in record.headers {
                        record_builder =
                            record_builder.header(header::Builder::from(header::Header {
                                key: bytes(header.key)?,
                                value: bytes(header.value)?,
                            }));
                    }
                    batch = batch.record(record_builder);
                }
                let batch = batch.max_timestamp(maximum).build()?.try_into()?;
                let offset = storage.produce(Some(id), &topition, batch).await?;
                return Ok(json!({"base_offset": offset.to_string()}));
            }
            "add_offsets_to_txn" => {
                let args: Producer = parameters(params)?;
                let producer = session.producer(&args.producer_id, args.producer_epoch)?;
                checked(
                    storage
                        .txn_add_offsets(id, producer.id, producer.epoch, &self.group_id)
                        .await?,
                )?;
            }
            "txn_offset_commit" => {
                let args: Offset = parameters(params)?;
                let producer = session.producer(&args.producer_id, args.producer_epoch)?;
                let delivery = pending
                    .filter(|delivery| {
                        delivery.ack.delivery_id == args.delivery_id
                            && delivery.ack.next_offset == args.next_offset
                    })
                    .ok_or_else(|| api(ErrorCode::InvalidRequest))?;
                let result = storage
                    .txn_offset_commit(TxnOffsetCommitRequest {
                        transaction_id: id.to_owned(),
                        group_id: self.group_id.clone(),
                        producer_id: producer.id,
                        producer_epoch: producer.epoch,
                        generation_id: None,
                        member_id: None,
                        group_instance_id: None,
                        topics: vec![
                            TxnOffsetCommitRequestTopic::default()
                                .name(self.topic.clone())
                                .partitions(Some(vec![
                                    TxnOffsetCommitRequestPartition::default()
                                        .partition_index(self.partition)
                                        .committed_offset(delivery.next_offset),
                                ])),
                        ],
                    })
                    .await?;
                match result.as_slice() {
                    [topic] if topic.name == self.topic => match topic.partitions.as_deref() {
                        Some([partition]) if partition.partition_index == self.partition => {
                            checked(ErrorCode::try_from(partition.error_code)?)?
                        }
                        _ => return Err(api(ErrorCode::UnknownServerError)),
                    },
                    _ => return Err(api(ErrorCode::UnknownServerError)),
                }
                session.staged_delivery = Some(args.delivery_id);
            }
            "end_txn" => {
                let args: End = parameters(params)?;
                let producer = session.producer(&args.producer_id, args.producer_epoch)?;
                if args.committed
                    && pending.is_some_and(|delivery| {
                        session.staged_delivery.as_ref() != Some(&delivery.ack.delivery_id)
                    })
                {
                    return Err(api(ErrorCode::InvalidRequest));
                }
                checked(
                    storage
                        .txn_end(id, producer.id, producer.epoch, args.committed)
                        .await?,
                )?;
                session.staged_delivery = None;
            }
            "offset_fetch" => {
                let _: Empty = parameters(params)?;
                let offsets = storage
                    .offset_fetch(Some(&self.group_id), &[self.topition()], Some(true))
                    .await?;
                let offset = offsets
                    .get(&self.topition())
                    .copied()
                    .filter(|offset| *offset >= 0)
                    .ok_or_else(|| api(ErrorCode::UnknownServerError))?;
                return Ok(json!({"next_offset": offset.to_string()}));
            }
            _ => return Err(api(ErrorCode::InvalidRequest)),
        }
        Ok(json!({}))
    }
}
