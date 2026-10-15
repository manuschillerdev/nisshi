// Copyright ⓒ 2026 Nisshi contributors
// SPDX-License-Identifier: Apache-2.0

//! Broker-initiated WebSocket subscriptions for consumers that sleep between deliveries.
//!
//! Each configured group/topic/partition must have exactly one broker owner. These
//! subscriptions use stored Kafka offsets, but do not join Kafka consumer groups.

use crate::{Error, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures::{SinkExt, StreamExt};
use nisshi_sans_io::{BatchAttribute, ErrorCode, IsolationLevel, TimestampType, record::inflated};
use nisshi_storage::{OffsetCommitRequest, Storage, Topition};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, path::Path, sync::Arc, time::Duration};
use tokio::{
    net::TcpStream,
    task::JoinSet,
    time::{self, timeout},
};
use tokio_tungstenite::{
    Connector, MaybeTlsStream, WebSocketStream, connect_async_tls_with_config,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::{HeaderValue, header::AUTHORIZATION},
        protocol::WebSocketConfig,
    },
};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use url::Url;
use uuid::Uuid;

const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const MAX_RECORDS: usize = 100;
const POLL_INTERVAL: Duration = Duration::from_millis(500);
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const ACK_TIMEOUT: Duration = Duration::from_secs(30);
const PING_INTERVAL: Duration = Duration::from_secs(30);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

mod rpc;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    subscriptions: Vec<Subscription>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Subscription {
    group_id: String,
    topic: String,
    partition: i32,
    endpoint: Url,
    bearer_token_env: Option<String>,
    transactional_id: Option<String>,
    #[serde(default)]
    output_topics: BTreeSet<String>,
    #[serde(skip)]
    authorization: Option<HeaderValue>,
}

/// Validated, operator-owned subscriptions loaded once at startup.
#[derive(Clone, Debug)]
pub struct Config {
    subscriptions: Vec<Subscription>,
    tls: Arc<rustls::ClientConfig>,
}

impl Config {
    /// Rejects invalid endpoints, duplicate assignments, and absent credentials.
    pub fn load(path: &Path) -> Result<Self> {
        let configuration: ConfigFile = serde_json::from_slice(&std::fs::read(path)?)?;
        let mut assignments = BTreeSet::new();
        let mut transactions = BTreeSet::new();
        let mut subscriptions = configuration.subscriptions;
        if subscriptions.is_empty() || subscriptions.len() > 1024 {
            return Err(invalid("expected between 1 and 1024 subscriptions"));
        }
        for subscription in &mut subscriptions {
            subscription.validate()?;
            if let Some(id) = &subscription.transactional_id
                && !transactions.insert(id.clone())
            {
                return Err(invalid(
                    "transactional_id must be unique across subscriptions",
                ));
            }
            if !assignments.insert((
                subscription.group_id.clone(),
                subscription.topic.clone(),
                subscription.partition,
            )) {
                return Err(invalid("duplicate group/topic/partition assignment"));
            }
            if let Some(variable) = &subscription.bearer_token_env {
                let token = std::env::var(variable)
                    .map_err(|_| invalid("bearer token environment variable is missing"))?;
                if token.is_empty() {
                    return Err(invalid("bearer token must not be empty"));
                }
                let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))
                    .map_err(|_| invalid("bearer token is not a valid HTTP header"))?;
                authorization.set_sensitive(true);
                subscription.authorization = Some(authorization);
            }
        }
        let mut roots = rustls::RootCertStore::empty();
        let certificates = rustls_native_certs::load_native_certs();
        let _ = roots.add_parsable_certificates(certificates.certs);
        if roots.is_empty()
            && subscriptions
                .iter()
                .any(|subscription| subscription.endpoint.scheme() == "wss")
        {
            return Err(invalid("no system TLS root certificates available"));
        }
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| invalid("TLS provider configuration failed"))?
        .with_root_certificates(roots)
        .with_no_client_auth();
        Ok(Self {
            subscriptions,
            tls: Arc::new(tls),
        })
    }

    pub(crate) async fn spawn<S: Storage + Clone>(
        &self,
        tasks: &mut JoinSet<()>,
        storage: S,
        cancellation: CancellationToken,
    ) -> Result<()> {
        if !storage.supports_transactional_consumption().await? {
            return Err(invalid(
                "storage backend does not support committed transactional consumption",
            ));
        }
        for subscription in self.subscriptions.clone() {
            let storage = storage.clone();
            let cancellation = cancellation.clone();
            let tls = self.tls.clone();
            let _ = tasks.spawn(async move {
                tokio::select! {
                    _ = cancellation.cancelled() => {},
                    _ = subscription.run(storage, tls) => {},
                }
            });
        }
        Ok(())
    }
}

fn invalid(message: &str) -> Error {
    Error::Message(format!("websocket push: {message}"))
}

impl Subscription {
    fn validate(&self) -> Result<()> {
        if self.group_id.is_empty()
            || self.group_id.len() > 255
            || self.topic.is_empty()
            || self.topic.len() > 249
            || self.partition < 0
        {
            return Err(invalid(
                "group_id/topic must be nonempty and bounded; partition must be nonnegative",
            ));
        }
        if !matches!(self.endpoint.scheme(), "ws" | "wss")
            || self.endpoint.host_str().is_none()
            || !self.endpoint.username().is_empty()
            || self.endpoint.password().is_some()
            || self.endpoint.query().is_some()
            || self.endpoint.fragment().is_some()
        {
            return Err(invalid(
                "endpoint must be ws/wss with a host, without credentials, query or fragment",
            ));
        }
        if let Some(id) = &self.transactional_id {
            if id.is_empty() || id.len() > 255 || self.output_topics.is_empty() {
                return Err(invalid(
                    "transactional_id must be bounded and have output_topics",
                ));
            }
        } else if !self.output_topics.is_empty() {
            return Err(invalid("output_topics requires transactional_id"));
        }
        if self.output_topics.len() > 100
            || self
                .output_topics
                .iter()
                .any(|topic| topic.is_empty() || topic.len() > 249)
        {
            return Err(invalid(
                "output_topics must contain at most 100 bounded topic names",
            ));
        }
        Ok(())
    }

    fn topition(&self) -> Topition {
        Topition::new(self.topic.clone(), self.partition)
    }

    async fn run<S: Storage>(&self, storage: S, tls: Arc<rustls::ClientConfig>) {
        let mut cursor = None;
        let mut pending: Option<Delivery> = None;
        let mut retry_delay = POLL_INTERVAL;
        loop {
            let attempt = async {
                // EndTxn may have committed even if its response was lost. Reconcile
                // before replaying a delivery and producing its output again.
                if self.transactional_id.is_some() && cursor.is_some() {
                    let committed = timeout(IO_TIMEOUT, self.committed_offset(&storage))
                        .await
                        .map_err(|_| invalid("offset fetch timeout"))??;
                    if let Some(delivery) = &pending {
                        if committed == delivery.next_offset {
                            cursor = Some(committed);
                            pending = None;
                        } else if Some(committed) != cursor {
                            return Err(invalid("transaction offset changed unexpectedly"));
                        }
                    } else {
                        cursor = Some(committed);
                    }
                }
                if cursor.is_none() {
                    let offsets = timeout(
                        IO_TIMEOUT,
                        storage.offset_fetch(Some(&self.group_id), &[self.topition()], Some(false)),
                    )
                    .await
                    .map_err(|_| invalid("offset fetch timeout"))??;
                    let committed = offsets
                        .get(&self.topition())
                        .copied()
                        .filter(|offset| *offset >= 0);
                    let offset = match committed {
                        Some(offset) => offset,
                        None => {
                            timeout(IO_TIMEOUT, storage.offset_stage(&self.topition()))
                                .await
                                .map_err(|_| invalid("offset stage timeout"))??
                                .log_start
                        }
                    };
                    // Persist the initial position so downtime before the first delivery cannot
                    // silently reset an existing subscription past records removed by retention.
                    self.commit(&storage, offset).await?;
                    cursor = Some(offset);
                }
                let mut request = self
                    .endpoint
                    .as_str()
                    .into_client_request()
                    .map_err(|_| invalid("invalid websocket request"))?;
                if let Some(authorization) = &self.authorization {
                    let _ = request
                        .headers_mut()
                        .insert(AUTHORIZATION, authorization.clone());
                }
                let limits = WebSocketConfig::default()
                    .max_message_size(Some(MAX_PAYLOAD_BYTES))
                    .max_frame_size(Some(MAX_PAYLOAD_BYTES));
                let (mut socket, _) = timeout(
                    IO_TIMEOUT,
                    connect_async_tls_with_config(
                        request,
                        Some(limits),
                        true,
                        Some(Connector::Rustls(tls.clone())),
                    ),
                )
                .await
                .map_err(|_| invalid("connection timeout"))?
                .map_err(|_| {
                    invalid("connection failed; check endpoint, TLS and authentication")
                })?;
                self.deliver(
                    &storage,
                    &mut socket,
                    &mut cursor,
                    &mut pending,
                    &mut retry_delay,
                )
                .await
            };
            let result = attempt.await;
            if let Err(error) = result {
                warn!(group_id = %self.group_id, topic = %self.topic, partition = self.partition, %error, "websocket delivery will retry");
            }
            time::sleep(retry_delay).await;
            retry_delay = retry_delay.saturating_mul(2).min(MAX_RETRY_DELAY);
        }
    }

    async fn commit(&self, storage: &impl Storage, next_offset: i64) -> Result<()> {
        let topition = self.topition();
        let committed = timeout(
            IO_TIMEOUT,
            storage.offset_commit(
                &self.group_id,
                None,
                &[(
                    topition.clone(),
                    OffsetCommitRequest::default().offset(next_offset),
                )],
            ),
        )
        .await
        .map_err(|_| invalid("offset commit timeout"))??;
        match committed.as_slice() {
            [(partition, ErrorCode::None)] if partition == &topition => Ok(()),
            _ => Err(invalid("offset commit failed")),
        }
    }

    async fn committed_offset(&self, storage: &impl Storage) -> Result<i64> {
        storage
            .offset_fetch(Some(&self.group_id), &[self.topition()], Some(true))
            .await?
            .get(&self.topition())
            .copied()
            .filter(|offset| *offset >= 0)
            .ok_or_else(|| invalid("missing committed subscription offset"))
    }

    async fn deliver(
        &self,
        storage: &impl Storage,
        socket: &mut Socket,
        cursor: &mut Option<i64>,
        pending: &mut Option<Delivery>,
        retry_delay: &mut Duration,
    ) -> Result<()> {
        let mut polls = time::interval(POLL_INTERVAL);
        polls.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        let mut pings = time::interval_at(time::Instant::now() + PING_INTERVAL, PING_INTERVAL);
        pings.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        let mut awaiting_pong = false;
        let mut deadline = None;
        let mut session = rpc::Session::default();
        loop {
            tokio::select! {
                _ = polls.tick() => {
                    let offset = cursor.ok_or_else(|| invalid("missing subscription cursor"))?;
                    if pending.is_none() {
                        let (delivery, next_offset) = timeout(IO_TIMEOUT, self.fetch(storage, offset)).await.map_err(|_| invalid("fetch timeout"))??;
                        *pending = delivery;
                        if pending.is_none() && next_offset > offset {
                            self.commit(storage, next_offset).await?;
                            *cursor = Some(next_offset);
                        }
                    }
                    if deadline.is_none() && let Some(delivery) = pending {
                        send(socket, Message::Text(delivery.json.clone().into())).await?;
                        deadline = Some(time::Instant::now() + ACK_TIMEOUT);
                    }
                },
                _ = time::sleep_until(deadline.unwrap_or_else(|| time::Instant::now() + ACK_TIMEOUT)), if deadline.is_some() => {
                    return Err(invalid("delivery completion timeout"));
                },
                _ = pings.tick() => {
                    if awaiting_pong { return Err(invalid("ping timeout")); }
                    send(socket, Message::Ping(Vec::new().into())).await?;
                    awaiting_pong = true;
                },
                incoming = socket.next() => match incoming {
                    Some(Ok(Message::Pong(_))) => { awaiting_pong = false; *retry_delay = POLL_INTERVAL; },
                    Some(Ok(Message::Ping(_))) => flush(socket).await?,
                    Some(Ok(Message::Text(message))) => {
                        if self.transactional_id.is_some() {
                            let response = timeout(IO_TIMEOUT, self.rpc(storage, &mut session, pending.as_ref(), &message))
                                .await.map_err(|_| invalid("transaction request timeout; reconnect to reconcile"))??;
                            let mut recovered_commit = false;
                            if response.committed || response.initialized {
                                let offset = timeout(IO_TIMEOUT, self.committed_offset(storage)).await
                                    .map_err(|_| invalid("offset reconciliation timeout"))??;
                                if let Some(delivery) = pending {
                                    if response.committed && offset != delivery.next_offset {
                                        return Err(invalid("transaction did not commit pending delivery offset"));
                                    }
                                    if offset == delivery.next_offset {
                                        recovered_commit = response.initialized;
                                        *cursor = Some(offset);
                                        *pending = None;
                                        deadline = None;
                                        polls.reset_immediately();
                                    } else if Some(offset) != *cursor {
                                        return Err(invalid("transaction offset changed unexpectedly"));
                                    }
                                }
                            }
                            send(socket, Message::Text(response.json.into())).await?;
                            if recovered_commit {
                                return Err(invalid("initialization completed previous commit; reconnect to resume"));
                            }
                            if response.reconnect { return Err(invalid("transaction request failed; reconnect to reconcile")); }
                        } else {
                            let ack: Ack = serde_json::from_str(&message).map_err(|_| invalid("invalid acknowledgement"))?;
                            let delivery = pending.as_ref().ok_or_else(|| invalid("no pending delivery"))?;
                            if ack != delivery.ack { return Err(invalid("acknowledgement does not match pending delivery")); }
                            self.commit(storage, delivery.next_offset).await?;
                            *cursor = Some(delivery.next_offset);
                            *pending = None;
                            deadline = None;
                            polls.reset_immediately();
                        }
                        *retry_delay = POLL_INTERVAL;
                    },
                    _ => return Err(invalid("connection closed or unexpected idle message")),
                },
            }
        }
    }

    async fn fetch(&self, storage: &impl Storage, cursor: i64) -> Result<(Option<Delivery>, i64)> {
        let topition = self.topition();
        let stage = storage.offset_stage(&topition).await?;
        if cursor < stage.log_start || cursor > stage.high_watermark {
            return Err(invalid(
                "stored offset is outside retained log; stop broker, reset group offset, then restart",
            ));
        }
        let batches = storage
            .fetch(
                &topition,
                cursor,
                0,
                MAX_PAYLOAD_BYTES as u32,
                IsolationLevel::ReadCommitted,
                POLL_INTERVAL,
            )
            .await?;
        let first = batches
            .first()
            .map_or(cursor, |batch| batch.base_offset.min(cursor));
        let fetched_end = batches.last().map_or(Ok(cursor), |batch| {
            batch
                .base_offset
                .checked_add(i64::from(batch.last_offset_delta))
                .and_then(|offset| offset.checked_add(1))
                .ok_or_else(|| invalid("invalid batch offset"))
        })?;
        let mut aborted = storage
            .aborted_transactions(&topition, first, fetched_end.min(stage.last_stable))
            .await?;
        // Retention may prune abort metadata after the batch read. Validate
        // after both reads so those bytes can never escape without their filter.
        if cursor < storage.offset_stage(&topition).await?.log_start {
            return Err(invalid(
                "retention advanced during fetch; stop broker, reset group offset, then restart",
            ));
        }
        aborted.sort_by_key(|transaction| transaction.first_offset);
        let mut aborted = aborted.into_iter().peekable();
        let mut active_aborts = BTreeSet::new();
        let mut envelope = Envelope {
            version: 1,
            r#type: "records",
            delivery_id: Uuid::now_v7().to_string(),
            group_id: self.group_id.clone(),
            topic: self.topic.clone(),
            partition: self.partition,
            next_offset: cursor.to_string(),
            records: Vec::new(),
        };
        let mut next_offset = cursor;
        let mut payload_bytes = serde_json::to_vec(&envelope)?.len() + 32;
        'batches: for batch in batches {
            let batch_end = batch
                .base_offset
                .checked_add(i64::from(batch.last_offset_delta))
                .and_then(|offset| offset.checked_add(1))
                .ok_or_else(|| invalid("invalid batch offset"))?;
            while aborted
                .peek()
                .is_some_and(|transaction| transaction.first_offset <= batch.base_offset)
            {
                if let Some(transaction) = aborted.next() {
                    let _ = active_aborts.insert(transaction.producer_id);
                }
            }
            if batch.is_control() {
                // The abort marker ends this producer's aborted range. A later
                // committed transaction may reuse the same producer ID and epoch.
                let _ = active_aborts.remove(&batch.producer_id);
                next_offset = next_offset.max(batch_end.min(stage.last_stable));
                continue;
            }
            if batch_end <= cursor {
                continue;
            }
            if batch.base_offset >= stage.last_stable {
                break;
            }
            if batch.is_transactional() && active_aborts.contains(&batch.producer_id) {
                next_offset = next_offset.max(batch_end.min(stage.last_stable));
                continue;
            }
            // Kafka fetch may return an oversized first batch to allow progress. The
            // existing decoder bounds decompression; split records into bounded JSON messages.
            let attributes = BatchAttribute::try_from(batch.attributes)?;
            let batch = inflated::Batch::try_from(batch)?;
            for record in batch.records {
                let offset = batch
                    .base_offset
                    .checked_add(i64::from(record.offset_delta))
                    .ok_or_else(|| invalid("invalid record offset"))?;
                if offset < cursor || offset >= stage.last_stable {
                    continue;
                }
                let timestamp = match attributes.timestamp {
                    TimestampType::LogAppendTime => batch.max_timestamp,
                    TimestampType::CreateTime => batch
                        .base_timestamp
                        .checked_add(record.timestamp_delta)
                        .ok_or_else(|| invalid("invalid record timestamp"))?,
                };
                let record = Record {
                    offset: offset.to_string(),
                    timestamp: timestamp.to_string(),
                    key: record.key.map(|bytes| STANDARD.encode(bytes)),
                    value: record.value.map(|bytes| STANDARD.encode(bytes)),
                    headers: record
                        .headers
                        .into_iter()
                        .map(|header| Header {
                            key: header.key.map(|bytes| STANDARD.encode(bytes)),
                            value: header.value.map(|bytes| STANDARD.encode(bytes)),
                        })
                        .collect(),
                };
                let record_bytes = serde_json::to_vec(&record)?.len() + 1;
                if envelope.records.len() == MAX_RECORDS
                    || payload_bytes + record_bytes > MAX_PAYLOAD_BYTES
                {
                    if envelope.records.is_empty() {
                        return Err(invalid(
                            "record exceeds 1 MiB delivery limit; offset has not advanced",
                        ));
                    }
                    break 'batches;
                }
                payload_bytes += record_bytes;
                envelope.records.push(record);
                next_offset = offset
                    .checked_add(1)
                    .ok_or_else(|| invalid("record offset overflow"))?;
            }
            next_offset = next_offset.max(batch_end.min(stage.last_stable));
        }
        if envelope.records.is_empty() {
            return Ok((None, next_offset));
        }
        envelope.next_offset = next_offset.to_string();
        let ack = Ack {
            version: 1,
            r#type: "ack".into(),
            delivery_id: envelope.delivery_id.clone(),
            group_id: self.group_id.clone(),
            topic: self.topic.clone(),
            partition: self.partition,
            next_offset: envelope.next_offset.clone(),
        };
        Ok((
            Some(Delivery {
                json: serde_json::to_string(&envelope)?,
                next_offset,
                ack,
            }),
            next_offset,
        ))
    }
}

#[derive(Debug)]
struct Delivery {
    json: String,
    next_offset: i64,
    ack: Ack,
}

#[derive(Serialize)]
struct Envelope {
    version: u8,
    r#type: &'static str,
    delivery_id: String,
    group_id: String,
    topic: String,
    partition: i32,
    next_offset: String,
    records: Vec<Record>,
}
#[derive(Serialize)]
struct Record {
    offset: String,
    timestamp: String,
    key: Option<String>,
    value: Option<String>,
    headers: Vec<Header>,
}
#[derive(Serialize)]
struct Header {
    key: Option<String>,
    value: Option<String>,
}
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ack {
    version: u8,
    r#type: String,
    delivery_id: String,
    group_id: String,
    topic: String,
    partition: i32,
    next_offset: String,
}

async fn send(socket: &mut Socket, message: Message) -> Result<()> {
    timeout(IO_TIMEOUT, socket.send(message))
        .await
        .map_err(|_| invalid("send timeout"))?
        .map_err(|_| invalid("websocket send failed"))
}
async fn flush(socket: &mut Socket) -> Result<()> {
    timeout(IO_TIMEOUT, socket.flush())
        .await
        .map_err(|_| invalid("flush timeout"))?
        .map_err(|_| invalid("websocket flush failed"))
}
#[cfg(test)]
mod tests;
