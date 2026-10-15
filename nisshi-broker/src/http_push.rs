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

//! Delivers configured topic partitions to HTTP consumers with acknowledged offsets.

use crate::{Error, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use nisshi_sans_io::{
    ErrorCode, IsolationLevel, TimestampType,
    record::{deflated, inflated},
};
use nisshi_storage::{OffsetCommitRequest, Storage, Topition};
use reqwest::{
    Client,
    header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue},
    redirect::Policy,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, path::Path, sync::Arc, time::Duration};
use tokio::{task::JoinSet, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};
use url::Url;

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Configuration {
    subscriptions: Vec<Subscription>,
    poll_interval_ms: u64,
    request_timeout_ms: u64,
    max_batch_bytes: u32,
    max_records: usize,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            subscriptions: Vec::new(),
            poll_interval_ms: 1_000,
            request_timeout_ms: 30_000,
            max_batch_bytes: 262_144,
            max_records: 100,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Subscription {
    group_id: String,
    topic: String,
    partition: i32,
    endpoint: Url,
    bearer_token_env: Option<String>,
    #[serde(skip)]
    authorization: Option<HeaderValue>,
}

impl Subscription {
    fn topition(&self) -> Topition {
        Topition::new(self.topic.clone(), self.partition)
    }
}

/// A fixed set of HTTP subscriptions. Each group/topic/partition must have one owner.
#[derive(Clone, Debug)]
pub struct HttpPush {
    configuration: Configuration,
    client: Client,
}

impl HttpPush {
    pub fn load(path: &Path) -> Result<Self> {
        Self::from_json(&std::fs::read_to_string(path)?)
    }

    /// Validates configuration and resolves bearer credentials before delivery starts.
    pub fn from_json(json: &str) -> Result<Self> {
        let mut configuration: Configuration = serde_json::from_str(json)?;
        if configuration.subscriptions.is_empty()
            || configuration.subscriptions.len() > 1_024
            || !(1..=60_000).contains(&configuration.poll_interval_ms)
            || !(1..=300_000).contains(&configuration.request_timeout_ms)
            || !(1_024..=16_777_216).contains(&configuration.max_batch_bytes)
            || !(1..=10_000).contains(&configuration.max_records)
        {
            return Err(Error::Message(
                "invalid HTTP push limits; see docs/http-push.md".into(),
            ));
        }
        let mut assignments = BTreeSet::new();
        for subscription in &mut configuration.subscriptions {
            if subscription.group_id.trim().is_empty()
                || subscription.group_id.len() > 255
                || subscription.topic.trim().is_empty()
                || subscription.topic.len() > 249
                || subscription.partition < 0
                || !assignments.insert((subscription.group_id.clone(), subscription.topition()))
            {
                return Err(Error::Message(
                    "invalid or duplicate HTTP push subscription".into(),
                ));
            }
            let endpoint = &subscription.endpoint;
            if !matches!(endpoint.scheme(), "http" | "https")
                || endpoint.host_str().is_none()
                || !endpoint.username().is_empty()
                || endpoint.password().is_some()
                || endpoint.query().is_some()
                || endpoint.fragment().is_some()
            {
                return Err(Error::Message(
                    "HTTP push endpoint must be HTTP(S), without userinfo, query or fragment"
                        .into(),
                ));
            }
            if let Some(name) = &subscription.bearer_token_env {
                if name.is_empty() || name.contains(['=', '\0']) {
                    return Err(Error::Message(
                        "invalid HTTP push bearer environment variable name".into(),
                    ));
                }
                let token = std::env::var(name).map_err(|_| {
                    Error::Message(format!(
                        "HTTP push bearer environment variable {name} is missing or not Unicode"
                    ))
                })?;
                if token.is_empty() {
                    return Err(Error::Message(format!(
                        "HTTP push bearer environment variable {name} is empty"
                    )));
                }
                let mut authorization =
                    HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
                        Error::Message(format!(
                            "HTTP push bearer environment variable {name} is not a valid header"
                        ))
                    })?;
                authorization.set_sensitive(true);
                subscription.authorization = Some(authorization);
            }
        }
        let client = Client::builder()
            .redirect(Policy::none())
            .retry(reqwest::retry::never())
            .timeout(Duration::from_millis(configuration.request_timeout_ms))
            .build()
            .map_err(http_error)?;
        Ok(Self {
            configuration,
            client,
        })
    }

    /// Runs until cancellation; a failed task stops the broker instead of losing a subscription.
    pub async fn run<S: Storage + Clone>(
        &self,
        storage: S,
        cancellation: CancellationToken,
    ) -> Result<()> {
        let mut tasks = JoinSet::new();
        let delivery = Arc::new(self.clone());
        for subscription in &self.configuration.subscriptions {
            let delivery = Arc::clone(&delivery);
            let subscription = subscription.clone();
            let storage = storage.clone();
            _ = tasks.spawn(async move { delivery.deliver(storage, subscription).await });
        }
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                Ok(())
            }
            finished = tasks.join_next() => match finished {
                Some(Err(error)) => Err(error.into()),
                _ => Err(Error::Message("HTTP push task stopped unexpectedly".into())),
            }
        }
    }

    async fn deliver<S: Storage>(&self, storage: S, subscription: Subscription) {
        let mut offset = None;
        let mut retry_delay = Duration::from_millis(250);
        loop {
            match self
                .deliver_next(&storage, &subscription, &mut offset)
                .await
            {
                Ok(delivered) => {
                    retry_delay = Duration::from_millis(250);
                    if !delivered {
                        sleep(Duration::from_millis(self.configuration.poll_interval_ms)).await;
                    }
                }
                Err(error) => {
                    error!(group_id = %subscription.group_id, topic = %subscription.topic,
                        partition = subscription.partition, ?offset, %error, "HTTP push is stalled; retrying without advancing its offset");
                    sleep(retry_delay).await;
                    retry_delay = (retry_delay * 2).min(Duration::from_secs(30));
                }
            }
        }
    }

    async fn deliver_next<S: Storage>(
        &self,
        storage: &S,
        subscription: &Subscription,
        offset: &mut Option<i64>,
    ) -> Result<bool> {
        let topition = subscription.topition();
        if offset.is_none() {
            let committed = storage
                .offset_fetch(
                    Some(&subscription.group_id),
                    std::slice::from_ref(&topition),
                    Some(false),
                )
                .await?;
            *offset = Some(
                match committed
                    .get(&topition)
                    .copied()
                    .filter(|offset| *offset >= 0)
                {
                    Some(offset) => offset,
                    None => {
                        let start = storage.offset_stage(&topition).await?.log_start;
                        // Persist the starting position so a restart cannot hide retention loss
                        // while the first delivery is still unacknowledged.
                        commit(storage, subscription, start).await?;
                        start
                    }
                },
            );
        }
        let cursor = offset.ok_or_else(|| Error::Message("missing HTTP push offset".into()))?;
        check_cursor(storage, &topition, cursor).await?;
        let batches = storage
            .fetch(
                &topition,
                cursor,
                1,
                self.configuration.max_batch_bytes,
                IsolationLevel::ReadUncommitted,
                Duration::from_secs(1),
            )
            .await?;
        // Retention can advance between checking the cursor and fetching the batches.
        check_cursor(storage, &topition, cursor).await?;
        let (body, next_offset) = encode_batch(subscription, cursor, batches, &self.configuration)?;
        if next_offset == cursor {
            return Ok(false);
        }
        if let Some(body) = body {
            self.post_until_acknowledged(subscription, body).await;
        }
        let mut retry_delay = Duration::from_millis(250);
        loop {
            match commit(storage, subscription, next_offset).await {
                Ok(()) => break,
                Err(error) => {
                    error!(group_id = %subscription.group_id, topic = %subscription.topic,
                        partition = subscription.partition, next_offset, %error, "HTTP push offset commit failed; retrying");
                    sleep(retry_delay).await;
                    retry_delay = (retry_delay * 2).min(Duration::from_secs(30));
                }
            }
        }
        *offset = Some(next_offset);
        Ok(true)
    }

    async fn post_until_acknowledged(&self, subscription: &Subscription, body: Bytes) {
        let mut retry_delay = Duration::from_millis(250);
        loop {
            let mut request = self
                .client
                .post(subscription.endpoint.clone())
                .header(CONTENT_TYPE, "application/json")
                .body(body.clone());
            if let Some(authorization) = &subscription.authorization {
                request = request.header(AUTHORIZATION, authorization.clone());
            }
            let failure = match request.send().await {
                Ok(response) if response.status().is_success() => return,
                Ok(response) => format!("HTTP {}", response.status().as_u16()),
                Err(error) => error.without_url().to_string(),
            };
            warn!(group_id = %subscription.group_id, topic = %subscription.topic,
                partition = subscription.partition, %failure, "HTTP push was not acknowledged; retrying the same batch");
            sleep(retry_delay).await;
            retry_delay = (retry_delay * 2).min(Duration::from_secs(30));
        }
    }
}

fn http_error(error: reqwest::Error) -> Error {
    Error::Message(error.without_url().to_string())
}

async fn check_cursor<S: Storage>(storage: &S, topition: &Topition, cursor: i64) -> Result<()> {
    let stage = storage.offset_stage(topition).await?;
    if cursor < stage.log_start || cursor > stage.high_watermark {
        return Err(Error::Message(format!(
            "HTTP push offset {cursor} is outside retained range {}..={}; stop the broker, reset the group offset explicitly, then restart to resume",
            stage.log_start, stage.high_watermark
        )));
    }
    Ok(())
}

async fn commit<S: Storage>(storage: &S, subscription: &Subscription, offset: i64) -> Result<()> {
    let topition = subscription.topition();
    let committed = storage
        .offset_commit(
            &subscription.group_id,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;
    if committed.len() != 1 || committed[0].0 != topition || committed[0].1 != ErrorCode::None {
        return Err(Error::Message(format!(
            "HTTP push offset commit rejected: {committed:?}"
        )));
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct Delivery<'a> {
    version: u8,
    group_id: &'a str,
    topic: &'a str,
    partition: i32,
    next_offset: String,
    records: Vec<PushRecord>,
}

#[derive(Debug, Serialize)]
struct PushRecord {
    offset: String,
    timestamp: String,
    key: Option<String>,
    value: Option<String>,
    headers: Vec<PushHeader>,
}

#[derive(Debug, Serialize)]
struct PushHeader {
    key: Option<String>,
    value: Option<String>,
}

fn base64(bytes: &Option<Bytes>) -> Option<String> {
    bytes.as_ref().map(|bytes| STANDARD.encode(bytes))
}

fn advance(base: i64, delta: i64) -> Result<i64> {
    base.checked_add(delta)
        .ok_or_else(|| Error::Message("HTTP push offset or timestamp overflow".into()))
}

fn encode_batch(
    subscription: &Subscription,
    cursor: i64,
    batches: Vec<deflated::Batch>,
    configuration: &Configuration,
) -> Result<(Option<Bytes>, i64)> {
    let mut delivery = Delivery {
        version: 1,
        group_id: &subscription.group_id,
        topic: &subscription.topic,
        partition: subscription.partition,
        next_offset: i64::MAX.to_string(),
        records: Vec::new(),
    };
    let mut encoded_bytes = serde_json::to_vec(&delivery)?.len();
    let mut next_offset = cursor;
    'batches: for batch in batches {
        let batch_end = advance(batch.base_offset, i64::from(batch.last_offset_delta) + 1)?;
        if batch_end <= next_offset {
            continue;
        }
        if batch.is_control() {
            next_offset = batch_end;
            continue;
        }
        // Storage can return an entire Kafka batch when the cursor is in its middle,
        // including a first batch larger than the fetch byte limit.
        let batch = inflated::Batch::try_from(batch)?;
        for record in batch.records {
            let record_offset = advance(batch.base_offset, i64::from(record.offset_delta))?;
            if record_offset < next_offset {
                continue;
            }
            let timestamp = match TimestampType::from(batch.attributes) {
                TimestampType::CreateTime => advance(batch.base_timestamp, record.timestamp_delta)?,
                TimestampType::LogAppendTime => batch.max_timestamp,
            };
            let record = PushRecord {
                offset: record_offset.to_string(),
                timestamp: timestamp.to_string(),
                key: base64(&record.key),
                value: base64(&record.value),
                headers: record
                    .headers
                    .into_iter()
                    .map(|header| PushHeader {
                        key: base64(&header.key),
                        value: base64(&header.value),
                    })
                    .collect(),
            };
            let record_bytes =
                serde_json::to_vec(&record)?.len() + usize::from(!delivery.records.is_empty());
            if encoded_bytes + record_bytes > configuration.max_batch_bytes as usize
                || delivery.records.len() == configuration.max_records
            {
                if delivery.records.is_empty() {
                    return Err(Error::Message(format!(
                        "HTTP push record at offset {record_offset} exceeds max_batch_bytes; increase the limit to resume"
                    )));
                }
                break 'batches;
            }
            encoded_bytes += record_bytes;
            delivery.records.push(record);
            next_offset = advance(record_offset, 1)?;
        }
        next_offset = next_offset.max(batch_end);
    }
    delivery.next_offset = next_offset.to_string();
    let body = if delivery.records.is_empty() {
        None
    } else {
        Some(Bytes::from(serde_json::to_vec(&delivery)?))
    };
    Ok((body, next_offset))
}
