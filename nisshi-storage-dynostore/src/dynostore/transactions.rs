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

use super::*;

impl Meta {
    pub(super) fn transaction_mut(
        &mut self,
        id: &str,
        producer: i64,
        epoch: i16,
    ) -> Result<&mut TxnDetail> {
        let txn = self
            .transactions
            .get_mut(id)
            .ok_or(Error::Api(ErrorCode::TransactionalIdNotFound))?;
        if txn.producer != producer {
            return Err(Error::Api(ErrorCode::UnknownProducerId));
        }
        if txn.epochs.last_key_value().map(|(epoch, _)| *epoch) != Some(epoch) {
            return Err(Error::Api(ErrorCode::ProducerFenced));
        }
        txn.epochs
            .get_mut(&epoch)
            .ok_or(Error::Api(ErrorCode::ProducerFenced))
    }
}

impl TxnDetail {
    pub(super) fn begin(&mut self) -> Result<()> {
        match self.state {
            Some(TxnState::PrepareCommit | TxnState::PrepareAbort) => {
                return Err(Error::Api(ErrorCode::ConcurrentTransactions));
            }
            Some(TxnState::Begin) => return Ok(()),
            _ => {}
        }
        self.produces.clear();
        self.offsets.clear();
        self.appends.clear();
        self.markers.clear();
        self.attempt = Uuid::new_v4();
        self.started_at = Some(SystemTime::now());
        self.state = Some(TxnState::Begin);
        Ok(())
    }

    fn append_mut(&mut self, tp: &Topition, control: bool) -> Option<&mut TxnAppend> {
        let appends = if control {
            &mut self.markers
        } else {
            &mut self.appends
        };
        appends
            .get_mut(tp.topic())
            .and_then(|partitions| partitions.get_mut(&tp.partition))
    }
}

impl DynoStore {
    /// A durable reservation precedes offset allocation and record publication.
    /// Retrying a failed write reuses that reservation; commit cannot pass an
    /// incomplete append. Abort retains its range even if a fenced writer's
    /// already-started object PUT arrives after the abort marker.
    pub(super) async fn transactional_produce(
        &self,
        id: &str,
        tp: &Topition,
        batch: deflated::Batch,
        control: bool,
        expected_attempt: Option<Uuid>,
    ) -> Result<i64> {
        if !control && (!batch.is_idempotent() || batch.base_sequence < 0) {
            return Err(Error::Api(ErrorCode::InvalidProducerEpoch));
        }
        if batch.last_offset_delta < 0 {
            return Err(Error::Api(ErrorCode::CorruptMessage));
        }
        let next_sequence = if control {
            0
        } else {
            batch
                .base_sequence
                .checked_add(batch.last_offset_delta)
                .and_then(|sequence| sequence.checked_add(1))
                .ok_or(Error::Api(ErrorCode::OutOfOrderSequenceNumber))?
        };
        let ticket = Uuid::new_v4();
        let reservation = self
            .meta
            .with_mut(&self.object_store, |meta| {
                let expected = meta
                    .producers
                    .get(&batch.producer_id)
                    .and_then(|producer| producer.sequences.get(&batch.producer_epoch))
                    .and_then(|topics| topics.get(tp.topic()))
                    .and_then(|partitions| partitions.get(&tp.partition))
                    .copied()
                    .unwrap_or_default();
                let detail = meta.transaction_mut(id, batch.producer_id, batch.producer_epoch)?;
                if expected_attempt.is_some_and(|attempt| attempt != detail.attempt) {
                    return Err(Error::Api(ErrorCode::InvalidTxnState));
                }
                let valid = if control {
                    matches!(
                        detail.state,
                        Some(TxnState::PrepareCommit | TxnState::PrepareAbort)
                    )
                } else {
                    detail.state == Some(TxnState::Begin)
                };
                if !valid {
                    return Err(Error::Api(ErrorCode::InvalidTxnState));
                }
                if !detail
                    .produces
                    .get(tp.topic())
                    .is_some_and(|partitions| partitions.contains_key(&tp.partition))
                {
                    return Err(Error::Api(ErrorCode::InvalidTxnState));
                }
                if let Some(append) = detail.append_mut(tp, control) {
                    if append.sequence == batch.base_sequence
                        && append.crc == batch.crc
                        && append.last_offset_delta == batch.last_offset_delta
                    {
                        return Ok(append.clone());
                    }
                    if !append.complete || control {
                        return Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber));
                    }
                }
                if !control && expected != batch.base_sequence {
                    return Err(Error::Api(if expected > batch.base_sequence {
                        ErrorCode::DuplicateSequenceNumber
                    } else {
                        ErrorCode::OutOfOrderSequenceNumber
                    }));
                }
                let append = TxnAppend {
                    id: ticket,
                    sequence: batch.base_sequence,
                    crc: batch.crc,
                    last_offset_delta: batch.last_offset_delta,
                    offset: None,
                    complete: false,
                };
                let appends = if control {
                    &mut detail.markers
                } else {
                    &mut detail.appends
                };
                _ = appends
                    .entry(tp.topic.clone())
                    .or_default()
                    .insert(tp.partition, append.clone());
                Ok(append)
            })
            .await?;
        if reservation.complete {
            return reservation
                .offset
                .ok_or(Error::Api(ErrorCode::UnknownServerError));
        }
        let offset = if let Some(offset) = reservation.offset {
            offset
        } else {
            let watermark = self
                .watermarks
                .lock()?
                .entry(tp.clone())
                .or_insert_with(|| OptiCon::<Watermark>::new(&self.cluster, tp))
                .clone();
            let allocated = watermark
                .with_mut(&self.object_store, |watermark| {
                    let offset = watermark.high.unwrap_or_default();
                    watermark.high = Some(
                        offset
                            .checked_add(i64::from(batch.last_offset_delta))
                            .and_then(|end| end.checked_add(1))
                            .ok_or(Error::Api(ErrorCode::OffsetOutOfRange))?,
                    );
                    watermark.timestamps = None;
                    Ok(offset)
                })
                .await?;
            self.meta
                .with_mut(&self.object_store, |meta| {
                    let detail =
                        meta.transaction_mut(id, batch.producer_id, batch.producer_epoch)?;
                    let valid = if control {
                        matches!(
                            detail.state,
                            Some(TxnState::PrepareCommit | TxnState::PrepareAbort)
                        )
                    } else {
                        detail.state == Some(TxnState::Begin)
                    };
                    if !valid {
                        return Err(Error::Api(ErrorCode::InvalidTxnState));
                    }
                    let append = detail
                        .append_mut(tp, control)
                        .filter(|append| append.id == reservation.id)
                        .ok_or(Error::Api(ErrorCode::InvalidTxnState))?;
                    let offset = *append.offset.get_or_insert(allocated);
                    let end = offset + i64::from(batch.last_offset_delta);
                    let range = detail
                        .produces
                        .entry(tp.topic.clone())
                        .or_default()
                        .entry(tp.partition)
                        .or_default();
                    if let Some(range) = range {
                        range.offset_start = range.offset_start.min(offset);
                        range.offset_end = range.offset_end.max(end);
                    } else {
                        *range = Some(TxnProduceOffset {
                            offset_start: offset,
                            offset_end: end,
                        });
                    }
                    Ok(offset)
                })
                .await?
        };
        let location = Path::from(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/records/{:0>20}.batch",
            self.cluster, tp.topic, tp.partition, offset
        ));
        let payload = self.encode(batch.clone())?;
        match self
            .object_store
            .put_opts(
                &location,
                payload.clone(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => {}
            Err(object_store::Error::AlreadyExists { .. }) => {
                // The previous PUT may have succeeded while its response was lost.
                let stored = self.object_store.get(&location).await?.bytes().await?;
                let expected: Vec<u8> = payload
                    .iter()
                    .flat_map(|bytes| bytes.iter().copied())
                    .collect();
                if stored.as_ref() != expected.as_slice() {
                    return Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber));
                }
            }
            Err(error) => return Err(error.into()),
        }
        self.meta
            .with_mut(&self.object_store, |meta| {
                let detail = meta.transaction_mut(id, batch.producer_id, batch.producer_epoch)?;
                let valid = if control {
                    matches!(
                        detail.state,
                        Some(TxnState::PrepareCommit | TxnState::PrepareAbort)
                    )
                } else {
                    detail.state == Some(TxnState::Begin)
                };
                if !valid {
                    return Err(Error::Api(ErrorCode::InvalidTxnState));
                }
                let append = detail
                    .append_mut(tp, control)
                    .filter(|append| append.id == reservation.id)
                    .ok_or(Error::Api(ErrorCode::InvalidTxnState))?;
                append.complete = true;
                if !control {
                    let producer = meta
                        .producers
                        .get_mut(&batch.producer_id)
                        .ok_or(Error::Api(ErrorCode::UnknownProducerId))?;
                    _ = producer
                        .sequences
                        .entry(batch.producer_epoch)
                        .or_default()
                        .entry(tp.topic.clone())
                        .or_default()
                        .insert(tp.partition, next_sequence);
                }
                Ok(offset)
            })
            .await
    }

    pub(super) async fn end_transaction(
        &self,
        id: &str,
        producer: i64,
        epoch: i16,
        committed: bool,
        expected_attempt: Option<Uuid>,
    ) -> Result<ErrorCode> {
        let terminal = if committed {
            TxnState::Committed
        } else {
            TxnState::Aborted
        };
        let prepared = if committed {
            TxnState::PrepareCommit
        } else {
            TxnState::PrepareAbort
        };
        let produced = self
            .meta
            .with_mut(&self.object_store, |meta| {
                if let Some(attempt) = expected_attempt {
                    let current = meta
                        .transactions
                        .get(id)
                        .filter(|txn| txn.producer == producer)
                        .and_then(|txn| txn.epochs.get(&epoch));
                    if current.is_none_or(|detail| {
                        detail.attempt != attempt
                            || (detail.state != Some(TxnState::Begin)
                                && detail.state != Some(prepared))
                    }) {
                        return Ok(None);
                    }
                }
                let detail = meta.transaction_mut(id, producer, epoch)?;
                if detail.state == Some(terminal) {
                    return Ok(None);
                }
                if detail.state != Some(TxnState::Begin) && detail.state != Some(prepared) {
                    return Err(Error::Api(ErrorCode::InvalidTxnState));
                }
                if committed
                    && detail
                        .appends
                        .values()
                        .flat_map(BTreeMap::values)
                        .any(|append| !append.complete)
                {
                    return Err(Error::Api(ErrorCode::ConcurrentTransactions));
                }
                detail.state = Some(prepared);
                Ok(Some((
                    detail.attempt,
                    detail
                        .produces
                        .iter()
                        .flat_map(|(topic, partitions)| {
                            partitions.iter().filter_map(move |(partition, range)| {
                                range.map(|_| Topition::new(topic, *partition))
                            })
                        })
                        .collect::<Vec<_>>(),
                )))
            })
            .await?;
        let Some((attempt, produced)) = produced else {
            return Ok(ErrorCode::None);
        };
        for tp in produced {
            let key: Bytes = if committed {
                ControlBatch::default().commit()
            } else {
                ControlBatch::default().abort()
            }
            .try_into()?;
            let value: Bytes = EndTransactionMarker::default().try_into()?;
            // A retried marker must have identical bytes, including timestamps.
            let marker = inflated::Batch::builder()
                .base_timestamp(0)
                .max_timestamp(0)
                .record(Record::builder().key(Some(key)).value(Some(value)))
                .attributes(
                    BatchAttribute::default()
                        .control(true)
                        .transaction(true)
                        .into(),
                )
                .producer_id(producer)
                .producer_epoch(epoch)
                .base_sequence(-1)
                .build()
                .and_then(TryInto::try_into)?;
            _ = self
                .transactional_produce(id, &tp, marker, true, Some(attempt))
                .await?;
        }
        self.meta
            .with_mut(&self.object_store, |meta| {
                let detail = meta.transaction_mut(id, producer, epoch)?;
                if detail.attempt != attempt {
                    return Err(Error::Api(ErrorCode::InvalidTxnState));
                }
                if detail.state == Some(terminal) {
                    return Ok(ErrorCode::None);
                }
                if detail.state != Some(prepared) {
                    return Err(Error::Api(ErrorCode::InvalidTxnState));
                }
                for (topic, partitions) in &detail.produces {
                    for (partition, range) in partitions {
                        if range.is_some()
                            && !detail
                                .markers
                                .get(topic)
                                .and_then(|partitions| partitions.get(partition))
                                .is_some_and(|marker| marker.complete)
                        {
                            return Err(Error::Api(ErrorCode::ConcurrentTransactions));
                        }
                    }
                }
                let offsets = std::mem::take(&mut detail.offsets);
                let produced = std::mem::take(&mut detail.produces);
                detail.state = Some(terminal);
                detail.started_at = None;
                detail.appends.clear();
                detail.markers.clear();
                if committed {
                    for (group, topics) in offsets {
                        for (topic, partitions) in topics {
                            for (partition, offset) in partitions {
                                _ = meta
                                    .committed_offsets
                                    .entry(group.clone())
                                    .or_default()
                                    .entry(topic.clone())
                                    .or_default()
                                    .insert(
                                        partition,
                                        OffsetCommitRequest {
                                            offset: offset.committed_offset,
                                            leader_epoch: offset.leader_epoch,
                                            metadata: offset.metadata,
                                            timestamp: None,
                                        },
                                    );
                            }
                        }
                    }
                } else {
                    for (topic, partitions) in produced {
                        for (partition, range) in partitions {
                            if let Some(range) = range {
                                meta.aborted
                                    .entry(topic.clone())
                                    .or_default()
                                    .entry(partition)
                                    .or_default()
                                    .push(AbortedRange {
                                        producer_id: producer,
                                        first_offset: range.offset_start,
                                        last_offset: range.offset_end,
                                    });
                            }
                        }
                    }
                }
                Ok(ErrorCode::None)
            })
            .await
    }
}
