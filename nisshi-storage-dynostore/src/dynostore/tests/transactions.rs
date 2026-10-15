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

use std::sync::atomic::{AtomicU8, Ordering};

use nisshi_sans_io::{
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    txn_offset_commit_request::{TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic},
};
use object_store::memory::InMemory;
use tokio::sync::Notify;

use crate::dynostore::*;

#[derive(Debug, Default)]
struct FaultStore {
    inner: InMemory,
    // 1/2: before/after record PUT; 3/4: before/after commit CAS;
    // 5: pause record PUT; 6: pause after watermark PUT.
    fault: AtomicU8,
    entered: Notify,
    resume: Notify,
}

impl Display for FaultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FaultStore")
    }
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let fault = self.fault.load(Ordering::SeqCst);
        let record = location.as_ref().ends_with(".batch");
        let final_commit = if location.filename() == Some("meta.json") && matches!(fault, 3 | 4) {
            let bytes: Vec<u8> = payload
                .iter()
                .flat_map(|part| part.iter().copied())
                .collect();
            let meta: Meta = serde_json::from_slice(&bytes).expect("metadata");
            meta.transactions
                .values()
                .flat_map(|txn| txn.epochs.values())
                .any(|txn| txn.state == Some(TxnState::Committed))
        } else {
            false
        };
        let trigger = (record && matches!(fault, 1 | 2 | 5))
            || final_commit
            || (fault == 6 && location.filename() == Some("watermark.json"));
        let trigger = trigger
            && self
                .fault
                .compare_exchange(fault, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok();
        let fail = || object_store::Error::Generic {
            store: "injected",
            source: std::io::Error::other("lost write/response").into(),
        };
        if trigger && matches!(fault, 1 | 3) {
            return Err(fail());
        }
        if trigger && fault == 5 {
            self.entered.notify_one();
            self.resume.notified().await;
        }
        let result = self.inner.put_opts(location, payload, options).await?;
        if trigger && fault == 6 {
            self.entered.notify_one();
            self.resume.notified().await;
        }
        if trigger && matches!(fault, 2 | 4) {
            return Err(fail());
        }
        Ok(result)
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, options).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

async fn setup() -> Result<(DynoStore, Arc<FaultStore>, ProducerIdResponse, Topition)> {
    let bucket = Arc::new(FaultStore::default());
    let store = DynoStore::new("transactions", 1, bucket.clone())?;
    _ = store
        .create_topic(
            CreatableTopic::default()
                .name("events".into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some(vec![]))
                .configs(Some(vec![])),
            false,
        )
        .await?;
    let producer = store
        .init_producer(Some("txn"), 100, Some(-1), Some(-1))
        .await?;
    let tp = Topition::new("events", 0);
    begin(&store, &producer, &tp).await?;
    Ok((store, bucket, producer, tp))
}

async fn begin(store: &DynoStore, producer: &ProducerIdResponse, tp: &Topition) -> Result<()> {
    _ = store
        .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: "txn".into(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            topics: vec![
                AddPartitionsToTxnTopic::default()
                    .name(tp.topic.clone())
                    .partitions(Some(vec![tp.partition])),
            ],
        })
        .await?;
    Ok(())
}

fn batch(producer: &ProducerIdResponse, sequence: i32, count: i32) -> Result<deflated::Batch> {
    let mut batch = inflated::Batch::builder()
        .base_timestamp(0)
        .max_timestamp(0)
        .producer_id(producer.id)
        .producer_epoch(producer.epoch)
        .base_sequence(sequence)
        .attributes(BatchAttribute::default().transaction(true).into())
        .last_offset_delta(count - 1);
    for offset in 0..count {
        batch = batch.record(
            Record::builder()
                .offset_delta(offset)
                .value(Some(Bytes::from_static(b"transaction output"))),
        );
    }
    batch
        .build()
        .and_then(TryInto::try_into)
        .map_err(Into::into)
}

async fn stage_offset(
    store: &DynoStore,
    producer: &ProducerIdResponse,
    tp: &Topition,
) -> Result<()> {
    let response = store
        .txn_offset_commit(TxnOffsetCommitRequest {
            transaction_id: "txn".into(),
            group_id: "consumer".into(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            generation_id: None,
            member_id: None,
            group_instance_id: None,
            topics: vec![
                TxnOffsetCommitRequestTopic::default()
                    .name(tp.topic.clone())
                    .partitions(Some(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .partition_index(tp.partition)
                            .committed_offset(42),
                    ])),
            ],
        })
        .await?;
    assert_eq!(
        0,
        response[0].partitions.as_ref().expect("partitions")[0].error_code
    );
    Ok(())
}

async fn cursor(store: &DynoStore, tp: &Topition) -> Result<i64> {
    Ok(store
        .offset_fetch(Some("consumer"), std::slice::from_ref(tp), Some(true))
        .await?[tp])
}

async fn batches(store: &DynoStore, tp: &Topition, offset: i64) -> Result<Vec<deflated::Batch>> {
    store
        .fetch(
            tp,
            offset,
            0,
            1_000_000,
            IsolationLevel::ReadCommitted,
            Duration::from_secs(1),
        )
        .await
}

#[tokio::test]
async fn abort_history_survives_reuse_and_partial_batch_fetch() -> Result<()> {
    let (store, bucket, producer, tp) = setup().await?;
    assert_eq!(
        0,
        store
            .produce(Some("txn"), &tp, batch(&producer, 0, 3)?)
            .await?
    );
    assert!(batches(&store, &tp, 0).await?.is_empty());
    stage_offset(&store, &producer, &tp).await?;
    assert_eq!(
        ErrorCode::None,
        store
            .txn_end("txn", producer.id, producer.epoch, false)
            .await?
    );
    assert_eq!(-1, cursor(&store, &tp).await?);
    begin(&store, &producer, &tp).await?;
    assert_eq!(
        4,
        store
            .produce(Some("txn"), &tp, batch(&producer, 3, 1)?)
            .await?
    );
    stage_offset(&store, &producer, &tp).await?;
    assert_eq!(
        ErrorCode::None,
        store
            .txn_end("txn", producer.id, producer.epoch, true)
            .await?
    );
    let restarted = DynoStore::new("transactions", 2, bucket)?;
    assert_eq!(42, cursor(&restarted, &tp).await?);
    let aborted = restarted.aborted_transactions(&tp, 2, 6).await?;
    assert_eq!(1, aborted.len());
    assert_eq!(producer.id, aborted[0].producer_id);
    assert_eq!(0, aborted[0].first_offset);
    assert!(restarted.aborted_transactions(&tp, 4, 6).await?.is_empty());
    assert_eq!(0, batches(&restarted, &tp, 2).await?[0].base_offset);
    let watermark = restarted.offset_stage(&tp).await?.high_watermark;
    assert_eq!(
        ErrorCode::None,
        restarted
            .txn_end("txn", producer.id, producer.epoch, true)
            .await?
    );
    assert_eq!(watermark, restarted.offset_stage(&tp).await?.high_watermark);
    assert!(matches!(
        restarted
            .txn_end("txn", producer.id, producer.epoch, false)
            .await,
        Err(Error::Api(ErrorCode::InvalidTxnState))
    ));
    _ = restarted
        .offset_commit(
            "consumer",
            None,
            &[(tp.clone(), OffsetCommitRequest::default().offset(43))],
        )
        .await?;
    assert_eq!(43, cursor(&restarted, &tp).await?);
    assert_eq!(
        43,
        restarted.committed_offset_topitions("consumer").await?[&tp]
    );
    assert!(
        restarted
            .list_groups(None)
            .await?
            .iter()
            .any(|group| group.group_id == "consumer")
    );
    _ = restarted.delete_groups(Some(&["consumer".into()])).await?;
    assert_eq!(-1, cursor(&restarted, &tp).await?);
    Ok(())
}

#[tokio::test]
async fn failed_or_uncertain_record_write_reuses_reservation_after_restart() -> Result<()> {
    for fault in [1, 2] {
        let (store, bucket, producer, tp) = setup().await?;
        bucket.fault.store(fault, Ordering::SeqCst);
        assert!(
            store
                .produce(Some("txn"), &tp, batch(&producer, 0, 1)?)
                .await
                .is_err()
        );
        assert!(matches!(
            store
                .txn_end("txn", producer.id, producer.epoch, true)
                .await,
            Err(Error::Api(ErrorCode::ConcurrentTransactions))
        ));
        assert!(batches(&store, &tp, 0).await?.is_empty());
        let restarted = DynoStore::new("transactions", 2, bucket)?;
        assert_eq!(
            0,
            restarted
                .produce(Some("txn"), &tp, batch(&producer, 0, 1)?)
                .await?
        );
        assert_eq!(
            0,
            restarted
                .produce(Some("txn"), &tp, batch(&producer, 0, 1)?)
                .await?
        );
        assert_eq!(
            ErrorCode::None,
            restarted
                .txn_end("txn", producer.id, producer.epoch, true)
                .await?
        );
        assert_eq!(2, batches(&restarted, &tp, 0).await?.len());
    }
    Ok(())
}

#[tokio::test]
async fn commit_failure_and_lost_response_keep_output_and_offset_atomic() -> Result<()> {
    for fault in [3, 4] {
        let (store, bucket, producer, tp) = setup().await?;
        _ = store
            .produce(Some("txn"), &tp, batch(&producer, 0, 1)?)
            .await?;
        stage_offset(&store, &producer, &tp).await?;
        bucket.fault.store(fault, Ordering::SeqCst);
        assert!(
            store
                .txn_end("txn", producer.id, producer.epoch, true)
                .await
                .is_err()
        );
        let restarted = DynoStore::new("transactions", 2, bucket)?;
        assert_eq!(
            if fault == 3 { -1 } else { 42 },
            cursor(&restarted, &tp).await?
        );
        assert_eq!(fault == 3, batches(&restarted, &tp, 0).await?.is_empty());
        restarted.maintain_transactions(SystemTime::now()).await?;
        assert_eq!(42, cursor(&restarted, &tp).await?);
        assert_eq!(2, restarted.offset_stage(&tp).await?.high_watermark);
        assert_eq!(
            ErrorCode::None,
            restarted
                .txn_end("txn", producer.id, producer.epoch, true)
                .await?
        );
        assert_eq!(2, batches(&restarted, &tp, 0).await?.len());
    }
    Ok(())
}

#[tokio::test]
async fn marker_failure_recovers_without_duplicate_markers() -> Result<()> {
    for fault in [1, 2] {
        let (store, bucket, producer, tp) = setup().await?;
        _ = store
            .produce(Some("txn"), &tp, batch(&producer, 0, 1)?)
            .await?;
        stage_offset(&store, &producer, &tp).await?;
        bucket.fault.store(fault, Ordering::SeqCst);
        assert!(
            store
                .txn_end("txn", producer.id, producer.epoch, true)
                .await
                .is_err()
        );
        let restarted = DynoStore::new("transactions", 2, bucket)?;
        assert_eq!(-1, cursor(&restarted, &tp).await?);
        assert!(batches(&restarted, &tp, 0).await?.is_empty());
        restarted.maintain_transactions(SystemTime::now()).await?;
        assert_eq!(42, cursor(&restarted, &tp).await?);
        assert_eq!(2, restarted.offset_stage(&tp).await?.high_watermark);
    }
    Ok(())
}

#[tokio::test]
async fn fencing_aborts_an_inflight_write_even_when_it_lands_late() -> Result<()> {
    let (store, bucket, producer, tp) = setup().await?;
    bucket.fault.store(5, Ordering::SeqCst);
    let task = {
        let store = store.clone();
        let tp = tp.clone();
        let batch = batch(&producer, 0, 1)?;
        tokio::spawn(async move { store.produce(Some("txn"), &tp, batch).await })
    };
    bucket.entered.notified().await;
    let replacement = store
        .init_producer(Some("txn"), 100, Some(-1), Some(-1))
        .await?;
    assert_eq!(producer.epoch + 1, replacement.epoch);
    bucket.resume.notify_one();
    assert!(matches!(
        task.await.expect("produce task"),
        Err(Error::Api(ErrorCode::ProducerFenced))
    ));
    let aborted = store.aborted_transactions(&tp, 0, 2).await?;
    assert_eq!(1, aborted.len());
    assert_eq!(producer.id, aborted[0].producer_id);
    assert!(matches!(
        store
            .txn_add_offsets("txn", producer.id, producer.epoch, "consumer")
            .await,
        Err(Error::Api(ErrorCode::ProducerFenced))
    ));
    assert!(matches!(
        store
            .txn_end("txn", producer.id, producer.epoch, true)
            .await,
        Err(Error::Api(ErrorCode::ProducerFenced))
    ));
    Ok(())
}

#[tokio::test]
async fn timeout_aborts_incomplete_append_and_preserves_consumed_offset() -> Result<()> {
    let (store, bucket, producer, tp) = setup().await?;
    bucket.fault.store(1, Ordering::SeqCst);
    assert!(
        store
            .produce(Some("txn"), &tp, batch(&producer, 0, 1)?)
            .await
            .is_err()
    );
    stage_offset(&store, &producer, &tp).await?;
    let restarted = DynoStore::new("transactions", 2, bucket)?;
    restarted
        .maintain_transactions(SystemTime::now() + Duration::from_secs(1))
        .await?;
    assert_eq!(-1, cursor(&restarted, &tp).await?);
    assert_eq!(2, restarted.offset_stage(&tp).await?.last_stable);
    assert_eq!(1, restarted.aborted_transactions(&tp, 0, 2).await?.len());
    Ok(())
}

#[tokio::test]
async fn legacy_transaction_history_fails_closed() -> Result<()> {
    let (store, _, _, _) = setup().await?;
    assert!(store.supports_transactional_consumption().await?);
    store
        .meta
        .with_mut(&store.object_store, |meta| {
            meta.transaction_history_complete = false;
            Ok(())
        })
        .await?;
    assert!(!store.supports_transactional_consumption().await?);
    Ok(())
}

#[tokio::test]
async fn another_broker_observes_reservations_without_metadata_ttl() -> Result<()> {
    let (store, bucket, producer, tp) = setup().await?;
    let reader = DynoStore::new("transactions", 2, bucket)?;
    assert!(reader.supports_transactional_consumption().await?);
    _ = store
        .produce(Some("txn"), &tp, batch(&producer, 0, 1)?)
        .await?;
    assert_eq!(0, reader.offset_stage(&tp).await?.last_stable);
    assert_eq!(1, reader.offset_stage(&tp).await?.high_watermark);
    assert!(batches(&reader, &tp, 0).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn rejected_metadata_mutation_is_not_published() -> Result<()> {
    let (store, _, _, _) = setup().await?;
    let result: Result<()> = store
        .meta
        .with_mut(&store.object_store, |meta| {
            meta.transaction_history_complete = false;
            Err(Error::Api(ErrorCode::InvalidTxnState))
        })
        .await;
    assert!(result.is_err());
    assert!(store.supports_transactional_consumption().await?);
    Ok(())
}

#[tokio::test]
async fn unassigned_reservation_pins_lso_before_record_publication() -> Result<()> {
    let (store, bucket, producer, tp) = setup().await?;
    bucket.fault.store(6, Ordering::SeqCst);
    let task = {
        let store = store.clone();
        let tp = tp.clone();
        let batch = batch(&producer, 0, 1)?;
        tokio::spawn(async move { store.produce(Some("txn"), &tp, batch).await })
    };
    bucket.entered.notified().await;
    let other = DynoStore::new("transactions", 2, bucket.clone())?;
    let ordinary = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"later"))))
        .build()
        .and_then(TryInto::try_into)?;
    assert_eq!(1, other.produce(None, &tp, ordinary).await?);
    assert_eq!(2, other.offset_stage(&tp).await?.high_watermark);
    assert_eq!(0, other.offset_stage(&tp).await?.last_stable);
    assert!(batches(&other, &tp, 0).await?.is_empty());
    bucket.resume.notify_one();
    assert_eq!(0, task.await.expect("produce task")?);
    assert_eq!(
        ErrorCode::None,
        store
            .txn_end("txn", producer.id, producer.epoch, true)
            .await?
    );
    let restarted = DynoStore::new("transactions", 3, bucket)?;
    assert_eq!(3, batches(&restarted, &tp, 0).await?.len());
    Ok(())
}

#[tokio::test]
async fn epoch_exhaustion_rotates_producer_and_prunes_completed_epochs() -> Result<()> {
    let (store, _, producer, tp) = setup().await?;
    _ = store
        .produce(Some("txn"), &tp, batch(&producer, 0, 1)?)
        .await?;
    assert_eq!(
        ErrorCode::None,
        store
            .txn_end("txn", producer.id, producer.epoch, false)
            .await?
    );
    store
        .meta
        .with_mut(&store.object_store, |meta| {
            let transaction = meta.transactions.get_mut("txn").expect("txn");
            let detail = transaction.epochs.remove(&0).expect("epoch");
            _ = transaction.epochs.insert(i16::MAX, detail);
            let pd = meta.producers.get_mut(&producer.id).expect("producer");
            pd.sequences.clear();
            _ = pd.sequences.insert(i16::MAX, BTreeMap::new());
            Ok(())
        })
        .await?;
    let replacement = store
        .init_producer(Some("txn"), 100, Some(producer.id), Some(i16::MAX))
        .await?;
    assert_eq!(ErrorCode::None, replacement.error);
    assert_ne!(producer.id, replacement.id);
    assert_eq!(0, replacement.epoch);
    assert_eq!(1, store.aborted_transactions(&tp, 0, 2).await?.len());
    store
        .meta
        .with_fresh(&store.object_store, |meta| {
            assert_eq!(1, meta.transactions["txn"].epochs.len());
            assert_eq!(1, meta.producers[&replacement.id].sequences.len());
            assert!(!meta.producers.contains_key(&producer.id));
            Ok(())
        })
        .await?;
    begin(&store, &replacement, &tp).await?;
    assert_eq!(
        2,
        store
            .produce(Some("txn"), &tp, batch(&replacement, 0, 1)?)
            .await?
    );
    Ok(())
}

#[tokio::test]
async fn expired_attempt_cannot_abort_next_transaction_in_same_epoch() -> Result<()> {
    let (store, _, producer, tp) = setup().await?;
    let expired = store
        .meta
        .with_fresh(&store.object_store, |meta| {
            Ok(meta.transactions["txn"].epochs[&producer.epoch].attempt)
        })
        .await?;
    _ = store
        .produce(Some("txn"), &tp, batch(&producer, 0, 1)?)
        .await?;
    assert_eq!(
        ErrorCode::None,
        store
            .txn_end("txn", producer.id, producer.epoch, true)
            .await?
    );
    begin(&store, &producer, &tp).await?;
    _ = store
        .produce(Some("txn"), &tp, batch(&producer, 1, 1)?)
        .await?;
    assert_eq!(
        ErrorCode::None,
        store
            .end_transaction("txn", producer.id, producer.epoch, false, Some(expired))
            .await?
    );
    assert_eq!(2, store.offset_stage(&tp).await?.last_stable);
    assert_eq!(
        ErrorCode::None,
        store
            .txn_end("txn", producer.id, producer.epoch, true)
            .await?
    );
    assert!(store.aborted_transactions(&tp, 0, 4).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn legacy_offsets_are_read_until_replaced_and_deleted() -> Result<()> {
    let (store, bucket, _, tp) = setup().await?;
    let path = store.committed_offset_location("consumer", &tp)?;
    let payload = Bytes::from(serde_json::to_vec(
        &OffsetCommitRequest::default().offset(12),
    )?);
    _ = bucket.put(&path, payload.into()).await?;
    assert_eq!(12, cursor(&store, &tp).await?);
    _ = store
        .offset_commit(
            "consumer",
            None,
            &[(tp.clone(), OffsetCommitRequest::default().offset(13))],
        )
        .await?;
    let restarted = DynoStore::new("transactions", 2, bucket)?;
    assert_eq!(13, cursor(&restarted, &tp).await?);
    _ = restarted.delete_groups(Some(&["consumer".into()])).await?;
    assert_eq!(-1, cursor(&restarted, &tp).await?);
    Ok(())
}

#[tokio::test]
async fn abort_history_survives_retention_for_fetches_already_in_progress() -> Result<()> {
    let (store, _, producer, tp) = setup().await?;
    _ = store
        .produce(Some("txn"), &tp, batch(&producer, 0, 2)?)
        .await?;
    assert_eq!(
        ErrorCode::None,
        store
            .txn_end("txn", producer.id, producer.epoch, false)
            .await?
    );
    for cutoff in [1, 3] {
        _ = store
            .delete_records(&[DeleteRecordsTopic::default()
                .name(tp.topic.clone())
                .partitions(Some(vec![
                    nisshi_sans_io::delete_records_request::DeleteRecordsPartition::default()
                        .partition_index(0)
                        .offset(cutoff),
                ]))])
            .await?;
        assert_eq!(1, store.aborted_transactions(&tp, 0, 3).await?.len());
    }
    Ok(())
}

#[tokio::test]
async fn another_broker_observes_retention_without_watermark_ttl() -> Result<()> {
    let (store, bucket, producer, tp) = setup().await?;
    _ = store
        .produce(Some("txn"), &tp, batch(&producer, 0, 2)?)
        .await?;
    assert_eq!(
        ErrorCode::None,
        store
            .txn_end("txn", producer.id, producer.epoch, true)
            .await?
    );
    let reader = DynoStore::new("transactions", 2, bucket)?;
    assert_eq!(0, reader.offset_stage(&tp).await?.log_start);
    _ = store
        .delete_records(&[DeleteRecordsTopic::default()
            .name(tp.topic.clone())
            .partitions(Some(vec![
                nisshi_sans_io::delete_records_request::DeleteRecordsPartition::default()
                    .partition_index(0)
                    .offset(1),
            ]))])
        .await?;
    assert_eq!(1, reader.offset_stage(&tp).await?.log_start);
    Ok(())
}
