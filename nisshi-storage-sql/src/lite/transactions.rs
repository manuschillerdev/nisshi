// Copyright ⓒ 2026 Nisshi contributors
// SPDX-License-Identifier: Apache-2.0

//! SQLite transaction outcomes survive producer reuse and broker restarts.

use super::*;

#[cfg(test)]
mod tests;

pub(super) async fn initialize_history(connection: &Connection) -> Result<()> {
    let mut tables = connection
        .query(
            "select 1 from sqlite_master where type = 'table' and name = 'aborted_transaction'",
            (),
        )
        .await?;
    if tables.next().await?.is_none() {
        let mut history = connection
            .query(
                "select 1 from txn_detail where status is not null limit 1",
                (),
            )
            .await?;
        if history.next().await?.is_some() {
            return Err(Error::Message(
                "SQLite transaction history predates abort tracking; use a fresh database".into(),
            ));
        }
    }
    let _ = connection
        .execute(include_str!("aborted_transaction.sql"), ())
        .await?;
    Ok(())
}

impl Delegate {
    pub(super) async fn transaction_status(
        &self,
        connection: &PoolConnection,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<Option<TxnState>> {
        let Some(row) = connection
            .query_opt(
                "producer_epoch_for_current_txn.sql",
                (self.cluster.as_str(), transaction_id),
            )
            .await?
        else {
            return Err(Error::Api(ErrorCode::TransactionalIdNotFound));
        };
        if row.get::<i64>(0)? != producer_id {
            return Err(Error::Api(ErrorCode::UnknownProducerId));
        }
        if row.get::<i32>(1)? != i32::from(producer_epoch) {
            return Err(Error::Api(ErrorCode::ProducerFenced));
        }
        row.get::<Option<String>>(2)?
            .map(TxnState::try_from)
            .transpose()
    }

    pub(super) async fn check_producer_epoch(
        &self,
        connection: &PoolConnection,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        let row = connection
            .query_opt(
                "producer_epoch_current_for_producer.sql",
                (self.cluster.as_str(), producer_id),
            )
            .await?
            .ok_or(Error::Api(ErrorCode::UnknownProducerId))?;
        if row.get::<i32>(0)? != i32::from(producer_epoch) {
            return Err(Error::Api(ErrorCode::ProducerFenced));
        }
        Ok(())
    }

    pub(super) async fn record_abort(&self, connection: &PoolConnection, txn: &Txn) -> Result<()> {
        let _ = connection.connection.execute(
            "insert into aborted_transaction (topition, producer_id, producer_epoch, first_offset, last_offset)
             select txn_tp.topition, p.id, pe.epoch, po.offset_start, po.offset_end
             from cluster c
             join producer p on p.cluster = c.id
             join producer_epoch pe on pe.producer = p.id
             join txn on txn.cluster = c.id and txn.producer = p.id
             join txn_detail d on d.\"transaction\" = txn.id and d.producer_epoch = pe.id
             join txn_topition txn_tp on txn_tp.txn_detail = d.id
             join txn_produce_offset po on po.txn_topition = txn_tp.id
             where c.name = ?1 and txn.name = ?2 and p.id = ?3 and pe.epoch = ?4
             on conflict do nothing",
            (self.cluster.as_str(), txn.name.as_str(), txn.producer_id, txn.producer_epoch),
        ).await?;
        Ok(())
    }

    pub(super) async fn expire_transactions(&self, now: SystemTime) -> Result<()> {
        let connection = self.connection().await?;
        let transaction = connection.transaction().await?;
        let mut rows = connection
            .connection
            .query(
                "select txn.name, p.id, pe.epoch, d.status
             from cluster c
             join producer p on p.cluster = c.id
             join producer_epoch pe on pe.producer = p.id
             join txn on txn.cluster = c.id and txn.producer = p.id
             join txn_detail d on d.\"transaction\" = txn.id and d.producer_epoch = pe.id
             where c.name = ?1 and
             ((d.status = 'BEGIN' and d.transaction_timeout_ms > 0 and
               ?2 - cast(strftime('%s', d.started_at) as integer) * 1000 > d.transaction_timeout_ms)
              or d.status in ('PREPARE_COMMIT', 'PREPARE_ABORT'))",
                (self.cluster.as_str(), to_timestamp(&now)?),
            )
            .await?;
        let mut expired = Vec::new();
        while let Some(row) = rows.next().await? {
            expired.push(Txn::try_from(row)?);
        }
        drop(rows);
        for txn in expired {
            let committed = txn.status == TxnState::PrepareCommit;
            let outcome = self
                .end_in_tx(
                    &txn.name,
                    txn.producer_id,
                    txn.producer_epoch,
                    committed,
                    &connection,
                )
                .await?;
            if outcome != ErrorCode::None {
                return Err(Error::Api(outcome));
            }
            if txn.status == TxnState::Begin {
                let _ = connection
                    .query_one(
                        "producer_epoch_insert.sql",
                        (self.cluster.as_str(), txn.producer_id),
                    )
                    .await?;
            }
        }
        connection.commit(transaction).await
    }

    pub(super) async fn list_aborted_transactions(
        &self,
        topition: &Topition,
        offset: i64,
        last_stable_offset: i64,
    ) -> Result<Vec<AbortedTransaction>> {
        let connection = self.connection().await?;
        let mut rows = connection
            .connection
            .query(
                "select a.producer_id, a.first_offset from aborted_transaction a
             join topition tp on tp.id = a.topition
             join topic t on t.id = tp.topic join cluster c on c.id = t.cluster
             where c.name = ?1 and t.name = ?2 and tp.partition = ?3
             and a.last_offset >= ?4 and a.first_offset < ?5 order by a.first_offset",
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                    offset,
                    last_stable_offset,
                ),
            )
            .await?;
        let mut aborted = Vec::new();
        while let Some(row) = rows.next().await? {
            aborted.push(
                AbortedTransaction::default()
                    .producer_id(row.get::<i64>(0)?)
                    .first_offset(row.get::<i64>(1)?),
            );
        }
        Ok(aborted)
    }
}
