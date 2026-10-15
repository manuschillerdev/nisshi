-- Copyright ⓒ 2026 Nisshi contributors
-- SPDX-License-Identifier: Apache-2.0

create table if not exists aborted_transaction (
    topition integer not null references topition (id) on delete cascade,
    producer_id integer not null,
    producer_epoch integer not null,
    first_offset integer not null,
    last_offset integer not null,
    primary key (topition, producer_id, producer_epoch, first_offset)
);
