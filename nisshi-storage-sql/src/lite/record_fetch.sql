-- Copyright ⓒ 2026 Nisshi contributors
-- SPDX-License-Identifier: Apache-2.0

with sized as (
    select r.offset_id, r.attributes, r.timestamp, r.k, r.v,
           sum(coalesce(length(r.k), 0) + coalesce(length(r.v), 0))
               over (order by r.offset_id) as bytes,
           r.producer_id, r.producer_epoch,
           row_number() over (order by r.offset_id) as ordinal
    from cluster c
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id
    join record r on r.topition = tp.id
    where c.name = ?1 and t.name = ?2 and tp.partition = ?3
      and r.offset_id >= ?4 and r.offset_id < ?6
      and (?7 is null or r.k = ?7)
      and (?8 = 0 or (coalesce(r.attributes, 0) & 32) != 0 or not exists (
          select 1 from aborted_transaction a
          where a.topition = tp.id and a.producer_id = r.producer_id
            and a.producer_epoch = r.producer_epoch
            and r.offset_id between a.first_offset and a.last_offset
      ))
)
select * from sized where bytes < ?5 or ordinal = 1 order by offset_id;
