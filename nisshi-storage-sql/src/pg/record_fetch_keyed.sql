-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- prepare record_fetch_keyed (text, text, integer, integer, integer, integer, bytea) as
-- A fetch reads at most 10000 rows past the fetch offset. The running byte total below is
-- computed over every row that this query reads, so without the limit, one fetch costs as much
-- as the whole backlog between the fetch offset and the high watermark, however few rows the
-- client can take. The fetch service reads again from the last offset that it received, so a
-- limit that ends a read early costs one more query.
--
-- The limit is inside a lateral join because a limit on the outer join does not stop the
-- planner from reading and sorting every row in the range: it chooses a bitmap scan over the
-- primary key, and the lateral form makes it walk the key in order and stop at the limit.
with candidates as (
select

r.offset_id,
r.attributes,
r.timestamp,
r.k,
r.v,
r.producer_id,
r.producer_epoch

from

cluster c
join topic t on t.cluster = c.id
join topition tp on tp.topic = t.id
cross join lateral (
    select
        r.offset_id,
        r.attributes,
        r.timestamp,
        r.k,
        r.v,
        r.producer_id,
        r.producer_epoch
    from record r
    where r.topition = tp.id
    and r.offset_id >= $4
    and r.offset_id < $6
    and r.k = convert_to($7, 'UTF-8')
    order by r.offset_id
    limit 10000) r

where

c.name = $1
and t.name = $2
and tp.partition = $3),

sized as (
select

offset_id,
attributes,
timestamp,
k,
v,
sum(coalesce(length(k), 0) + coalesce(length(v), 0)) over (order by offset_id) as bytes,
producer_id,
producer_epoch

from

candidates)

select * from sized where bytes < $5 order by offset_id;
