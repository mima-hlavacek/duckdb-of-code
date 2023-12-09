create table raw_input as 
select
    column0 as line,
    row_number() over () - 1 as row
from read_csv_auto('input.txt', sep=chr(28))
;

create table sequences as
select
    row as sequence_id,
    unnest(generate_series(1, len(string_to_array(line, ' ')))) as position,
    unnest(string_to_array(line, ' '))::int64 as value
from raw_input
;

with recursive
    differences as (
        select
            sequence_id,
            position,
            value,
            0 as iteration
        from sequences
        union all
        select
            sequence_id,
            position,
            lead(value) over (partition by sequence_id order by position) - value as difference,
            iteration + 1 as iteration
        from differences
        qualify lead(value) over (partition by sequence_id order by position) is not null
    ), last_difference_per_iteration as (
        select distinct on (sequence_id, iteration)
            sequence_id,
            value
        from differences
        order by
            sequence_id,
            iteration,
            position desc
    ), inferred_values as (
        select
            sequence_id,
            sum(value) as value
        from last_difference_per_iteration
        group by sequence_id
    )
select sum(value) as part_one
from inferred_values
;

with recursive
    differences as (
        select
            sequence_id,
            position,
            value,
            0 as iteration
        from sequences
        union all
        select
            sequence_id,
            position,
            lead(value) over (partition by sequence_id order by position) - value as difference,
            iteration + 1 as iteration
        from differences
        qualify lead(value) over (partition by sequence_id order by position) is not null
    ), first_difference_per_iteration as materialized (
        select distinct on (sequence_id, iteration)
            sequence_id,
            iteration,
            value
        from differences
        order by
            sequence_id,
            iteration,
            position 
    ), inferred_values as (
        select
            sequence_id,
            sum(value * ((-1)**iteration)::int64) as value
        from first_difference_per_iteration
        group by sequence_id
    )
select sum(value) as part_two
from inferred_values
;
