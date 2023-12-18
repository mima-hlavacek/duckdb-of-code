create table raw_input as 
select
    line,
    row_number() over () - 1 as row
from read_csv('input.txt', sep=chr(28), columns={'line': 'text'})
;

with recursive
    instructions as materialized (
        select 
            row,
            split_part(line, ' ', 1) as direction,
            split_part(line, ' ', 2)::int as distance
        from raw_input
    ), borders as (
        select
            0 as row_from,
            0 as col_from,
            0 as row_to,
            0 as col_to,
            0 as next_instruction 
        union all
        select
            row_to as row_from,
            col_to as col_from,
            case
                when direction = 'U' then
                    row_to - distance
                when direction = 'D' then
                    row_to + distance
                else
                    row_to
            end as row_to,
            case
                when direction = 'R' then
                    col_to + distance
                when direction = 'L' then
                    col_to - distance
                else
                    col_to
            end as col_to,
            next_instruction + 1 as next_instruction
        from 
            borders
            join instructions on
                next_instruction = row
    )
select
    ( -- Inner area
        select sum(col_from*row_to - col_to*row_from) // 2
        from borders
    ) + ( -- Half of circumference - regular tiles contribute 1/2, corners either 3/4 or 1/4 and we have a loop so all but four conrers pair up
        select sum(distance) // 2
        from instructions
    ) + 1 -- four corners contribute 3/4 each
    as part_one
;

with recursive
    code_to_direction(code, direction) as ( values
        (0, 'R'),
        (1, 'D'),
        (2, 'L'),
        (3, 'U')
    ), instructions as materialized (
        select 
            row,
            code_to_direction.direction as direction,
            ('0x' || split_part(line, ' ', 3)[3:-3])::int64 as distance
        from 
            raw_input
            join code_to_direction on
                split_part(line, ' ', 3)[-2]::int = code
    ), borders as (
        select
            0::int64 as row_from,
            0::int64 as col_from,
            0::int64 as row_to,
            0::int64 as col_to,
            0 as next_instruction 
        union all
        select
            row_to as row_from,
            col_to as col_from,
            case
                when direction = 'U' then
                    row_to - distance
                when direction = 'D' then
                    row_to + distance
                else
                    row_to
            end as row_to,
            case
                when direction = 'R' then
                    col_to + distance
                when direction = 'L' then
                    col_to - distance
                else
                    col_to
            end as col_to,
            next_instruction + 1 as next_instruction
        from 
            borders
            join instructions on
                next_instruction = row
    )
select
    ( -- Inner area
        select sum(col_from*row_to - col_to*row_from) // 2
        from borders
    ) + ( -- Half of circumference - regular tiles contribute 1/2, corners either 3/4 or 1/4 and we have a loop so all but four conrers pair up
        select sum(distance) // 2
        from instructions
    ) + 1 -- four corners contribute 3/4 each
    as part_two
;
