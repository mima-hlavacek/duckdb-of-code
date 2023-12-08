create table raw_inputs as 
select
    column0 as line,
    row_number() over () - 1 as row
from read_csv_auto('input.txt', sep=chr(28))
;

create table grid as (
with
    unnested as (
        select
            row,
            unnest(string_split(line, '')) as character
        from raw_inputs
    )
select
    row,
    row_number() over (partition by row) - 1 as col,
    character
from unnested
)
;

with
    symbols as materialized (
        select *
        from grid
        where not regexp_matches(character, '\d|\.')
    ), numbers as materialized ( -- Oof, duckdb does not have "match index", need to do some involved parsing
        with
            raw_number_starts as (
                select
                    row,
                    col
                from grid
                qualify 
                    regexp_matches(character, '\d')
                    and 
                        (
                            not regexp_matches(lag(character) over (partition by row order by col), '\d')
                            or lag(character) over (partition by row order by col) is null
                        )
            ), number_starts_with_order as (
                select
                    row,
                    col,
                    row_number() over (partition by row order by col) as order_in_row
                from raw_number_starts
            ), raw_numbers as (
                select
                    row,
                    unnest(regexp_extract_all(line, '\d+')) as number
                from raw_inputs
            ), numbers_with_order as (
                select
                    row,
                    number,
                    row_number() over (partition by row) as order_in_row
                from raw_numbers
            )
        select 
            row,
            col,
            number 
        from 
            number_starts_with_order
            join numbers_with_order using
                (row, order_in_row)
    )
select
    sum(number::int) as part_one
from 
    numbers
    semi join symbols on
        abs(numbers.row - symbols.row) <= 1
        and numbers.col - 1 <= symbols.col
        and symbols.col <= numbers.col + len(numbers.number)
;

with
    symbols as materialized (
        select *
        from grid
        where not regexp_matches(character, '\d|\.')
    ), numbers as materialized ( -- Oof, duckdb does not have "match index", need to do some involved parsing
        with
            raw_number_starts as (
                select
                    row,
                    col
                from grid
                qualify 
                    regexp_matches(character, '\d')
                    and 
                        (
                            not regexp_matches(lag(character) over (partition by row order by col), '\d')
                            or lag(character) over (partition by row order by col) is null
                        )
            ), number_starts_with_order as (
                select
                    row,
                    col,
                    row_number() over (partition by row order by col) as order_in_row
                from raw_number_starts
            ), raw_numbers as (
                select
                    row,
                    unnest(regexp_extract_all(line, '\d+')) as number
                from raw_inputs
            ), numbers_with_order as (
                select
                    row,
                    number,
                    row_number() over (partition by row) as order_in_row
                from raw_numbers
            )
        select 
            row,
            col,
            number 
        from 
            number_starts_with_order
            join numbers_with_order using
                (row, order_in_row)
    ), gear_ratios as (
        select
            symbols.row,
            symbols.col,
            product(number::int)::int as gear_ratio
        from 
            symbols
            join numbers on
                abs(numbers.row - symbols.row) <= 1
                and numbers.col - 1 <= symbols.col
                and symbols.col <= numbers.col + len(numbers.number)
        where
            symbols.character = '*'
        group by
            symbols.row,
            symbols.col
        having
            count(*) = 2
    )
select sum(gear_ratio) as part_two
from gear_ratios
;
