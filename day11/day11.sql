create table raw_input as 
select
    column0 as line,
    row_number() over () - 1 as row
from read_csv_auto('input.txt', sep=chr(28))
;

with
    space as materialized (
        select
            row,
            unnest(generate_series(1, len(line))) - 1 as col,
            unnest(string_to_array(line, '')) = '#' as is_galaxy
        from raw_input
    ), empty_rows as materialized (
        select
            row,
            bool_and(not is_galaxy) as is_empty
        from space
        group by row
    ), empty_cols as materialized (
        select
            col,
            bool_and(not is_galaxy) as is_empty
        from space
        group by col
    ), row_shifts as materialized (
        select
            row,
            count(*) filter (where is_empty) over (order by row) as row_shift
        from empty_rows
    ), col_shifts as materialized (
        select
            col,
            count(*) filter (where is_empty) over (order by col) as col_shift
        from empty_cols
    ), shifted_galaxies as materialized (
        select
            row + row_shift as row,
            col + col_shift as col
        from
            space
            join row_shifts using
                (row)
            join col_shifts using
                (col)
        where is_galaxy
    )
select sum(abs(first_galaxy.row - second_galaxy.row) + abs(first_galaxy.col - second_galaxy.col)) // 2 as part_one
from
    shifted_galaxies first_galaxy
    cross join shifted_galaxies second_galaxy
;

with
    space as materialized (
        select
            row,
            unnest(generate_series(1, len(line))) - 1 as col,
            unnest(string_to_array(line, '')) = '#' as is_galaxy
        from raw_input
    ), empty_rows as materialized (
        select
            row,
            bool_and(not is_galaxy) as is_empty
        from space
        group by row
    ), empty_cols as materialized (
        select
            col,
            bool_and(not is_galaxy) as is_empty
        from space
        group by col
    ), row_shifts as materialized (
        select
            row,
            count(*) filter (where is_empty) over (order by row) as row_shift
        from empty_rows
    ), col_shifts as materialized (
        select
            col,
            count(*) filter (where is_empty) over (order by col) as col_shift
        from empty_cols
    ), shifted_galaxies as materialized (
        select
            row + 999999*row_shift as row,
            col + 999999*col_shift as col
        from
            space
            join row_shifts using
                (row)
            join col_shifts using
                (col)
        where is_galaxy
    )
select sum(abs(first_galaxy.row - second_galaxy.row) + abs(first_galaxy.col - second_galaxy.col)) // 2 as part_two
from
    shifted_galaxies first_galaxy
    cross join shifted_galaxies second_galaxy
;
