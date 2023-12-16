create table raw_input as 
select
    column0 as line,
    row_number() over () - 1 as row
from read_csv_auto('input.txt', sep=chr(28))
;

create type direction as enum ('up', 'right', 'down', 'left'); 

with recursive
    grid as materialized (
        select
            row,
            unnest(generate_series(1, len(line))) - 1 as col,
            unnest(string_to_array(line, '')) as tile
        from raw_input
    ), grid_bounds as materialized (
        select 
            max(row) as max_row,
            max(col) as max_col
        from grid
    ), bouncing_beams as (
        select
            0 as row,
            0 as col,
            'right'::direction as direction
        union
        (
            with
                new_direction as (
                    select
                        row,
                        col,
                        case
                            when direction = 'right' then
                                case
                                    when tile = '.' then 'right'
                                    when tile = '-' then 'right'
                                    when tile = '|' then unnest(['up', 'down'])
                                    when tile = '\' then 'down'
                                    when tile = '/' then 'up'
                                end
                            when direction = 'up' then
                                case
                                    when tile = '.' then 'up'
                                    when tile = '-' then unnest(['left', 'right'])
                                    when tile = '|' then 'up'
                                    when tile = '\' then 'left'
                                    when tile = '/' then 'right'
                                end
                            when direction = 'left' then
                                case
                                    when tile = '.' then 'left'
                                    when tile = '-' then 'left'
                                    when tile = '|' then unnest(['up', 'down'])
                                    when tile = '\' then 'up'
                                    when tile = '/' then 'down'
                                end
                            when direction = 'down' then
                                case
                                    when tile = '.' then 'down'
                                    when tile = '-' then unnest(['left', 'right'])
                                    when tile = '|' then 'down'
                                    when tile = '\' then 'right'
                                    when tile = '/' then 'left'
                                end
                        end as direction
                    from
                        bouncing_beams
                        join grid using
                            (row, col)
                )
            select
                case
                    when direction = 'down' then
                        row + 1
                    when direction = 'up' then
                        row - 1
                    else
                        row
                end as row,
                case
                    when direction = 'left' then
                        col - 1
                    when direction = 'right' then
                        col + 1
                    else
                        col
                end as col,
                direction
            from new_direction
        )
    ), energized_tiles as (
        select distinct
            row,
            col
        from bouncing_beams, grid_bounds
        where 
            row >= 0
            and row <= max_row
            and col >= 0
            and col <= max_col
    )
select count(*) as part_one
from energized_tiles
;

with recursive
    grid as materialized (
        select
            row,
            unnest(generate_series(1, len(line))) - 1 as col,
            unnest(string_to_array(line, '')) as tile
        from raw_input
    ), grid_bounds as materialized (
        select 
            max(row) as max_row,
            max(col) as max_col
        from grid
    ), initial_positions as (
        select
            0 as row,
            unnest(generate_series(0, max_col)) as col,
            'down'::direction as direction
        from grid_bounds
        union all
        select
            max_row as row,
            unnest(generate_series(0, max_col)) as col,
            'up'::direction as direction
        from grid_bounds
        union all
        select
            unnest(generate_series(0, max_row)) as row,
            0 as col,
            'right'::direction as direction
        from grid_bounds
        union all
        select
            unnest(generate_series(0, max_row)) as row,
            max_col as col,
            'left'::direction as direction
        from grid_bounds
    ), bouncing_beams as (
        select 
            row,
            col,
            direction,
            direction::text || row || ',' || col as origin_id
        from initial_positions
        union
        (
            with
                new_direction as (
                    select
                        row,
                        col,
                        case
                            when direction = 'right' then
                                case
                                    when tile = '.' then 'right'
                                    when tile = '-' then 'right'
                                    when tile = '|' then unnest(['up', 'down'])
                                    when tile = '\' then 'down'
                                    when tile = '/' then 'up'
                                end
                            when direction = 'up' then
                                case
                                    when tile = '.' then 'up'
                                    when tile = '-' then unnest(['left', 'right'])
                                    when tile = '|' then 'up'
                                    when tile = '\' then 'left'
                                    when tile = '/' then 'right'
                                end
                            when direction = 'left' then
                                case
                                    when tile = '.' then 'left'
                                    when tile = '-' then 'left'
                                    when tile = '|' then unnest(['up', 'down'])
                                    when tile = '\' then 'up'
                                    when tile = '/' then 'down'
                                end
                            when direction = 'down' then
                                case
                                    when tile = '.' then 'down'
                                    when tile = '-' then unnest(['left', 'right'])
                                    when tile = '|' then 'down'
                                    when tile = '\' then 'right'
                                    when tile = '/' then 'left'
                                end
                        end as direction,
                        origin_id
                    from
                        bouncing_beams
                        join grid using
                            (row, col)
                )
            select
                case
                    when direction = 'down' then
                        row + 1
                    when direction = 'up' then
                        row - 1
                    else
                        row
                end as row,
                case
                    when direction = 'left' then
                        col - 1
                    when direction = 'right' then
                        col + 1
                    else
                        col
                end as col,
                direction,
                origin_id
            from new_direction
        )
    ), energized_tiles as (
        select distinct
            origin_id,
            row,
            col
        from bouncing_beams, grid_bounds
        where 
            row >= 0
            and row <= max_row
            and col >= 0
            and col <= max_col
    ), energized_tile_counts as (
        select
            origin_id,
            count(*) as count
        from energized_tiles
        group by origin_id
    )
select max(count) as part_two
from energized_tile_counts
;