create table raw_input as 
select
    column0 as line,
    row_number() over () - 1 as row
from read_csv_auto('input.txt', sep=chr(28))
;

create table grid as
select
    unnest(string_to_array(line, '')) as pipe,
    row,
    unnest(generate_series(1, len(line))) - 1 as col
from raw_input
;

create table pipes(
    direction_before struct(x int64, y int64), 
    pipe text, 
    direction_after struct(x int64, y int64)
);

insert into pipes values
    ({'x': 1, 'y': 0}, '-', {'x': 1, 'y': 0}),
    ({'x': 1, 'y': 0}, 'J', {'x': 0, 'y': -1}),
    ({'x': 1, 'y': 0}, '7', {'x': 0, 'y': 1}),
    ({'x': -1, 'y': 0}, '-', {'x': -1, 'y': 0}),
    ({'x': -1, 'y': 0}, 'L', {'x': 0, 'y': -1}),
    ({'x': -1, 'y': 0}, 'F', {'x': 0, 'y': 1}),
    ({'x': 0, 'y': 1}, '|', {'x': 0, 'y': 1}),
    ({'x': 0, 'y': 1}, 'L', {'x': 1, 'y': 0}),
    ({'x': 0, 'y': 1}, 'J', {'x': -1, 'y': 0}),
    ({'x': 0, 'y': -1}, '|', {'x': 0, 'y': -1}),
    ({'x': 0, 'y': -1}, 'F', {'x': 1, 'y': 0}),
    ({'x': 0, 'y': -1}, '7', {'x': -1, 'y': 0})
;

with recursive
    maze_traversal as (
        select
            row,
            col,
            unnest([{'x': 1, 'y': 0}, {'x': -1, 'y': 0}, {'x': 0, 'y': 1}, {'x': 0, 'y': -1}])::struct(x int64, y int64) as direction,
            0 as distance
        from grid
        where pipe = 'S'
        union all 
        select
            grid.row,
            grid.col,
            direction_after,
            distance + 1
        from
            maze_traversal
            join grid on
                maze_traversal.row + maze_traversal.direction.y = grid.row
                and maze_traversal.col + maze_traversal.direction.x = grid.col
            join pipes on
                grid.pipe = pipes.pipe
                and maze_traversal.direction = pipes.direction_before
        where (select count(*) from (select distinct row, col from maze_traversal where distance > 1)) != 1
    )
select max(distance) as part_one
from maze_traversal
;

with recursive
    maze_traversal as (
        select
            row,
            col,
            unnest([{'x': 1, 'y': 0}, {'x': -1, 'y': 0}, {'x': 0, 'y': 1}, {'x': 0, 'y': -1}])::struct(x int64, y int64) as direction,
            0 as distance
        from grid
        where pipe = 'S'
        union all 
        select
            grid.row,
            grid.col,
            direction_after,
            distance + 1
        from
            maze_traversal
            join grid on
                maze_traversal.row + maze_traversal.direction.y = grid.row
                and maze_traversal.col + maze_traversal.direction.x = grid.col
            join pipes on
                grid.pipe = pipes.pipe
                and maze_traversal.direction = pipes.direction_before
        where (select count(*) from (select distinct row, col from maze_traversal where distance > 1)) != 1
    ), loop as materialized (
        select distinct
            row,
            col
        from maze_traversal
    ), vertical_loop_crossings as materialized (  -- Let's hope we won't have to figure out what 'S' is :D
        select
            row,
            col
        from
            loop
            join grid using
                (row, col)
        where pipe in ('J', '7', '|', 'F', 'L')
        qualify
            not (pipe = 'L' and (lead(pipe) over (partition by row order by col)) = 'J')
            and not (pipe = 'F' and (lead(pipe) over (partition by row order by col)) = '7')
            and pipe in ('L', 'F', '|')
    ), non_loop_tiles_possibly_inside as (
        select
            row,
            col
        from
            grid
            anti join loop whole_loop using
                (row, col)
    ), loop_crossings as (
        select
            non_loop_tiles_possibly_inside.row,
            non_loop_tiles_possibly_inside.col,
            count(*) as vertical_crossings
        from 
            non_loop_tiles_possibly_inside
            join vertical_loop_crossings on
                non_loop_tiles_possibly_inside.row = vertical_loop_crossings.row
                and non_loop_tiles_possibly_inside.col > vertical_loop_crossings.col
        group by
            non_loop_tiles_possibly_inside.row,
            non_loop_tiles_possibly_inside.col
    )
select count(*) as part_two
from loop_crossings
where vertical_crossings % 2 = 1
;