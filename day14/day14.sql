create table raw_input as 
select
    column0 as line,
    (count(*) over ()) - (row_number() over () - 1) as row  -- Favorable indexing this time
from read_csv_auto('input.txt', sep=chr(28))
;

with
    grid as materialized (
        select
            row,
            unnest(generate_series(1, len(line))) as col,
            unnest(string_to_array(line, '')) as value
        from raw_input
    ), square_stones as materialized (
        select
            row,
            col
        from grid
        where value = '#'
    ), round_stones as (
        select
            row,
            col
        from grid
        where value = 'O'
    ), moved_round_stones as (
        select
            round_stones.col as col,
            coalesce(
                square_stones.row, 
                (select max(row) + 1 from grid), 
            ) - (
                row_number()
                over (partition by square_stones.row, round_stones.col)
            ) as row
        from
            round_stones
            asof left join square_stones on
                round_stones.col = square_stones.col
                and round_stones.row < square_stones.row
    )
select sum(row) as part_one
from moved_round_stones
;

with recursive
    grid as materialized (
        select
            row,
            unnest(generate_series(1, len(line))) as col,
            unnest(string_to_array(line, '')) as value
        from raw_input
    ), square_stones as materialized (
        select
            row,
            col
        from grid
        where value = '#'
    ), round_stones as (
        select
            row,
            col
        from grid
        where value = 'O'
    ), stone_moving_progress as materialized (
        /*
        Recursive CTEs do not allow arbitrary lookbacks, just into the last iteration.
        Hence, we'll need to copy all the data every time, ending up with O((board*iterations)^2)
        data instead of O(board*iterations).

        We'll add the cycle length to the last iteration to work with it further.
        */
        select
            row,
            col,
            0 as cycle,
            null::int as cycle_length
        from grid
        where value = 'O'
        union all
        (
            with
                latest_cycle as (
                    select
                        row,
                        col,
                        cycle
                    from stone_moving_progress
                    where 
                        cycle = (select max(cycle) from stone_moving_progress)
                        and cycle_length is null
                ), moved_north as (
                    select
                        round_stones.col as col,
                        coalesce(
                            square_stones.row, 
                            (select max(row) + 1 from grid), 
                        ) - (
                            row_number()
                            over (partition by square_stones.row, round_stones.col)
                        ) as row,
                        cycle
                    from
                        latest_cycle as round_stones
                        asof left join square_stones on
                            round_stones.col = square_stones.col
                            and round_stones.row < square_stones.row
                ), moved_west as (
                    select
                        coalesce(
                            square_stones.col,
                            0,
                        ) + (
                            row_number()
                            over (partition by round_stones.row, square_stones.col)
                        ) as col,
                        round_stones.row as row,
                        cycle
                    from
                        moved_north as round_stones
                        asof left join square_stones on
                            round_stones.row = square_stones.row
                            and round_stones.col > square_stones.col
                ), moved_south as (
                    select
                        round_stones.col as col,
                        coalesce(
                            square_stones.row, 
                            0, 
                        ) + (
                            row_number()
                            over (partition by square_stones.row, round_stones.col)
                        ) as row,
                        cycle
                    from
                        moved_west as round_stones
                        asof left join square_stones on
                            round_stones.col = square_stones.col
                            and round_stones.row > square_stones.row
                ), moved_east as materialized (
                    select
                        coalesce(
                            square_stones.col,
                            (select max(col) + 1 from grid),
                        ) - (
                            row_number()
                            over (partition by round_stones.row, square_stones.col)
                        ) as col,
                        round_stones.row as row,
                        cycle + 1 as cycle
                    from
                        moved_south as round_stones
                        asof left join square_stones on
                            round_stones.row = square_stones.row
                            and round_stones.col < square_stones.col
                ), cycle_length as (
                    select any_value(moved_east.cycle) - stone_moving_progress.cycle as cycle_length
                    from
                        stone_moving_progress
                        left join moved_east using
                            (row, col)
                    group by stone_moving_progress.cycle
                    having bool_and(moved_east.row is not null)
                ), progress_with_moved_added as (
                    select * 
                    from stone_moving_progress
                    where cycle_length is null
                    union all
                    select row, col, cycle, null as cycle_length
                    from moved_east
                )
            select
                row,
                col,
                cycle,
                (select cycle_length from cycle_length) as cycle_length
            from progress_with_moved_added
            where cycle_length is null
        )
    ), cycle_data as (
        select
            (select max(cycle) from stone_moving_progress) - (select any_value(cycle_length) from stone_moving_progress) as cycle_start,
            (select any_value(cycle_length) from stone_moving_progress) as cycle_length
    ), billionth_cycle_id as (
        select cycle_start + (1000000000 - cycle_start) % cycle_length as cycle_id
        from cycle_data
    )
select sum(row) as part_two
from stone_moving_progress
where
    cycle_length is not null
    and cycle = (select cycle_id from billionth_cycle_id)
;

/*
To prevent using quadratic memory, we can instead try to
iteratively widen our search horizon by nesting recursive
CTEs. It's quite unholy, but a nice challenge.

It turns out to also be faster for large inputs.
*/

with recursive
    grid as materialized (
        select
            row,
            unnest(generate_series(1, len(line))) as col,
            unnest(string_to_array(line, '')) as value
        from raw_input
    ), square_stones as materialized (
        select
            row,
            col
        from grid
        where value = '#'
    ), round_stones as (
        select
            row,
            col
        from grid
        where value = 'O'
    ), iterative_deepening_progress as (
        select  -- The state is either the last iteration _OR_ the result
            row,
            col,
            0 as cycle,
            2 as max_cycles,
            null as sum_of_rows_after_billion_cycles
        from grid
        where value = 'O'
        union all
        (
            with recursive
                stone_moving_progress as materialized (
                    select
                        row,
                        col,
                        cycle,
                        max_cycles
                    from iterative_deepening_progress
                    where sum_of_rows_after_billion_cycles is null  -- Repeat until we have a solution
                    union all
                    (
                        with
                            moved_north as (
                                select
                                    round_stones.col as col,
                                    coalesce(
                                        square_stones.row, 
                                        (select max(row) + 1 from grid), 
                                    ) - (
                                        row_number()
                                        over (partition by square_stones.row, round_stones.col)
                                    ) as row,
                                    cycle,
                                    max_cycles
                                from
                                    stone_moving_progress as round_stones
                                    asof left join square_stones on
                                        round_stones.col = square_stones.col
                                        and round_stones.row < square_stones.row
                            ), moved_west as (
                                select
                                    coalesce(
                                        square_stones.col,
                                        0,
                                    ) + (
                                        row_number()
                                        over (partition by round_stones.row, square_stones.col)
                                    ) as col,
                                    round_stones.row as row,
                                    cycle,
                                    max_cycles
                                from
                                    moved_north as round_stones
                                    asof left join square_stones on
                                        round_stones.row = square_stones.row
                                        and round_stones.col > square_stones.col
                            ), moved_south as (
                                select
                                    round_stones.col as col,
                                    coalesce(
                                        square_stones.row, 
                                        0, 
                                    ) + (
                                        row_number()
                                        over (partition by square_stones.row, round_stones.col)
                                    ) as row,
                                    cycle,
                                    max_cycles
                                from
                                    moved_west as round_stones
                                    asof left join square_stones on
                                        round_stones.col = square_stones.col
                                        and round_stones.row > square_stones.row
                            ), moved_east as materialized (
                                select
                                    coalesce(
                                        square_stones.col,
                                        (select max(col) + 1 from grid),
                                    ) - (
                                        row_number()
                                        over (partition by round_stones.row, square_stones.col)
                                    ) as col,
                                    round_stones.row as row,
                                    cycle,
                                    max_cycles
                                from
                                    moved_south as round_stones
                                    asof left join square_stones on
                                        round_stones.row = square_stones.row
                                        and round_stones.col < square_stones.col
                            )
                        select
                            row,
                            col,
                            cycle + 1 as cycle,
                            max_cycles
                        from moved_east
                        where cycle < max_cycles
                    )
                ), last_iteration as materialized (
                    select *
                    from stone_moving_progress
                    where cycle = max_cycles
                ), all_detected_cycles as (
                    select
                        stone_moving_progress.cycle as cycle_start,
                        any_value(last_iteration.cycle) - stone_moving_progress.cycle as cycle_length
                    from
                        stone_moving_progress
                        left join last_iteration using
                            (row, col)
                    where stone_moving_progress.cycle < stone_moving_progress.max_cycles
                    group by stone_moving_progress.cycle
                    having bool_and(last_iteration.row is not null)
                ), shortest_detected_cycle as (
                    select
                        arg_min(cycle_start, cycle_length) as cycle_start,
                        min(cycle_length) as cycle_length
                    from all_detected_cycles
                ), sum_of_rows_for_billionth_iteration as (
                    select sum(row) as sum_of_rows
                    from stone_moving_progress
                    where cycle = (
                        select cycle_start + (1000000000 - cycle_start) % cycle_length
                        from shortest_detected_cycle
                    )
                )
            select
                row,
                col,
                cycle,
                max_cycles*2 as max_cycles,
                sum_of_rows_for_billionth_iteration.sum_of_rows
            from 
                last_iteration
                left join sum_of_rows_for_billionth_iteration on true
        )
    )
select any_value(sum_of_rows_after_billion_cycles) as part_two
from iterative_deepening_progress
;
