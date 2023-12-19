create table raw_input as 
select
    line,
    row_number() over () - 1 as row
from read_csv('input.txt', sep=chr(28), columns={'line': 'text'})
;

.timer on 

create type direction as enum ('up', 'right', 'left', 'down');

-- Create explicit edges to help with our pathfinding endeavors.
-- State space is ROWS x COLUMNS x DIRECTIONS (we came from)
with recursive
    grid as materialized (
        select
            row,
            unnest(generate_series(1, len(line))) - 1 as col,
            unnest(string_to_array(line, ''))::int as heat_level
        from raw_input
    ), edges_right as (
        select
            {
                'row': src.row,
                'col': src.col,
                'direction': unnest(['up', 'down']::direction[])
            } as src,
            {
                'row': dst.row,
                'col': dst.col,
                'direction': 'right'::direction
            } as dst,
            sum(dst.heat_level) over (partition by src.row, src.col order by dst.col) as heat_level
        from
            grid src
            join grid dst on
                src.row = dst.row
                and src.col < dst.col
                and dst.col <= src.col + 3
    ), edges_left as (
        select
            {
                'row': src.row,
                'col': src.col,
                'direction': unnest(['up', 'down']::direction[])
            } as src,
            {
                'row': dst.row,
                'col': dst.col,
                'direction': 'left'::direction
            } as dst,
            sum(dst.heat_level) over (partition by src.row, src.col order by dst.col desc) as heat_level
        from
            grid src
            join grid dst on
                src.row = dst.row
                and src.col > dst.col
                and dst.col >= src.col - 3
    ), edges_up as (
        select
            {
                'row': src.row,
                'col': src.col,
                'direction': unnest(['left', 'right']::direction[])
            } as src,
            {
                'row': dst.row,
                'col': dst.col,
                'direction': 'up'::direction
            } as dst,
            sum(dst.heat_level) over (partition by src.row, src.col order by dst.row desc) as heat_level
        from
            grid src
            join grid dst on
                src.col = dst.col
                and src.row > dst.row
                and dst.row >= src.row - 3
    ), edges_down as (
        select
            {
                'row': src.row,
                'col': src.col,
                'direction': unnest(['left', 'right']::direction[])
            } as src,
            {
                'row': dst.row,
                'col': dst.col,
                'direction': 'down'::direction
            } as dst,
            sum(dst.heat_level) over (partition by src.row, src.col order by dst.row) as heat_level
        from
            grid src
            join grid dst on
                src.col = dst.col
                and src.row < dst.row
                and dst.row <= src.row + 3
    ), edges as materialized (    
        select * from edges_right
        union all
        select * from edges_left
        union all
        select * from edges_up
        union all
        select * from edges_down
    ), path_finding_progress as (
        select
            {
                'row': 0,
                'col': 0,
                'direction': unnest(['down', 'right']::direction[]),
            } as state,
            0 as best_cost,
            true as is_active,
            max(row) as final_row,
            max(col) as final_col,
            null as best_target_cost
        from grid
        union all
        (
            with
                new_best_costs as materialized (
                    select distinct on (edges.dst)
                        edges.dst as state,
                        progress.best_cost + edges.heat_level as best_cost,
                        coalesce(progress.best_cost + edges.heat_level < progress.best_target_cost, true) as is_active,
                        progress.final_row,
                        progress.final_col
                    from
                        path_finding_progress progress
                        join edges on
                            progress.state = edges.src
                    where progress.is_active
                    order by
                        edges.dst,
                        progress.best_cost + edges.heat_level
                ), added_best_costs as (
                    select *
                    from
                        new_best_costs new
                        anti join path_finding_progress progress on
                            new.state = progress.state
                ), improved_best_costs as (
                    select
                        progress.state,
                        least(progress.best_cost, new.best_cost) as best_cost,
                        new.best_cost is not null and new.best_cost < progress.best_cost as is_active,
                        progress.final_row,
                        progress.final_col
                    from
                        path_finding_progress progress
                        left join new_best_costs new on
                            progress.state = new.state
                ), all_costs as (
                    select *
                    from added_best_costs
                    union all
                    select *
                    from improved_best_costs
                )
            select 
                *,
                min(best_cost) filter (where state.row = final_row and state.col = final_col) over () as best_target_cost
            from all_costs
            qualify bool_or(is_active) over ()
        )
    )
select min(best_target_cost) as part_one
from path_finding_progress
;


with recursive
    grid as materialized (
        select
            row,
            unnest(generate_series(1, len(line))) - 1 as col,
            unnest(string_to_array(line, ''))::int as heat_level
        from raw_input
    ), edges_right as (
        select
            {
                'row': src.row,
                'col': src.col,
                'direction': unnest(['up', 'down']::direction[])
            } as src,
            {
                'row': dst.row,
                'col': dst.col,
                'direction': 'right'::direction
            } as dst,
            sum(dst.heat_level) over (partition by src.row, src.col order by dst.col) as heat_level
        from
            grid src
            join grid dst on
                src.row = dst.row
                and src.col < dst.col
                and dst.col <= src.col + 10
    ), edges_left as (
        select
            {
                'row': src.row,
                'col': src.col,
                'direction': unnest(['up', 'down']::direction[])
            } as src,
            {
                'row': dst.row,
                'col': dst.col,
                'direction': 'left'::direction
            } as dst,
            sum(dst.heat_level) over (partition by src.row, src.col order by dst.col desc) as heat_level
        from
            grid src
            join grid dst on
                src.row = dst.row
                and src.col > dst.col
                and dst.col >= src.col - 10
    ), edges_up as (
        select
            {
                'row': src.row,
                'col': src.col,
                'direction': unnest(['left', 'right']::direction[])
            } as src,
            {
                'row': dst.row,
                'col': dst.col,
                'direction': 'up'::direction
            } as dst,
            sum(dst.heat_level) over (partition by src.row, src.col order by dst.row desc) as heat_level
        from
            grid src
            join grid dst on
                src.col = dst.col
                and src.row > dst.row
                and dst.row >= src.row - 10
    ), edges_down as (
        select
            {
                'row': src.row,
                'col': src.col,
                'direction': unnest(['left', 'right']::direction[])
            } as src,
            {
                'row': dst.row,
                'col': dst.col,
                'direction': 'down'::direction
            } as dst,
            sum(dst.heat_level) over (partition by src.row, src.col order by dst.row) as heat_level
        from
            grid src
            join grid dst on
                src.col = dst.col
                and src.row < dst.row
                and dst.row <= src.row + 10
    ), all_edges as (
        select * from edges_right
        union all
        select * from edges_left
        union all
        select * from edges_up
        union all
        select * from edges_down
    ), edges as materialized (
        select *
        from all_edges
        where
            abs(src.row - dst.row) >= 4
            or abs(src.col - dst.col) >= 4
    ), path_finding_progress as (
        select
            {
                'row': 0,
                'col': 0,
                'direction': unnest(['down', 'right']::direction[]),
            } as state,
            0 as best_cost,
            true as is_active,
            max(row) as final_row,
            max(col) as final_col,
            null as best_target_cost
        from grid
        union all
        (
            with
                new_best_costs as materialized (
                    select distinct on (edges.dst)
                        edges.dst as state,
                        progress.best_cost + edges.heat_level as best_cost,
                        coalesce(progress.best_cost + edges.heat_level < progress.best_target_cost, true) as is_active,
                        progress.final_row,
                        progress.final_col
                    from
                        path_finding_progress progress
                        join edges on
                            progress.state = edges.src
                    where progress.is_active
                    order by
                        edges.dst,
                        progress.best_cost + edges.heat_level
                ), added_best_costs as (
                    select *
                    from
                        new_best_costs new
                        anti join path_finding_progress progress on
                            new.state = progress.state
                ), improved_best_costs as (
                    select
                        progress.state,
                        least(progress.best_cost, new.best_cost) as best_cost,
                        new.best_cost is not null and new.best_cost < progress.best_cost as is_active,
                        progress.final_row,
                        progress.final_col
                    from
                        path_finding_progress progress
                        left join new_best_costs new on
                            progress.state = new.state
                ), all_costs as (
                    select *
                    from added_best_costs
                    union all
                    select *
                    from improved_best_costs
                )
            select 
                *,
                min(best_cost) filter (where state.row = final_row and state.col = final_col) over () as best_target_cost
            from all_costs
            qualify bool_or(is_active) over ()
        )
    )
select min(best_target_cost) as part_two
from path_finding_progress
;
