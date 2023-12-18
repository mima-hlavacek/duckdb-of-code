create table raw_input as 
select
    line,
    row_number() over () - 1 as row
from read_csv('input.txt', sep=chr(28), columns={'line': 'text'})
;

-- :'(
.timer on 

create type direction as enum ('up', 'right', 'left', 'down');

create table grid as 
select
    row,
    unnest(generate_series(1, len(line))) - 1 as col,
    unnest(string_to_array(line, ''))::int as heat_level
from raw_input
;

/*

-- Create explicit edges to help with our pathfinding endeavors.
-- State space is ROWS x COLUMNS x DIRECTIONS (we came from)
create table edges as (
with
    edges_right as (
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
    )    
select * from edges_right
union all
select * from edges_left
union all
select * from edges_up
union all
select * from edges_down
)
;

with recursive
    path_finding_progress as (
        select
            {
                'row': 0,
                'col': 0,
                'direction': unnest(['down', 'right']::direction[]),
            } as state,
            0 as total_heat,
            [
                { 
                    'state': 
                        {
                            'row': 0,
                            'col': 0,
                            'direction': unnest(['down', 'right']::direction[]),
                        },
                    'cost': 0
                }
            ] as visited_list, -- Recursive queries can't refer to the whole history of computation, so we have to carry the visited list with us
            max(row) as final_row,
            max(col) as final_col,
            null as best_final_cost
        from grid  -- To get to final row/col
        union all
        (
            with
                visited_nodes_with_duplicates as (
                    select
                        unnest(visited_list).state as state,
                        unnest(visited_list).cost as cost
                    from path_finding_progress
                ), visited_nodes as (
                    select
                        state,
                        min(cost) as cost
                    from visited_nodes_with_duplicates
                    group by state
                )
            select distinct on (edges.dst)
                edges.dst as state,
                progress.total_heat + edges.heat_level as total_heat,
                array_append(progress.visited_list, {'state': edges.dst, 'cost': progress.total_heat + edges.heat_level}) as visited_list,
                final_row,
                final_col,
                min(  -- Update the best cost for the target node
                    least(
                        progress.best_final_cost,
                        case
                            when edges.dst.row = final_row and edges.dst.col = final_col then
                                progress.total_heat + edges.heat_level
                        end
                    )
                ) over () as best_final_cost
            from
                path_finding_progress progress
                join edges on
                    progress.state = edges.src
                anti join visited_nodes on  -- We were not there with a better score
                    edges.dst = visited_nodes.state
                    and visited_nodes.cost <= progress.total_heat + edges.heat_level
            where
                progress.best_final_cost is null
                or progress.total_heat + edges.heat_level < progress.best_final_cost  -- Cannot reach the end with better cost, terminate
            order by
                edges.dst,
                progress.total_heat + edges.heat_level
        )
    )
select min(total_heat) as part_one
from path_finding_progress
where
    state.row = final_row
    and state.col = final_col
;
*/

-- Part two

create or replace table edges as (
with
    edges_right as (
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
    )
select *
from all_edges
where
    abs(src.row - dst.row) >= 4
    or abs(src.col - dst.col) >= 4
)
;

with recursive
    path_finding_progress as (
        select
            {
                'row': 0,
                'col': 0,
                'direction': unnest(['down', 'right']::direction[]),
            } as state,
            0 as total_heat,
            [
                { 
                    'state': 
                        {
                            'row': 0,
                            'col': 0,
                            'direction': unnest(['down', 'right']::direction[]),
                        },
                    'cost': 0
                }
            ] as visited_list, -- Recursive queries can't refer to the whole history of computation, so we have to carry the visited list with us
            max(row) as final_row,
            max(col) as final_col,
            null as best_final_cost
        from grid  -- To get to final row/col
        union all
        (
            with
                visited_nodes_with_duplicates as (
                    select
                        unnest(visited_list).state as state,
                        unnest(visited_list).cost as cost
                    from path_finding_progress
                ), visited_nodes as (
                    select
                        state,
                        min(cost) as cost
                    from visited_nodes_with_duplicates
                    group by state
                )
            select distinct on (edges.dst)
                edges.dst as state,
                progress.total_heat + edges.heat_level as total_heat,
                array_append(progress.visited_list, {'state': edges.dst, 'cost': progress.total_heat + edges.heat_level}) as visited_list,
                final_row,
                final_col,
                min(  -- Update the best cost for the target node
                    least(
                        progress.best_final_cost,
                        case
                            when edges.dst.row = final_row and edges.dst.col = final_col then
                                progress.total_heat + edges.heat_level
                        end
                    )
                ) over () as best_final_cost
            from
                path_finding_progress progress
                join edges on
                    progress.state = edges.src
                anti join visited_nodes on  -- We were not there with a better score
                    edges.dst = visited_nodes.state
                    and visited_nodes.cost <= progress.total_heat + edges.heat_level
            where
                progress.best_final_cost is null
                or progress.total_heat + edges.heat_level < progress.best_final_cost  -- Cannot reach the end with better cost, terminate
            order by
                edges.dst,
                progress.total_heat + edges.heat_level
        )
    )
select min(total_heat) as part_one
from path_finding_progress
where
    state.row = final_row
    and state.col = final_col
;
