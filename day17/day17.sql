create table raw_input as 
select
    column0 as line,
    row_number() over () - 1 as row
from read_csv_auto('test-input.txt', sep=chr(28))
;

with recursive
    grid as materialized (
        select
            row,
            unnest(generate_series(1, len(line))) - 1 as col,
            unnest(string_to_array(line, ''))::int as heat_level
        from raw_input
    ), path_finding_progress as (
        select
            {
                'row': 0,
                'col': 0,
                'row_direction': 0,
                'col_direction': 0,
                'traveled_in_direction': 0
            } as state,
            0 as total_heat,
            []::struct("row" bigint, col bigint, row_direction bigint, col_direction bigint, traveled_in_direction integer)[] as path,
            max(row) as final_row,
            max(col) as final_col,
            false as end_reached
        from grid
        union all
        (
            with
                all_neighbors as (
                    select
                        {
                            'row': grid.row,
                            'col': grid.col,
                            'row_direction': grid.row - progress.state.row,
                            'col_direction': grid.col - progress.state.col,
                            'traveled_in_direction': 
                                case
                                    when grid.row - progress.state.row = progress.state.row_direction and grid.col - progress.state.col = progress.state.col_direction then
                                        progress.state.traveled_in_direction + 1
                                    else
                                        1
                                end
                        } as state,
                        total_heat + heat_level as total_heat,
                        path,
                        final_row,
                        final_col,
                        end_reached
                    from
                        path_finding_progress progress
                        join grid on
                            (grid.col = progress.state.col and abs(grid.row - progress.state.row) = 1)
                            or (grid.row = progress.state.row and abs(grid.col - progress.state.col) = 1)
                    where not progress.end_reached
                )
            select distinct on (state)
                state,
                total_heat,
                array_append(path, state) as path,
                final_row,
                final_col,
                max(state.row = final_row and state.col = final_col) over () as end_reached
            from
                all_neighbors
            where 
                not exists(
                    select 1
                    from path_finding_progress as progress
                    where list_contains(progress.path, all_neighbors.state)
                ) 
                and state.traveled_in_direction <= 3
            order by
                state,
                total_heat
        )
    ), best_path_to_end as (
        select 
            min(total_heat) as total_heat,
            arg_min(path, total_heat) as path
        from path_finding_progress
        where state.row = final_row and state.col = final_col
    )
select total_heat, unnest(path)
from best_path_to_end
;

