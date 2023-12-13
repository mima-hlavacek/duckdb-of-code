create table raw_input as 
select
    column0 as line,
    row_number() over () - 1 as row
from read_csv_auto('input.txt', sep=chr(28))
;

with
    numbered_cases as (
        select
            count(*) filter (where line is null) over (order by row) as case_id,
            line,
            row
        from raw_input
        qualify line is not null
    ), cases_with_row_numbers as materialized (
        select
            case_id,
            line,
            row_number() over (partition by case_id order by row) - 1 as row
        from numbered_cases
    ), individual_cells as ( -- Let's transpose the cases to do column matching the same way as row matching 
        select
            case_id,
            unnest(string_to_array(line, '')) as cell,
            row,
            unnest(generate_series(1, len(line))) - 1 as col
        from cases_with_row_numbers
    ), transposed_cases_with_row_numbers as (
        select
            case_id,
            string_agg(cell, '') as line,
            col as row,
        from individual_cells
        group by 
            case_id,
            col
    ), cases_to_match as materialized (
        select
            case_id,
            100 as multiplier,
            row,
            line
        from cases_with_row_numbers
        union all
        select
            case_id,
            1 as multiplier,
            row,
            line
        from transposed_cases_with_row_numbers
    ), cases_row_counts as (
        select
            case_id,
            multiplier,
            count(*) as row_count
        from cases_to_match
        group by
            case_id,
            multiplier
    ), possible_reflection_points as (
        select
            case_id,
            multiplier,
            unnest(generate_series(1, row_count - 1)) as reflected_lines,
            row_count
        from cases_row_counts
    ), perfect_reflections as (
        select
            reflection_line.case_id,
            reflection_line.multiplier,
            reflection_line.reflected_lines
        from
            possible_reflection_points as reflection_line
            join cases_to_match as original on
                reflection_line.case_id = original.case_id
                and reflection_line.multiplier = original.multiplier
                and reflection_line.reflected_lines > original.row
            join cases_to_match as reflected on
                reflection_line.case_id = reflected.case_id
                and reflection_line.multiplier = reflected.multiplier
                and reflected.row = reflection_line.reflected_lines  + (reflection_line.reflected_lines - original.row - 1)
        group by
            reflection_line.case_id,
            reflection_line.multiplier,
            reflection_line.reflected_lines
        having bool_and(original.line = reflected.line)
    )
select sum(multiplier * reflected_lines) as part_one 
from perfect_reflections
;

with
    numbered_cases as (
        select
            count(*) filter (where line is null) over (order by row) as case_id,
            line,
            row
        from raw_input
        qualify line is not null
    ), cases_with_row_numbers as materialized (
        select
            case_id,
            line,
            row_number() over (partition by case_id order by row) - 1 as row
        from numbered_cases
    ), individual_cells as ( -- Let's transpose the cases to do column matching the same way as row matching 
        select
            case_id,
            unnest(string_to_array(line, '')) as cell,
            row,
            unnest(generate_series(1, len(line))) - 1 as col
        from cases_with_row_numbers
    ), transposed_cases_with_row_numbers as (
        select
            case_id,
            string_agg(cell, '') as line,
            col as row,
        from individual_cells
        group by 
            case_id,
            col
    ), cases_to_match as materialized (
        select
            case_id,
            100 as multiplier,
            row,
            line
        from cases_with_row_numbers
        union all
        select
            case_id,
            1 as multiplier,
            row,
            line
        from transposed_cases_with_row_numbers
    ), cases_row_counts as (
        select
            case_id,
            multiplier,
            count(*) as row_count
        from cases_to_match
        group by
            case_id,
            multiplier
    ), possible_reflection_points as (
        select
            case_id,
            multiplier,
            unnest(generate_series(1, row_count - 1)) as reflected_lines,
            row_count
        from cases_row_counts
    ), smudged_reflections as (
        select
            reflection_line.case_id,
            reflection_line.multiplier,
            reflection_line.reflected_lines
        from
            possible_reflection_points as reflection_line
            join cases_to_match as original on
                reflection_line.case_id = original.case_id
                and reflection_line.multiplier = original.multiplier
                and reflection_line.reflected_lines > original.row
            join cases_to_match as reflected on
                reflection_line.case_id = reflected.case_id
                and reflection_line.multiplier = reflected.multiplier
                and reflected.row = reflection_line.reflected_lines  + (reflection_line.reflected_lines - original.row - 1)
        group by
            reflection_line.case_id,
            reflection_line.multiplier,
            reflection_line.reflected_lines
        having sum(hamming(original.line, reflected.line)) = 1
    )
select sum(multiplier * reflected_lines) as part_two 
from smudged_reflections
;
