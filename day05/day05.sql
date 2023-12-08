create table raw_inputs as 
select
    column0 as line,
    row_number() over () - 1 as row
from read_csv_auto('input.txt', sep=chr(28))
;

create table mapping_stages as (
with
    stage_numbers as (
        select
            line,
            row,
            count(*) filter (where line is null) over (order by row) as stage_number
        from raw_inputs
    )
select
    split_part(line, ' ', 1)::int128 as destination_range_start,
    split_part(line, ' ', 2)::int128 as source_range_start,
    split_part(line, ' ', 3)::int128 as range_length,
    stage_number
from stage_numbers
where stage_number > 0
qualify row_number() over (partition by stage_number order by row) >= 3
)
;

with recursive
    mapping_progress as (
        select
            0 as stage_number,
            unnest(regexp_extract_all(line, '\d+'))::int128 as value
        from raw_inputs
        where row = 0
        union all
        select
            mapping_progress.stage_number + 1 as stage_number,
            coalesce(
                mapping_progress.value - mapping_stages.source_range_start + mapping_stages.destination_range_start,
                mapping_progress.value
            ) as value
        from
            mapping_progress
            left join mapping_stages on
                mapping_progress.stage_number + 1 = mapping_stages.stage_number
                and mapping_progress.value between mapping_stages.source_range_start and mapping_stages.source_range_start + mapping_stages.range_length - 1
        where mapping_progress.stage_number < (select max(stage_number) from mapping_stages)
    )
select min(value) as part_one 
from mapping_progress
where stage_number = (select max(stage_number) from mapping_stages);

with recursive
    mapping_progress as (
        select
            0 as stage_number,
            unnest((regexp_extract_all(line, '\d+'))[:-:2])::int128 as range_start,
            unnest((regexp_extract_all(line, '\d+'))[2:-:2])::int128 as range_length
        from raw_inputs
        where row = 0
        union all
        (
            with
                raw_mapped_ranges as (
                    select
                        mapping_progress.stage_number + 1 as stage_number,
                        mapping_progress.range_start as original_range_start,
                        mapping_progress.range_length as original_range_length,
                        coalesce(
                            greatest(mapping_progress.range_start, mapping_stages.source_range_start) - mapping_stages.source_range_start + mapping_stages.destination_range_start,
                            mapping_progress.range_start
                        ) as mapped_range_start,
                        coalesce(
                            least(mapping_progress.range_start + mapping_progress.range_length, mapping_stages.source_range_start + mapping_stages.range_length) - greatest(mapping_progress.range_start, mapping_stages.source_range_start),
                            mapping_progress.range_length
                        ) as range_length,
                        coalesce(
                            greatest(mapping_progress.range_start, mapping_stages.source_range_start),
                            mapping_progress.range_start
                        ) as source_range_start
                    from
                        mapping_progress
                        left join mapping_stages on
                            mapping_progress.stage_number + 1 = mapping_stages.stage_number
                            and mapping_progress.range_start < mapping_stages.source_range_start + mapping_stages.range_length
                            and mapping_stages.source_range_start < mapping_progress.range_start + mapping_progress.range_length
                    where mapping_progress.stage_number < (select max(stage_number) from mapping_stages)
                ), filled_holes_between_mapped_ranges as (
                    select
                        stage_number,
                        source_range_start + range_length as range_start,
                        (lead(source_range_start) over (partition by original_range_start, original_range_length order by source_range_start)) - (source_range_start + range_length) as range_length
                    from raw_mapped_ranges
                    qualify (lead(source_range_start) over (partition by original_range_start, original_range_length order by source_range_start)) > (source_range_start + range_length)
                ), filled_hole_before_mapped_ranges as (
                    select
                        stage_number,
                        original_range_start as range_start,
                        min(source_range_start) - original_range_start as range_length
                    from raw_mapped_ranges
                    group by
                        stage_number,
                        original_range_start,
                        original_range_length
                    having min(source_range_start) > original_range_start
                ), filled_hole_after_mapped_ranges as (
                    select
                        stage_number,
                        max(source_range_start + range_length) as range_start,
                        original_range_length - (max(source_range_start + range_length) - original_range_start) as range_length
                    from raw_mapped_ranges
                    group by
                        stage_number,
                        original_range_start,
                        original_range_length
                    having max(source_range_start + range_length) < original_range_start + original_range_length
                )
            select
                stage_number,
                mapped_range_start as range_start,
                range_length
            from raw_mapped_ranges
            union
            select *
            from filled_holes_between_mapped_ranges
            union
            select *
            from filled_hole_before_mapped_ranges
            union
            select *
            from filled_hole_after_mapped_ranges
        )
    )
select min(range_start) as part_one
from mapping_progress
where stage_number = (select max(stage_number) from mapping_stages)
;