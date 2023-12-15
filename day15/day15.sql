create table raw_input as 
select
    column0 as line,
    row_number() over () - 1 as row
from read_csv_auto('input.txt', sep=chr(28))
;

with recursive
    individual_steps as (
        select unnest(string_to_array(line, ',')) as step
        from raw_input
    ), hash_computation_progress as (
        select
            step as to_process,
            0 as current_value
        from individual_steps
        union all
        select
            to_process[2:] as to_process,
            ((current_value + ascii(to_process[1]))*17) % 256
        from hash_computation_progress
        where to_process != ''
    )
select sum(current_value) as part_one 
from hash_computation_progress
where to_process = ''
; 

with recursive
    individual_steps as (
        select 
            unnest(string_to_array(line, ',')) as step,
            unnest(generate_series(1, len(string_to_array(line, ',')))) as step_order
        from raw_input
    ), hash_computation_progress as (
        select
            regexp_extract(step, '[a-z]+') as to_process,
            step_order,
            0 as current_value
        from individual_steps
        union all
        select
            to_process[2:] as to_process,
            step_order,
            ((current_value + ascii(to_process[1]))*17) % 256
        from hash_computation_progress
        where to_process != ''
    ), steps_with_box_ids as materialized (
        select
            regexp_extract(step, '[a-z]+') as label,
            regexp_extract(step, '=|-') as operation,
            nullif(regexp_extract(step, '\d+'), '')::int as argument,
            step_order,
            current_value as box_id
        from 
            hash_computation_progress
            join individual_steps using
                (step_order)
        where to_process = ''
    ), step_processing_progress as (
        select distinct on (box_id)
            box_id,
            0 as last_processed_instruction,
            []::struct(label text, focal_length int)[] as contents
        from steps_with_box_ids
        union all
        select
            step_processing_progress.box_id,
            steps_with_box_ids.step_order as last_processed_instruction,
            case
                when operation = '-' then
                    list_filter(contents, x -> x.label != steps_with_box_ids.label)
                when list_filter(contents, x -> x.label = steps_with_box_ids.label) = [] then
                    list_append(contents, {'label': steps_with_box_ids.label, 'focal_length': steps_with_box_ids.argument})
                else
                    list_transform(
                        contents, 
                        x -> case
                            when x.label = steps_with_box_ids.label then
                                {'label': steps_with_box_ids.label, 'focal_length': steps_with_box_ids.argument}
                            else
                                x
                        end
                    )
                end as contents
        from
            step_processing_progress
            asof join steps_with_box_ids on
                step_processing_progress.box_id = steps_with_box_ids.box_id
                and step_processing_progress.last_processed_instruction < steps_with_box_ids.step_order
    ), box_contents as (
        select distinct on (box_id)
            box_id,
            contents
        from step_processing_progress
        order by
            box_id,
            last_processed_instruction desc
    ), individual_lens_data as (
        select
            box_id,
            unnest(generate_series(1, len(contents))) as slot,
            unnest(contents).focal_length as focal_length
        from box_contents 
    )
select sum((box_id + 1) * slot * focal_length) as part_two
from individual_lens_data
; 
