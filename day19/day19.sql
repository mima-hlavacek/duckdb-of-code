create table raw_input as 
select
    line,
    row_number() over () - 1 as row
from read_csv('input.txt', sep=chr(28), columns={'line': 'text'})
;

with recursive
    flagged_sections as materialized (
        select
            *,
            (count(*) filter (where line is null) over (order by row))::bool as is_part,
        from raw_input
        qualify line is not null
    ), parts as materialized (
        select
            regexp_extract(line, 'x=(\d+)', 1)::int64 as x,
            regexp_extract(line, 'm=(\d+)', 1)::int64 as m,
            regexp_extract(line, 'a=(\d+)', 1)::int64 as a,
            regexp_extract(line, 's=(\d+)', 1)::int64 as s,
            row as id
        from flagged_sections
        where is_part
    ), raw_workflows as (
        select
            unnest(string_split(regexp_extract(line, '\{([^}]+)\}', 1), ',')) as part,
            regexp_extract(line, '[a-z]+') as workflow_id,
            unnest(generate_series(1, len(string_split(regexp_extract(line, '\{([^}]+)\}', 1), ',')))) - 1 as workflow_part_order
        from flagged_sections
        where not is_part
    ), workflows as materialized (
        select
            nullif(regexp_extract(part, 'x<(\d+):(.+)', 1), '')::int64 as value_greater_than_x,
            nullif(regexp_extract(part, 'x<(\d+):(.+)', 2), '')::text as greater_than_x_target,
            nullif(regexp_extract(part, 'x>(\d+):(.+)', 1), '')::int64 as value_less_than_x,
            nullif(regexp_extract(part, 'x>(\d+):(.+)', 2), '')::text as less_than_x_target,
            nullif(regexp_extract(part, 'm<(\d+):(.+)', 1), '')::int64 as value_greater_than_m,
            nullif(regexp_extract(part, 'm<(\d+):(.+)', 2), '')::text as greater_than_m_target,
            nullif(regexp_extract(part, 'm>(\d+):(.+)', 1), '')::int64 as value_less_than_m,
            nullif(regexp_extract(part, 'm>(\d+):(.+)', 2), '')::text as less_than_m_target,
            nullif(regexp_extract(part, 'a<(\d+):(.+)', 1), '')::int64 as value_greater_than_a,
            nullif(regexp_extract(part, 'a<(\d+):(.+)', 2), '')::text as greater_than_a_target,
            nullif(regexp_extract(part, 'a>(\d+):(.+)', 1), '')::int64 as value_less_than_a,
            nullif(regexp_extract(part, 'a>(\d+):(.+)', 2), '')::text as less_than_a_target,
            nullif(regexp_extract(part, 's<(\d+):(.+)', 1), '')::int64 as value_greater_than_s,
            nullif(regexp_extract(part, 's<(\d+):(.+)', 2), '')::text as greater_than_s_target,
            nullif(regexp_extract(part, 's>(\d+):(.+)', 1), '')::int64 as value_less_than_s,
            nullif(regexp_extract(part, 's>(\d+):(.+)', 2), '')::text as less_than_s_target,
            nullif(regexp_extract(part, '^[^:]+$'), '')::text as direct_target,
            workflow_id,
            workflow_part_order
        from raw_workflows
    ), workflow_processing_progress as (
        select
            *,
            'in' as workflow_id,
            0 as workflow_part_order
        from parts
        union all
        select
            x,
            m,
            a,
            s,
            id,
            coalesce(
                greater_than_x.greater_than_x_target,
                less_than_x.less_than_x_target,
                greater_than_m.greater_than_m_target,
                less_than_m.less_than_m_target,
                greater_than_a.greater_than_a_target,
                less_than_a.less_than_a_target,
                greater_than_s.greater_than_s_target,
                less_than_s.less_than_s_target,
                direct.direct_target,
                progress.workflow_id
            ) as workflow_id,
            case
                when coalesce(
                    greater_than_x.greater_than_x_target,
                    less_than_x.less_than_x_target,
                    greater_than_m.greater_than_m_target,
                    less_than_m.less_than_m_target,
                    greater_than_a.greater_than_a_target,
                    less_than_a.less_than_a_target,
                    greater_than_s.greater_than_s_target,
                    less_than_s.less_than_s_target,
                    direct.direct_target
                ) is null then
                    progress.workflow_part_order + 1
                else
                    0
            end as workflow_part_order
        from 
            workflow_processing_progress progress
            left join workflows greater_than_x on x < greater_than_x.value_greater_than_x and progress.workflow_id = greater_than_x.workflow_id and progress.workflow_part_order = greater_than_x.workflow_part_order 
            left join workflows less_than_x on x > less_than_x.value_less_than_x and progress.workflow_id = less_than_x.workflow_id and progress.workflow_part_order = less_than_x.workflow_part_order 
            left join workflows greater_than_m on m < greater_than_m.value_greater_than_m and progress.workflow_id = greater_than_m.workflow_id and progress.workflow_part_order = greater_than_m.workflow_part_order 
            left join workflows less_than_m on m > less_than_m.value_less_than_m and progress.workflow_id = less_than_m.workflow_id and progress.workflow_part_order = less_than_m.workflow_part_order 
            left join workflows greater_than_a on a < greater_than_a.value_greater_than_a and progress.workflow_id = greater_than_a.workflow_id and progress.workflow_part_order = greater_than_a.workflow_part_order 
            left join workflows less_than_a on a > less_than_a.value_less_than_a and progress.workflow_id = less_than_a.workflow_id and progress.workflow_part_order = less_than_a.workflow_part_order 
            left join workflows greater_than_s on s < greater_than_s.value_greater_than_s and progress.workflow_id = greater_than_s.workflow_id and progress.workflow_part_order = greater_than_s.workflow_part_order 
            left join workflows less_than_s on s > less_than_s.value_less_than_s and progress.workflow_id = less_than_s.workflow_id and progress.workflow_part_order = less_than_s.workflow_part_order 
            left join workflows direct on progress.workflow_id = direct.workflow_id and progress.workflow_part_order = direct.workflow_part_order
        where progress.workflow_id not in ('A', 'R')
    )
select sum(x + m + a + s) as part_one
from workflow_processing_progress
where workflow_id = 'A'
;

with recursive
    flagged_sections as materialized (
        select
            *,
            (count(*) filter (where line is null) over (order by row))::bool as is_part,
        from raw_input
        qualify line is not null
    ), raw_workflows as (
        select
            unnest(string_split(regexp_extract(line, '\{([^}]+)\}', 1), ',')) as part,
            regexp_extract(line, '[a-z]+') as workflow_id,
            unnest(generate_series(1, len(string_split(regexp_extract(line, '\{([^}]+)\}', 1), ',')))) - 1 as workflow_part_order
        from flagged_sections
        where not is_part
    ), workflows as materialized (
        select
            nullif(regexp_extract(part, 'x<(\d+):(.+)', 1), '')::int64 as value_greater_than_x,
            nullif(regexp_extract(part, 'x<(\d+):(.+)', 2), '')::text as greater_than_x_target,
            nullif(regexp_extract(part, 'x>(\d+):(.+)', 1), '')::int64 as value_less_than_x,
            nullif(regexp_extract(part, 'x>(\d+):(.+)', 2), '')::text as less_than_x_target,
            nullif(regexp_extract(part, 'm<(\d+):(.+)', 1), '')::int64 as value_greater_than_m,
            nullif(regexp_extract(part, 'm<(\d+):(.+)', 2), '')::text as greater_than_m_target,
            nullif(regexp_extract(part, 'm>(\d+):(.+)', 1), '')::int64 as value_less_than_m,
            nullif(regexp_extract(part, 'm>(\d+):(.+)', 2), '')::text as less_than_m_target,
            nullif(regexp_extract(part, 'a<(\d+):(.+)', 1), '')::int64 as value_greater_than_a,
            nullif(regexp_extract(part, 'a<(\d+):(.+)', 2), '')::text as greater_than_a_target,
            nullif(regexp_extract(part, 'a>(\d+):(.+)', 1), '')::int64 as value_less_than_a,
            nullif(regexp_extract(part, 'a>(\d+):(.+)', 2), '')::text as less_than_a_target,
            nullif(regexp_extract(part, 's<(\d+):(.+)', 1), '')::int64 as value_greater_than_s,
            nullif(regexp_extract(part, 's<(\d+):(.+)', 2), '')::text as greater_than_s_target,
            nullif(regexp_extract(part, 's>(\d+):(.+)', 1), '')::int64 as value_less_than_s,
            nullif(regexp_extract(part, 's>(\d+):(.+)', 2), '')::text as less_than_s_target,
            nullif(regexp_extract(part, '^[^:]+$'), '')::text as direct_target,
            workflow_id,
            workflow_part_order
        from raw_workflows
    ), workflow_processing_progress as (
        select
            1 as x_low,
            4000 as x_high,
            1 as m_low,
            4000 as m_high,
            1 as a_low,
            4000 as a_high,
            1 as s_low,
            4000 as s_high,
            'in' as workflow_id,
            0 as workflow_part_order
        union all
        (
            with
                split_intervals as (
                    select
                        unnest([x_low, greatest(value_less_than_x + 1, x_low)]) as x_low,
                        unnest([least(value_less_than_x, x_high), x_high]) as x_high,
                        m_low,
                        m_high,
                        a_low,
                        a_high,
                        s_low,
                        s_high,
                        unnest([workflow_id, less_than_x_target]) as workflow_id,
                        unnest([workflow_part_order + 1, 0]) as workflow_part_order
                    from 
                        workflow_processing_progress progress
                        join workflows using
                            (workflow_id, workflow_part_order)
                    where workflows.value_less_than_x is not null
                    union all
                    select
                        unnest([x_low, greatest(value_greater_than_x, x_low)]) as x_low,
                        unnest([least(value_greater_than_x - 1, x_high), x_high]) as x_high,
                        m_low,
                        m_high,
                        a_low,
                        a_high,
                        s_low,
                        s_high,
                        unnest([greater_than_x_target, workflow_id]) as workflow_id,
                        unnest([0, workflow_part_order + 1]) as workflow_part_order
                    from 
                        workflow_processing_progress progress
                        join workflows using
                            (workflow_id, workflow_part_order)
                    where workflows.value_greater_than_x is not null
                    union all
                    select
                        x_low,
                        x_high,
                        unnest([m_low, greatest(value_less_than_m + 1, m_low)]) as m_low,
                        unnest([least(value_less_than_m, m_high), m_high]) as m_high,
                        a_low,
                        a_high,
                        s_low,
                        s_high,
                        unnest([workflow_id, less_than_m_target]) as workflow_id,
                        unnest([workflow_part_order + 1, 0]) as workflow_part_order
                    from 
                        workflow_processing_progress progress
                        join workflows using
                            (workflow_id, workflow_part_order)
                    where workflows.value_less_than_m is not null
                    union all
                    select
                        x_low,
                        x_high,
                        unnest([m_low, greatest(value_greater_than_m, m_low)]) as m_low,
                        unnest([least(value_greater_than_m - 1, m_high), m_high]) as m_high,
                        a_low,
                        a_high,
                        s_low,
                        s_high,
                        unnest([greater_than_m_target, workflow_id]) as workflow_id,
                        unnest([0, workflow_part_order + 1]) as workflow_part_order
                    from 
                        workflow_processing_progress progress
                        join workflows using
                            (workflow_id, workflow_part_order)
                    where workflows.value_greater_than_m is not null
                    union all
                    select
                        x_low,
                        x_high,
                        m_low,
                        m_high,
                        unnest([a_low, greatest(value_less_than_a + 1, a_low)]) as a_low,
                        unnest([least(value_less_than_a, a_high), a_high]) as a_high,
                        s_low,
                        s_high,
                        unnest([workflow_id, less_than_a_target]) as workflow_id,
                        unnest([workflow_part_order + 1, 0]) as workflow_part_order
                    from 
                        workflow_processing_progress progress
                        join workflows using
                            (workflow_id, workflow_part_order)
                    where workflows.value_less_than_a is not null
                    union all
                    select
                        x_low,
                        x_high,
                        m_low,
                        m_high,
                        unnest([a_low, greatest(value_greater_than_a, a_low)]) as a_low,
                        unnest([least(value_greater_than_a - 1, a_high), a_high]) as a_high,
                        s_low,
                        s_high,
                        unnest([greater_than_a_target, workflow_id]) as workflow_id,
                        unnest([0, workflow_part_order + 1]) as workflow_part_order
                    from 
                        workflow_processing_progress progress
                        join workflows using
                            (workflow_id, workflow_part_order)
                    where workflows.value_greater_than_a is not null
                    union all
                    select
                        x_low,
                        x_high,
                        m_low,
                        m_high,
                        a_low,
                        a_high,
                        unnest([s_low, greatest(value_less_than_s + 1, s_low)]) as s_low,
                        unnest([least(value_less_than_s, s_high), s_high]) as s_high,
                        unnest([workflow_id, less_than_s_target]) as workflow_id,
                        unnest([workflow_part_order + 1, 0]) as workflow_part_order
                    from 
                        workflow_processing_progress progress
                        join workflows using
                            (workflow_id, workflow_part_order)
                    where workflows.value_less_than_s is not null
                    union all
                    select
                        x_low,
                        x_high,
                        m_low,
                        m_high,
                        a_low,
                        a_high,
                        unnest([s_low, greatest(value_greater_than_s, s_low)]) as s_low,
                        unnest([least(value_greater_than_s - 1, s_high), s_high]) as s_high,
                        unnest([greater_than_s_target, workflow_id]) as workflow_id,
                        unnest([0, workflow_part_order + 1]) as workflow_part_order
                    from 
                        workflow_processing_progress progress
                        join workflows using
                            (workflow_id, workflow_part_order)
                    where workflows.value_greater_than_s is not null
                    union all
                    select
                        x_low,
                        x_high,
                        m_low,
                        m_high,
                        a_low,
                        a_high,
                        s_low,
                        s_high,
                        direct_target as workflow_id,
                        0 as workflow_part_order
                    from 
                        workflow_processing_progress progress
                        join workflows using
                            (workflow_id, workflow_part_order)
                    where direct_target is not null
                ), cropped_intervals as (
                    select
                        greatest(1, least(4000, x_low)) as x_low,
                        greatest(1, least(4000, x_high)) as x_high,
                        greatest(1, least(4000, m_low)) as m_low,
                        greatest(1, least(4000, m_high)) as m_high,
                        greatest(1, least(4000, a_low)) as a_low,
                        greatest(1, least(4000, a_high)) as a_high,
                        greatest(1, least(4000, s_low)) as s_low,
                        greatest(1, least(4000, s_high)) as s_high,
                        workflow_id,
                        workflow_part_order
                    from split_intervals
                )
            select *
            from cropped_intervals
            where
                x_high >= x_low
                and m_high >= m_low
                and a_high >= a_low
                and s_high >= s_low
        )
    )
select sum((x_high - x_low + 1)::int128 * (m_high - m_low + 1)::int128 * (a_high - a_low + 1)::int128 * (s_high - s_low + 1)::int128) as part_two
--select *
from workflow_processing_progress
where workflow_id = 'A'
;