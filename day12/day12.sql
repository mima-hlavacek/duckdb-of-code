create table raw_input as 
select
    column0 as line,
    row_number() over () - 1 as row
from read_csv_auto('input.txt', sep=chr(28))
;

with recursive
    springs as materialized (
        select
            row as id,
            split_part(line, ' ', 1) || '.' as spring,  -- Add a cap on the end of the line to simplify dynamic programming
            string_split(
                split_part(line, ' ', 2),
                ','
            )::int[] as groups
        from raw_input
    ), placement_options as (
        select
            id,
            0 as spring_position,
            1 as current_group,
            0 as current_group_size,
            1 as options
        from springs
        union all
        (
            with
                next_characters as materialized (
                    select
                        id,
                        spring_position + 1 as spring_position,
                        current_group,
                        current_group_size,
                        options,
                        springs.spring[spring_position + 1] as next_character,
                        springs.groups[current_group] as intended_current_group_size
                    from
                        placement_options
                        join springs using
                            (id)
                    where springs.spring[spring_position + 1] != ''
                ), next_is_operational_and_no_group_has_started as (
                    select
                        id,
                        spring_position,
                        current_group,
                        0 as current_group_size,
                        options
                    from next_characters
                    where 
                        next_character in ('.', '?')
                        and current_group_size = 0
                ), next_is_operational_and_group_is_complete as (
                    select
                        id,
                        spring_position,
                        current_group + 1,
                        0 as current_group_size,
                        options
                    from next_characters
                    where 
                        next_character in ('.', '?')
                        and current_group_size = intended_current_group_size
                ), next_is_damaged as (
                    select
                        id,
                        spring_position,
                        current_group,
                        current_group_size + 1,
                        options
                    from next_characters
                    where 
                        next_character in ('#', '?')
                        and current_group_size < intended_current_group_size
                ), all_options as (
                    select *
                    from next_is_operational_and_no_group_has_started
                    union all
                    select *
                    from next_is_operational_and_group_is_complete
                    union all
                    select *
                    from next_is_damaged
                )
            select
                id,
                spring_position,
                current_group,
                current_group_size,
                sum(options) as options
            from all_options
            group by
                id,
                spring_position,
                current_group,
                current_group_size                
        )
    )
select
    sum(options) as part_one
from 
    placement_options
    join springs using
        (id)
where
    spring_position = len(spring)
    and current_group = len(groups) + 1
    and current_group_size = 0
;


with recursive
    springs as (
        select
            row as id,
            (repeat(split_part(line, ' ', 1) || '?', 5))[:-2] || '.' as spring,  -- Add a cap on the end of the line to simplify dynamic programming
            string_split(
                (repeat(split_part(line, ' ', 2) || ',', 5))[:-2],
                ','
            )::int[] as groups
        from raw_input
    ), placement_options as (
        select
            id,
            0 as spring_position,
            1 as current_group,
            0 as current_group_size,
            1::int64 as options
        from springs
        union all
        (
            with
                next_characters as materialized (
                    select
                        id,
                        spring_position + 1 as spring_position,
                        current_group,
                        current_group_size,
                        options,
                        springs.spring[spring_position + 1] as next_character,
                        springs.groups[current_group] as intended_current_group_size
                    from
                        placement_options
                        join springs using
                            (id)
                    where springs.spring[spring_position + 1] != ''
                ), next_is_operational_and_no_group_has_started as (
                    select
                        id,
                        spring_position,
                        current_group,
                        0 as current_group_size,
                        options
                    from next_characters
                    where 
                        next_character in ('.', '?')
                        and current_group_size = 0
                ), next_is_operational_and_group_is_complete as (
                    select
                        id,
                        spring_position,
                        current_group + 1,
                        0 as current_group_size,
                        options
                    from next_characters
                    where 
                        next_character in ('.', '?')
                        and current_group_size = intended_current_group_size
                ), next_is_damaged as (
                    select
                        id,
                        spring_position,
                        current_group,
                        current_group_size + 1,
                        options
                    from next_characters
                    where 
                        next_character in ('#', '?')
                        and current_group_size < intended_current_group_size
                ), all_options as (
                    select *
                    from next_is_operational_and_no_group_has_started
                    union all
                    select *
                    from next_is_operational_and_group_is_complete
                    union all
                    select *
                    from next_is_damaged
                )
            select
                id,
                spring_position,
                current_group,
                current_group_size,
                sum(options) as options
            from all_options
            group by
                id,
                spring_position,
                current_group,
                current_group_size                
        )
    )
select
    sum(options) as part_two
from 
    placement_options
    join springs using
        (id)
where
    spring_position = len(spring)
    and current_group = len(groups) + 1
    and current_group_size = 0
;
