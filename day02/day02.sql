create table raw_inputs as 
select
    column0 as line,
    row_number() over () - 1 as row
from read_csv_auto('input.txt', sep=chr(28))
;

with
    game_ids as materialized (
        select
            regexp_extract(line, 'Game ([0-9]+)', 1)::int as game_id,
            line,
            row
        from raw_inputs
    ), blues as (
        select
            game_id,
            unnest(regexp_extract_all(line, '([0-9]+) blue', 1)) as n_blues
        from game_ids
    ), reds as (
        select
            game_id,
            unnest(regexp_extract_all(line, '([0-9]+) red', 1)) as n_reds
        from game_ids
    ), greens as (
        select
            game_id,
            unnest(regexp_extract_all(line, '([0-9]+) green', 1)) as n_greens
        from game_ids
    ), possible_games as (
        select game_id
        from blues
        group by game_id
        having bool_and(n_blues::int <= 14)
        intersect
        select game_id
        from reds
        group by game_id
        having bool_and(n_reds::int <= 12)
        intersect
        select game_id
        from greens
        group by game_id
        having bool_and(n_greens::int <= 13)
    )
select sum(game_id) as part_one
from possible_games
;

with
    game_ids as materialized (
        select
            regexp_extract(line, 'Game ([0-9]+)', 1)::int as game_id,
            line,
            row
        from raw_inputs
    ), blues as (
        select
            game_id,
            unnest(regexp_extract_all(line, '([0-9]+) blue', 1)) as n_blues
        from game_ids
    ), reds as (
        select
            game_id,
            unnest(regexp_extract_all(line, '([0-9]+) red', 1)) as n_reds
        from game_ids
    ), greens as (
        select
            game_id,
            unnest(regexp_extract_all(line, '([0-9]+) green', 1)) as n_greens
        from game_ids
    ), minimum_blues as (
        select
            game_id,
            max(n_blues::int) as minimum_blues
        from blues
        group by game_id
    ), minimum_reds as (
        select
            game_id,
            max(n_reds::int) as minimum_reds
        from reds
        group by game_id
    ), minimum_greens as (
        select
            game_id,
            max(n_greens::int) as minimum_greens
        from greens
        group by game_id
    )
select
    sum(minimum_blues * minimum_reds * minimum_greens) as part_two
from
    minimum_blues
    join minimum_reds using
        (game_id)
    join minimum_greens using
        (game_id)
;
