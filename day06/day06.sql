-- I'm NOT parsing four pairs of integers!
create table races(
    id int,
    duration int,
    distance int
)
;

-- insert into races values
--     (1, 7, 9),
--     (2, 15, 40),
--     (3, 30, 200)
-- ;
insert into races values
    (1, 62, 644),
    (2, 73, 1023),
    (3, 75, 1240),
    (4, 65, 1023)
;

with
    real_limits as (
        select
            id,
            (duration - sqrt(duration**2 - 4*distance)) / 2 as lower,
            (duration + sqrt(duration**2 - 4*distance)) / 2 as upper
        from races
    ), rounded_limits as (
        select
            id,
            case
                when lower = lower::int64::double then
                    lower::int64 + 1
                else
                    ceil(lower)::int64
            end as lower,
            case
                when upper = upper::int64::double then
                    upper::int64 - 1
                else
                    floor(upper)::int64
            end as upper
        from real_limits
    )
select product(upper - lower + 1)::int64 as part_one_fast
from rounded_limits;

with
    speeds as (
        select
            id,
            duration,
            distance,
            unnest(generate_series(0, duration)) as speed
        from races
    ), won_races as (
        select
            id,
            count(*) as victories
        from speeds
        where speed * (duration - speed) > distance
        group by id
    )
select product(victories)::int as part_one
from won_races
;

with
    race as (
        select
            62737565 as duration,
            644102312401023 as distance
    ), real_limits as (
        select
            (duration - sqrt(duration**2 - 4*distance)) / 2 as lower,
            (duration + sqrt(duration**2 - 4*distance)) / 2 as upper
        from race
    ), rounded_limits as (
        select
            case
                when lower = lower::int64::double then
                    lower::int64 + 1
                else
                    ceil(lower)::int64
            end as lower,
            case
                when upper = upper::int64::double then
                    upper::int64 - 1
                else
                    floor(upper)::int64
            end as upper
        from real_limits
    )
select (upper - lower + 1)::int64 as part_two_fast
from rounded_limits;

with
    speeds as (
        select
            --71530 as duration,
            --940200 as distance,
            --unnest(generate_series(0, 71530)) as speed
            62737565 as duration,
            644102312401023 as distance,
            unnest(generate_series(0, 62737565)) as speed
    )
select count(*) as part_two
from speeds
where speed * (duration - speed) > distance
;
