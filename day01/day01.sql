create table raw_inputs as 
select
    column0 as line,
    row_number() over () - 1 as row
from read_csv_auto('input.txt', sep=chr(28))
;

with
    numbers as materialized (
        select
            regexp_extract_all(line, '[0-9]') as numbers
        from
            raw_inputs
    )
select
    sum((numbers[1] || numbers[-1])::int) as part_one
from
    numbers
;

with
    numbers as materialized (
        select
            line,
            regexp_extract(line, 'one|two|three|four|five|six|seven|eight|nine|[0-9]') as first_raw_digit,
            -- Cannot regexp_extract_all because it extracts non overlapping occurences.
            reverse(
                regexp_extract(reverse(line), reverse('one|two|three|four|five|six|seven|eight|nine') || '|[0-9]')
            ) as last_raw_digit
        from
            raw_inputs
    )
select
    sum(
        (
            case
                when first_raw_digit = 'one' then '1'
                when first_raw_digit = 'two' then '2'
                when first_raw_digit = 'three' then '3'
                when first_raw_digit = 'four' then '4'
                when first_raw_digit = 'five' then '5'
                when first_raw_digit = 'six' then '6'
                when first_raw_digit = 'seven' then '7'
                when first_raw_digit = 'eight' then '8'
                when first_raw_digit = 'nine' then '9'
                else first_raw_digit
            end || case
                when last_raw_digit = 'one' then '1'
                when last_raw_digit = 'two' then '2'
                when last_raw_digit = 'three' then '3'
                when last_raw_digit = 'four' then '4'
                when last_raw_digit = 'five' then '5'
                when last_raw_digit = 'six' then '6'
                when last_raw_digit = 'seven' then '7'
                when last_raw_digit = 'eight' then '8'
                when last_raw_digit = 'nine' then '9'
                else last_raw_digit
            end
        )::int
    ) as part_two
from
    numbers
;
