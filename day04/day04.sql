create table raw_inputs as 
select
    column0 as line,
    row_number() over () - 1 as row
from read_csv_auto('input.txt', sep=chr(28))
;

with
    card_ids as (
        select
            regexp_extract(split_part(line, ':', 1), '\d+')::int as card_id,
            split_part(line, ':', 2) as card_attributes
        from raw_inputs
    ), winning_numbers as (
        select
            card_id,
            unnest(regexp_extract_all(split_part(card_attributes, '|', 1), '\d+'))::int as winning_number
        from card_ids
    ), card_numbers as (
        select
            card_id,
            unnest(regexp_extract_all(split_part(card_attributes, '|', 2), '\d+'))::int as card_number
        from card_ids
    ), card_scores as (
        select
            card_numbers.card_id,
            (2**(count(*) - 1))::int as card_score 
        from 
            card_numbers
            join winning_numbers on
                card_numbers.card_id = winning_numbers.card_id
                and card_numbers.card_number = winning_numbers.winning_number
        group by card_numbers.card_id
    )
select sum(card_score) as part_one
from card_scores
;

with recursive
    card_ids as (
        select
            regexp_extract(split_part(line, ':', 1), '\d+')::int as card_id,
            split_part(line, ':', 2) as card_attributes
        from raw_inputs
    ), winning_numbers as (
        select
            card_id,
            unnest(regexp_extract_all(split_part(card_attributes, '|', 1), '\d+'))::int as winning_number
        from card_ids
    ), card_numbers as (
        select
            card_id,
            unnest(regexp_extract_all(split_part(card_attributes, '|', 2), '\d+'))::int as card_number
        from card_ids
    ), ranked_cards as materialized (
        select
            card_numbers.card_id,
            count(winning_numbers.card_id) as cards_won
        from 
            card_numbers
            left join winning_numbers on
                card_numbers.card_id = winning_numbers.card_id
                and card_numbers.card_number = winning_numbers.winning_number
        group by card_numbers.card_id
    ), total_cards as (
        select
            card_id,
            1::int128 as copies
        from ranked_cards
        union all
        select
            card_won_info.card_id,
            sum(total_cards.copies) as copies
        from 
            total_cards
            join ranked_cards as winning_card_info using 
                (card_id)
            join ranked_cards as card_won_info on
                total_cards.card_id + 1 <= card_won_info.card_id
                and card_won_info.card_id < total_cards.card_id + 1 + winning_card_info.cards_won
        group by
            card_won_info.card_id
    )
select sum(copies) as part_two
from total_cards
;
