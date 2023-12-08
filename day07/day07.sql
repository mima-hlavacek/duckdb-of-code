create table raw_inputs as 
select
    column0 as line,
    row_number() over () - 1 as row
from read_csv_auto('input.txt', sep=chr(28))
;

create table card_to_value(card text, value text);
insert into card_to_value values
    ('A', 'a'),
    ('K', 'b'),
    ('Q', 'c'),
    ('J', 'd'),
    ('T', 'e'),
    ('9', 'f'),
    ('8', 'g'),
    ('7', 'h'),
    ('6', 'i'),
    ('5', 'j'),
    ('4', 'k'),
    ('3', 'l'),
    ('2', 'm')
;

with
    hands as (
        select
            row as hand_id,
            trim(split_part(line, ' ', 1)) as hand,
            split_part(line, ' ', 2)::int as bid
        from raw_inputs
    ), hands_long as (
        select 
            hand_id,
            unnest(generate_series(0, 4)) as card_id,
            unnest(string_to_array(hand, '')) as card
        from hands
    ), cards_per_hand_and_value as (
        select
            hand_id,
            value,
            count(*) as cards
        from
            hands_long
            join card_to_value using
                (card)
        group by
            hand_id,
            value
    ), hand_values as (
        select
            hand_id,
            case
                when max(cards) = 5 then -- five of a kind
                    '1'
                when max(cards) = 4 then -- four of a kind
                    '2'
                when max(cards) = 3 and count(*) = 2 then -- full house
                    '3'
                when max(cards) = 3 then -- three of a kind
                    '4'
                when max(cards) = 2 and count(*) = 3 then -- two pair
                    '5'
                when max(cards) = 2 then -- one pair
                    '6'
                else -- high card
                    '7'
            end as value
        from cards_per_hand_and_value
        group by hand_id
    ), hand_certificates as (
        select
            hand_id,
            hand_values.value || ',' || string_agg(card_to_value.value order by card_id, '') as certificate
        from
            hand_values
            join hands_long using 
                (hand_id)
            join card_to_value using
                (card)
        group by
            hand_id,
            hand_values.value
    ), ranks_by_certificate as (
        select
            hand_id,
            row_number() over (order by certificate desc) as rank
        from hand_certificates
    )
select sum(rank*bid) as part_one
from 
    hands
    join ranks_by_certificate using
        (hand_id)
;

truncate card_to_value;
insert into card_to_value values
    ('A', 'a'),
    ('K', 'b'),
    ('Q', 'c'),
    ('T', 'e'),
    ('9', 'f'),
    ('8', 'g'),
    ('7', 'h'),
    ('6', 'i'),
    ('5', 'j'),
    ('4', 'k'),
    ('3', 'l'),
    ('2', 'm'),
    ('J', 'n')
;

with recursive
    hands as (
        select
            row as hand_id,
            trim(split_part(line, ' ', 1)) as hand,
            split_part(line, ' ', 2)::int as bid
        from raw_inputs
    ), hands_long as (
        select 
            hand_id,
            unnest(generate_series(0, 4)) as card_id,
            unnest(string_to_array(hand, '')) as card
        from hands
    ), joker_replacement_options as materialized (
        select
            hand_id,
            card_id,
            card_to_value.card
        from 
            hands_long
            join card_to_value on
                hands_long.card = card_to_value.card
                or hands_long.card = 'J'
    ), hand_variants as (
        select
            card_0.hand_id,
            card_0.card || card_1.card || card_2.card || card_3.card || card_4.card as variant_id,
            unnest([card_0.card, card_1.card, card_2.card, card_3.card, card_4.card]) as card
        from
            (select * from joker_replacement_options where card_id = 0) as card_0
            join (select * from joker_replacement_options where card_id = 1) as card_1 on
                card_0.hand_id = card_1.hand_id
            join (select * from joker_replacement_options where card_id = 2) as card_2 on
                card_0.hand_id = card_2.hand_id
            join (select * from joker_replacement_options where card_id = 3) as card_3 on
                card_0.hand_id = card_3.hand_id
            join (select * from joker_replacement_options where card_id = 4) as card_4 on
                card_0.hand_id = card_4.hand_id
    ), cards_per_hand_variant_and_value as (
        select
            hand_id,
            variant_id,
            value,
            count(*) as cards
        from
            hand_variants
            join card_to_value using
                (card)
        group by
            hand_id,
            variant_id,
            value
    ), variant_values as (
        select
            hand_id,
            variant_id,
            case
                when max(cards) = 5 then -- five of a kind
                    '1'
                when max(cards) = 4 then -- four of a kind
                    '2'
                when max(cards) = 3 and count(*) = 2 then -- full house
                    '3'
                when max(cards) = 3 then -- three of a kind
                    '4'
                when max(cards) = 2 and count(*) = 3 then -- two pair
                    '5'
                when max(cards) = 2 then -- one pair
                    '6'
                else -- high card
                    '7'
            end as value
        from cards_per_hand_variant_and_value
        group by 
            hand_id,
            variant_id
    ), hand_values as (
        select
            hand_id,
            min(value) as value
        from variant_values
        group by hand_id    
    ), hand_certificates as (
        select
            hand_id,
            hand_values.value || ',' || string_agg(card_to_value.value order by card_id, '') as certificate
        from
            hand_values
            join hands_long using 
                (hand_id)
            join card_to_value using
                (card)
        group by
            hand_id,
            hand_values.value
    ), ranks_by_certificate as (
        select
            hand_id,
            row_number() over (order by certificate desc) as rank
        from hand_certificates
    )
select sum(rank*bid) as part_one
from 
    hands
    join ranks_by_certificate using
        (hand_id)
;
