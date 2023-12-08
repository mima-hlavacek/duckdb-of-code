create table raw_input as 
select
    column0 as line,
    row_number() over () - 1 as row
from read_csv_auto('input.txt', sep=chr(28))
;

create table steps as (
with
    raw_steps as (
        select
            unnest(generate_series(1, len(line))) - 1 as id,
            unnest(string_to_array(line, '')) as direction
        from raw_input
        where row = 0
    )
select
    id,
    case
        when direction = 'L' then
            1
        else
            2
    end as direction
from raw_steps
);

create table adjacency_list as
select
    regexp_extract(line, '[A-Z0-9]{3}') as node,
    regexp_extract_all(line, '[A-Z0-9]{3}')[2:] as neighbors
from raw_input
where row >= 2
;

with recursive
    exploration_progress as (
        select
            'AAA' as node,
            0 as steps,
            (select max(id) + 1 from steps) as max_step_id
        union all
        select
            neighbors[direction] as node,
            exploration_progress.steps + 1 as steps,
            max_step_id
        from
            exploration_progress
            join adjacency_list using
                (node)
            join steps on
                exploration_progress.steps % max_step_id = steps.id
        where exploration_progress.node != 'ZZZ'
    )
select max(steps) as part_one
from exploration_progress
;

/*
Part two.

First, I did some analysis on how the solution space looks like by taking note of all reachable
end nodes.

with recursive
    all_reachable_nodes as materialized (
        select
            node,
            0 as steps,
            (select max(id) + 1 from steps) as max_step_id,
            node as starting_node
        from adjacency_list
        where node[3] = 'A'
        union
        select
            neighbors[direction] as node,
            (all_reachable_nodes.steps + 1) % max_step_id as steps,
            max_step_id,
            starting_node
        from
            all_reachable_nodes
            join adjacency_list using
                (node)
            join steps on
                all_reachable_nodes.steps = steps.id
    ), reachable_end_nodes as materialized (
        select *
        from all_reachable_nodes
        where node[3] = 'Z'
    )
select *
from reachable_end_nodes
;

┌─────────┬───────┬─────────────┬───────────────┐
│  node   │ steps │ max_step_id │ starting_node │
│ varchar │ int32 │    int64    │    varchar    │
├─────────┼───────┼─────────────┼───────────────┤
│ DDZ     │     0 │         263 │ RVA           │
│ RNZ     │     0 │         263 │ CMA           │
│ XKZ     │     0 │         263 │ MNA           │
│ ZZZ     │     0 │         263 │ AAA           │
│ LFZ     │     0 │         263 │ NJA           │
│ HMZ     │     0 │         263 │ DRA           │
└─────────┴───────┴─────────────┴───────────────┘

It can be seen that

1. Each starting node has exactly one reachable end node
2. The end node is always reached at the start of the input sequence

To solve the second part, we just need to find how long it takes us
to reach the end node and how long it takes to make a single cycle. Then,
we just need to combine this. 

with recursive
    nodes_visited_before_end as materialized (
        select
            node,
            0 as steps,
            (select max(id) + 1 from steps) as max_step_id,
            node as starting_node
        from adjacency_list
        where node[3] = 'A'
        union all
        select
            neighbors[direction] as node,
            nodes_visited_before_end.steps + 1 as steps,
            max_step_id,
            starting_node
        from
            nodes_visited_before_end
            join adjacency_list using
                (node)
            join steps on
                nodes_visited_before_end.steps % max_step_id = steps.id
        where node[3] != 'Z'
    ), steps_until_first_end_node as materialized (
        select distinct on (starting_node) *
        from nodes_visited_before_end
        order by starting_node, steps desc
    ), nodes_visited_between_end_nodes as materialized (
        select
            node,
            0 as steps,
            max_step_id,
            starting_node
        from steps_until_first_end_node
        union all
        select
            neighbors[direction] as node,
            nodes_visited_between_end_nodes.steps + 1 as steps,
            max_step_id,
            starting_node
        from
            nodes_visited_between_end_nodes
            join adjacency_list using
                (node)
            join steps on
                nodes_visited_between_end_nodes.steps % max_step_id = steps.id
        where neighbors[direction][3] != 'Z'
    ), cycle_lengths as materialized (
        select distinct on (starting_node)
            starting_node,
            steps + 1 as cycle_length
        from nodes_visited_between_end_nodes
        order by starting_node, steps desc
    )
select 
    starting_node,
    steps_until_first_end_node.steps as steps_until_end_node,
    cycle_length
from 
    steps_until_first_end_node
    join cycle_lengths using
        (starting_node)
;

┌───────────────┬──────────────────────┬──────────────┐
│ starting_node │ steps_until_end_node │ cycle_length │
│    varchar    │        int32         │    int32     │
├───────────────┼──────────────────────┼──────────────┤
│ AAA           │                18673 │        18673 │
│ CMA           │                13939 │        13939 │
│ DRA           │                20777 │        20777 │
│ MNA           │                17621 │        17621 │
│ NJA           │                19199 │        19199 │
│ RVA           │                12361 │        12361 │
└───────────────┴──────────────────────┴──────────────┘

Nice! The cycle length is the same as the steps until the first node is reached!
We just need to find the lowest common multiple of all these numbers e voila!
*/

with recursive
    nodes_visited_before_end as materialized (
        select
            node,
            0 as steps,
            (select max(id) + 1 from steps) as max_step_id,
            node as starting_node
        from adjacency_list
        where node[3] = 'A'
        union all
        select
            neighbors[direction] as node,
            nodes_visited_before_end.steps + 1 as steps,
            max_step_id,
            starting_node
        from
            nodes_visited_before_end
            join adjacency_list using
                (node)
            join steps on
                nodes_visited_before_end.steps % max_step_id = steps.id
        where node[3] != 'Z'
    ), cycle_lengths as materialized (
        select distinct on (starting_node)
            starting_node,
            steps::int64 as cycle_length
        from nodes_visited_before_end
        order by starting_node, steps desc
    ), cycle_lengths_with_id as materialized ( -- Duckdb has a pairwise LCM function, we need to iteratively apply it
        select
            *,
            row_number() over () as id
        from cycle_lengths
    ), lcm_computation_progress as (
        select
            0 as id,
            1::int64 as lcm
        union all
        select
            cycle_lengths_with_id.id,
            lcm(lcm_computation_progress.lcm, cycle_lengths_with_id.cycle_length) as lcm
        from
            lcm_computation_progress
            join cycle_lengths_with_id on
                lcm_computation_progress.id + 1 = cycle_lengths_with_id.id
    )
select max(lcm) as part_two
from lcm_computation_progress
;
