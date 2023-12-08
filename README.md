# DuckDB of code

Advent of code 2023 in SQL, in duckdb.

The idea is to finish Advent of code 2023 while implementing things in SQL using
duckdb. Because coming up with SQL-based solutions is difficult, it will likely
take we way longer than until 25th of december. But hopefully I will manage to
complete it during 2024 😄.

## Why?

It's fun, and it makes me learn obscure SQL tricks I won't have an option to
learn otherwise. Also, bragging rights.

## How to run things

1. Install duckdb from duckdb.org
2. Add `.prompt '­🦆 '` to `~/.duckdbrc` to have a duck emoji as your prompt
   character (CRUCIAL!)
3. Put your inputs to the desired day into `dayXY/inputs.txt`
4. `cd dayXY && duckdb < dayXY.sql`
