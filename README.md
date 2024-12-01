# Advent of Code 2024

This year I will try to solve as many puzzles I can with SQL and duckdb. 
You can run this repository via the run.sh shell script. 

SQL is an interesting language, because you typically don't write the compiler what it needs to do, instead you declare the result and the database will do the details like sorting for you. 
For anyone who reads this, I hope you'll also see that for some puzzles / challenges that SQL can be very elegant and more than what it is typically used for.
The language is powerfull, but at the same time simple to read. 

## Turing completenes of SQL 
This [presentation](https://cdn.oreillystatic.com/en/assets/1/event/27/High%20Performance%20SQL%20with%20PostgreSQL%20Presentation.pdf) from David Fetter shows that SQL is turing complete and thus every problem that can be solved with a typical programming language can also be solved with SQL.

## DuckDB
DuckDB is a fairly new analytical in-memory database. 
This makes it a good fit for these challenges, because its performant and easy to use. 
Because it is built for analytical use-cases it is well suited for aggregating data. 
