#!/bin/zsh

cd 02

#duckdb < day1.sql
duckdb < day2.sql test.db
duckdb < day2_2.sql test.db
