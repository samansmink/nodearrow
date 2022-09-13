# How to run demo
Firstly clone this demo

Then get the required duckdb branch:
https://github.com/samansmink/duckdb/tree/nodearrow

Requires this folder structure:
```
./nodearrow
./duckdb
```

Install arrow (e.g. with homebrew or apt)

Build DuckDB node lib:
```
cd ./duckdb/tools/nodejs
./configure
make debug
```

Build demo:
```
cd ./nodearrow
ARROW_PATH=<insert_whereever_arrrow_installed> make debug
```