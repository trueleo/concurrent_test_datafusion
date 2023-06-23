### Run concurrent datafusion queries


#### Sample data source
Clone apache-datafusion repo

Initalize parquet testing 
```sh 
git submodule update --init --recursive
```

Run command to generate
```sh
cd parquet-testing
cargo run --release --bin parquet -- filter  --path ./data --scale-factor 1.0
```

you can interrupt the program and copy data/logs.parquet

[from paruqet benchmarks](https://github.com/apache/arrow-datafusion/tree/main/benchmarks#parquet-benchmarks)

#### Running test

Clone this repo and run

```sh
cargo r -- <count> | less -S
```

change count to set number of concurrent requests
