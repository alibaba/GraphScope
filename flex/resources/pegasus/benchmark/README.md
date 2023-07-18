## Build Dynamic Lib
```
# Create directory as input
cd gie-codegen/benchmark
mkdir code
cp ${rs_path} code

# build dynamic lib
# path of dynamic lib query/target/release/libcodegen_queries.dylib
./build_codegen.sh -c=code
```

## Run Codegen Query
```
# Build codegen runner
cd runner && cargo build --release && cd ..
RUST_LOG=debug CSR_PATH=${GRAPH_PATH} PARTITION_ID=0 runner/target/release/run_ldbc-w ${worker_num} -q ${query_file} -p -l query/target/release/libcodegen_queries.dylib
```
