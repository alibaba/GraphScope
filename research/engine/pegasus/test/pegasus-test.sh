#!/bin/bash

RUST_LOG=info pegasus/target/release/examples/logistic_regression_distributed --data pegasus/pegasus/examples/data/binary.csv -s server.toml -t 50

sleep 10

RUST_LOG=info pegasus/target/release/examples/logistic_regression_distributed --data pegasus/pegasus/examples/data/binary.csv -s server.toml -p 2 -t 50
