#!/bin/bash
exit 0
if $(! command -v cargo &> /dev/null)
then
    echo "cargo not exit, skip compile"
else
    cd ../executor/ir/core
    cargo build --release
fi
