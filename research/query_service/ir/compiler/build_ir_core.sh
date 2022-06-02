#!/bin/bash
cd ../core
if $(! command -v cargo &> /dev/null)
then
    echo "cargo not exit, skip compile"
else
    cargo build --release
fi
