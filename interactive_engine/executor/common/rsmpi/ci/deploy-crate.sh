#!/bin/sh

set -e

if [ -n $1 -a $1 != "mpi" ]; then
  cd "$1"
fi

cargo publish
