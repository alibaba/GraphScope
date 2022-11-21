#!/bin/bash
set -euxo pipefail

echo "Working directory is ${WORKDIR:="/tmp"}"

cd ${WORKDIR} && \
    git clone --depth=1 https://github.com/NixOS/patchelf.git && \
    cd patchelf && \
    ./bootstrap.sh && \
    ./configure && \
    make install -j && \
    rm -rf ${WORKDIR}/patchelf
