#!/bin/bash
set -euxo pipefail

echo "Working directory is ${WORKDIR:="/tmp"}"

# use specified version of patchelf, as we found issues on AliLinux 2
# with newer version.
#
cd ${WORKDIR} && \
    git clone --depth=1 -b ec72eeb4ddba65abea31719ed84e41760bcb993a https://github.com/NixOS/patchelf.git && \
    cd patchelf && \
    ./bootstrap.sh && \
    ./configure && \
    make install -j && \
    rm -rf ${WORKDIR}/patchelf
