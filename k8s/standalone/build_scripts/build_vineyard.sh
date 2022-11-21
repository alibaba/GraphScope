#!/bin/bash
set -euxo pipefail

echo "Working directory is ${WORKDIR:="/tmp"}"

# libgrape-lite, required by vineyard
echo "Installing libgrape-lite"
cd ${WORKDIR} && \
    git clone -b master https://github.com/alibaba/libgrape-lite.git --depth=1 && \
    cd libgrape-lite && \
    cmake . && \
    make -j`nproc` && \
    make install && \
    rm -rf ${WORKDIR}/libgrape-lite

# Vineyard
echo "Installing vineyard"
cd ${WORKDIR} && \
    git clone -b v0.10.2 https://github.com/v6d-io/v6d.git --depth=1 && \
    pushd v6d && \
    git submodule update --init && \
    cmake . -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DBUILD_VINEYARD_TESTS=OFF \
        -DBUILD_VINEYARD_PYTHON_BINDINGS=ON && \
    make -j`nproc` && \
    make install && \
    strip /usr/local/bin/vineyard* /usr/local/lib/libvineyard* && \
    popd && \
    python3 setup.py bdist_wheel && \
    python3 setup_io.py bdist_wheel && \
    pip3 install dist/*
    rm -rf ${WORKDIR}/v6d
