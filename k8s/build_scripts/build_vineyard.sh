#!/bin/bash
set -euxo pipefail

echo "Working directory is ${WORKDIR:="/tmp"}"

# libgrape-lite, required by vineyard
echo "Installing libgrape-lite"
cd ${WORKDIR} && \
    git clone -b ${LIBGRAPE_LITE_VERSION:-master} https://github.com/alibaba/libgrape-lite.git --depth=1 && \
    cd libgrape-lite && \
    cmake . -DCMAKE_INSTALL_PREFIX=/opt/vineyard && \
    make -j$(nproc) && \
    make install && \
    strip /opt/vineyard/bin/run_app && \
    rm -rf ${WORKDIR}/libgrape-lite

# Vineyard
echo "Installing vineyard"
cd ${WORKDIR} && \
    git clone -b v0.11.6 https://github.com/v6d-io/v6d.git --depth=1 && \
    pushd v6d && \
    git submodule update --init && \
    cmake . -DCMAKE_PREFIX_PATH=/opt/vineyard \
        -DCMAKE_INSTALL_PREFIX=/opt/vineyard \
        -DBUILD_VINEYARD_TESTS=OFF \
        -DBUILD_SHARED_LIBS=ON \
        -DBUILD_VINEYARD_PYTHON_BINDINGS=ON && \
    make -j$(nproc) && \
    make install && \
    strip /opt/vineyard/bin/vineyard* /opt/vineyard/lib/libvineyard* && \
    python3 setup.py bdist_wheel && \
    python3 setup_io.py bdist_wheel && \
    pip3 install dist/* && \
    sudo cp -rs /opt/vineyard/* /usr/local/ && \
    rm -rf ${WORKDIR}/v6d
