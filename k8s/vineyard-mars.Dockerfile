# graphscope-vineyard image is based on graphscope-runtime, and will install
# libgrape-lite, vineyard, as well as necessary IO dependencies (e.g., hdfs, oss)
# in the image

ARG BASE_VERSION=latest
FROM reg.docker.alibaba-inc.com/7brs/gsa-mars-runtime:$BASE_VERSION

RUN cd /tmp && \
    git clone https://github.com/alibaba/libgrape-lite.git && \
    cd libgrape-lite && \
    mkdir build && \
    cd build && \
    cmake .. -DCMAKE_INSTALL_PREFIX=/opt/graphscope -DCMAKE_PREFIX_PATH=/opt/conda && \
    make -j`nproc` && \
    make install && \
    cd /tmp && \
    git clone https://github.com/alibaba/libvineyard.git && \
    cd libvineyard && \
    git submodule update --init && \
    mkdir -p /tmp/libvineyard/build && \
    cd /tmp/libvineyard/build && \
    cmake .. -DCMAKE_PREFIX_PATH="/opt/graphscope;/opt/conda" \
             -DCMAKE_INSTALL_PREFIX=/opt/graphscope \
             -DBUILD_VINEYARD_PYPI_PACKAGES=ON \
             -DBUILD_SHARED_LIBS=ON \
             -DBUILD_VINEYARD_IO_OSS=ON && \
    make install vineyard_client_python -j && \
    cd /tmp/libvineyard && \
    python3 setup.py bdist_wheel && \
    cd dist && \
    pip install *.whl
