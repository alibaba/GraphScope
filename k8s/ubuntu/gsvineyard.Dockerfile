# graphscope-vineyard image is based on graphscope-runtime:ubuntu, and will install
# libgrape-lite, vineyard, as well as necessary IO dependencies (e.g., hdfs, oss)
# in the image

ARG BASE_VERSION=ubuntu
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:$BASE_VERSION

RUN cd /tmp && \
    git clone https://github.com/alibaba/libgrape-lite.git --depth=1 && \
    cd libgrape-lite && \
    mkdir build && \
    cd build && \
    cmake .. && \
    make -j`nproc` && \
    make install && \
    cd /tmp && \
    git clone -b v0.2.6 https://github.com/alibaba/libvineyard.git --depth=1 && \
    cd libvineyard && \
    git submodule update --init && \
    mkdir -p /tmp/libvineyard/build && \
    cd /tmp/libvineyard/build && \
    cmake .. -DBUILD_VINEYARD_PYPI_PACKAGES=ON \
             -DBUILD_SHARED_LIBS=ON \
             -DBUILD_VINEYARD_IO_OSS=ON && \
    make install vineyard_client_python -j && \
    cd /tmp/libvineyard && \
    python3 setup.py bdist_wheel && \
    cd dist && \
    mkdir -p /opt/graphscope/dist && \
    cp -f ./*.whl /opt/graphscope/dist && \
    pip3 install ./*.whl && \
    cd /tmp/libvineyard/modules/io && \
    python3 setup.py bdist_wheel && \
    cp -f dist/* /opt/graphscope/dist && \
    pip3 install dist/* && \
    cd /tmp && \
    rm -fr /tmp/libvineyard /tmp/libgrape-lite
