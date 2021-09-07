# graphscope-vineyard image is based on graphscope-runtime, and will install
# libgrape-lite, vineyard, as well as necessary IO dependencies (e.g., hdfs, oss)
# in the image

ARG BASE_VERSION=latest
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:$BASE_VERSION

RUN sudo mkdir -p /opt/graphscope && \
    sudo chown -R graphscope:graphscope /opt/graphscope && \
    cd /tmp && \
    git clone https://github.com/alibaba/libgrape-lite.git --depth=1 && \
    cd libgrape-lite && \
    mkdir build && \
    cd build && \
    cmake .. -DCMAKE_INSTALL_PREFIX=/opt/graphscope && \
    make -j`nproc` && \
    make install && \
    cd /tmp && \
    git clone -b zsy/schema https://github.com/siyuan0322/v6d.git --depth=1 && \
    cd v6d && \
    git submodule update --init && \
    mkdir -p /tmp/v6d/build && \
    cd /tmp/v6d/build && \
    cmake .. -DCMAKE_PREFIX_PATH=/opt/graphscope \
             -DCMAKE_INSTALL_PREFIX=/opt/graphscope \
             -DBUILD_VINEYARD_PYPI_PACKAGES=ON \
             -DBUILD_SHARED_LIBS=ON \
             -DBUILD_VINEYARD_IO_OSS=ON && \
    make install vineyard_client_python -j && \
    cd /tmp/v6d && \
    python3 setup.py bdist_wheel && \
    cd dist && \
    auditwheel repair --plat=manylinux2014_x86_64 ./*.whl && \
    mkdir -p /opt/graphscope/dist && \
    cp -f wheelhouse/* /opt/graphscope/dist && \
    pip3 install wheelhouse/*.whl && \
    cd /tmp/v6d/modules/io && \
    python3 setup.py bdist_wheel && \
    cp -f dist/* /opt/graphscope/dist && \
    pip3 install dist/* && \
    cd /tmp && \
    rm -fr /tmp/v6d /tmp/libgrape-lite
