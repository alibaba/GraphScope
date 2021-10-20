# graphscope-vineyard image is based on graphscope-runtime, and will install
# libgrape-lite, vineyard, as well as necessary IO dependencies (e.g., hdfs, oss)
# in the image

ARG BASE_VERSION=latest
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:$BASE_VERSION

RUN sudo mkdir -p /opt/vineyard && \
    sudo chown -R $(id -u):$(id -g) /opt/vineyard && \
    cd /tmp && \
    git clone https://github.com/alibaba/libgrape-lite.git --depth=1 && \
    cd libgrape-lite && \
    mkdir build && \
    cd build && \
    cmake .. -DCMAKE_INSTALL_PREFIX=/opt/vineyard && \
    make -j`nproc` && \
    make install && \
    cd /tmp && \
    git clone -b v0.3.1 https://github.com/alibaba/libvineyard.git --depth=1 && \
    cd libvineyard && \
    git submodule update --init && \
    mkdir -p /tmp/libvineyard/build && \
    cd /tmp/libvineyard/build && \
    cmake .. -DCMAKE_PREFIX_PATH=/opt/vineyard \
             -DCMAKE_INSTALL_PREFIX=/opt/vineyard \
             -DBUILD_SHARED_LIBS=ON \
             -DBUILD_VINEYARD_IO_OSS=ON && \
    make install vineyard_client_python -j && \
    cd /tmp/libvineyard && \
    python3 setup.py bdist_wheel && \
    cd dist && \
    auditwheel repair --plat=manylinux2014_x86_64 ./*.whl && \
    mkdir -p /opt/vineyard/dist && \
    cp -f wheelhouse/* /opt/vineyard/dist && \
    pip3 install wheelhouse/*.whl && \
    cd /tmp/libvineyard/modules/io && \
    python3 setup.py bdist_wheel && \
    cp -f dist/* /opt/vineyard/dist && \
    pip3 install dist/* && \
    sudo cp -r /opt/vineyard/* /usr/local/ && \
    cd /tmp && \
    rm -fr /tmp/libvineyard /tmp/libgrape-lite
