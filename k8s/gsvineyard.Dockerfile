# graphscope-vineyard image is based on graphscope-runtime, and will install
# libgrape-lite, vineyard, as well as necessary IO dependencies (e.g., hdfs, oss)
# in the image

ARG BASE_VERSION=latest
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:$BASE_VERSION

# build & install fastffi
RUN cd /opt/ && \
    sudo git clone https://github.com/alibaba/fastFFI.git && \
    sudo chown -R $(id -u):$(id -g) /opt/fastFFI && \
    cd fastFFI && \
    git checkout a166c6287f2efb938c27fb01b3d499932d484f9c && \
    export PATH=${PATH}:${LLVM11_HOME}/bin && \
    mvn clean install -DskipTests

ENV LLVM4JNI_HOME=/opt/fastFFI/llvm4jni

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
    git clone -b v0.3.21 https://github.com/v6d-io/v6d.git --depth=1 && \
    cd v6d && \
    git submodule update --init && \
    mkdir -p /tmp/v6d/build && \
    cd /tmp/v6d/build && \
    cmake .. -DCMAKE_PREFIX_PATH=/opt/vineyard \
             -DCMAKE_INSTALL_PREFIX=/opt/vineyard \
             -DBUILD_SHARED_LIBS=ON && \
    make install vineyard_client_python -j && \
    cd /tmp/v6d && \
    python3 setup.py bdist_wheel && \
    cd dist && \
    auditwheel repair --plat=manylinux2014_x86_64 ./*.whl && \
    mkdir -p /opt/vineyard/dist && \
    cp -f wheelhouse/* /opt/vineyard/dist && \
    pip3 install wheelhouse/*.whl && \
    cd /tmp/v6d/modules/io && \
    python3 setup.py bdist_wheel && \
    cp -f dist/* /opt/vineyard/dist && \
    pip3 install dist/* && \
    sudo cp -r /opt/vineyard/* /usr/local/ && \
    cd /tmp && \
    rm -fr /tmp/v6d /tmp/libgrape-lite
