ARG PLATFORM=x86_64
ARG ARCH=amd64
ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG VINEYARD_VERSION=latest
FROM $REGISTRY/graphscope/graphscope-dev:$VINEYARD_VERSION-$ARCH AS builder
ARG PARALLEL=16

RUN sudo mkdir -p /opt/flex/wheel && sudo chown -R graphscope:graphscope /opt/flex/
USER graphscope
WORKDIR /home/graphscope

# change bash as default
SHELL ["/bin/bash", "-c"]

COPY --chown=graphscope:graphscope . /home/graphscope/GraphScope

RUN cd ${HOME}/GraphScope && \
    git submodule update --init && cd flex/interactive/sdk && bash generate_sdk.sh -g python -t server && \
    cd master && pip3 install -r requirements.txt &&  python3 setup.py bdist_wheel &&  \
    cp dist/*.whl /opt/flex/wheel/ 

# install flex
RUN cd ${HOME}/GraphScope/flex && \
    mkdir build && cd build && cmake .. -DCMAKE_INSTALL_PREFIX=/opt/flex -DBUILD_DOC=OFF -DBUILD_TEST=OFF -DOPTIMIZE_FOR_HOST=${OPTIMIZE_FOR_HOST} -DUSE_STATIC_ARROW=ON && \
    make -j ${PARALLEL} && make install

# strip all .so in /opt/flex/lib
RUN sudo find /opt/flex/lib/ -name "*.so" -type f -exec strip {} \;
# strip all binary in /opt/flex/bin
RUN sudo strip /opt/flex/bin/bulk_loader


########################### RUNTIME IMAGE ###########################

FROM ubuntu:22.04 as master
ARG PLATFORM=x86_64

ENV DEBIAN_FRONTEND=noninteractive

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

RUN apt-get update && apt-get -y install sudo locales tzdata python3 python3-pip && \
    locale-gen en_US.UTF-8 && apt-get clean -y && rm -rf /var/lib/apt/lists/* && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY --from=builder /opt/flex/wheel/ /opt/flex/wheel/

RUN apt-get update && apt-get install -y git && pip3 install --upgrade pip && \
    pip3 install git+https://github.com/kragniz/python-etcd3.git@e58a899579ba416449c4e225b61f039457c8072a && \
    pip3 install /opt/flex/wheel/*.whl && \
    apt-get clean -y && rm -rf /var/lib/apt/lists/*

RUN mkdir /opt/vineyard/

    # copy builder's /opt/flex to final image
COPY --from=builder /opt/flex/bin/bulk_loader /opt/flex/bin/
COPY --from=builder /opt/flex/lib/ /opt/flex/lib/
COPY --from=builder /opt/graphscope/lib/libgrape-lite.so /opt/flex/lib/
COPY --from=builder /usr/lib/$PLATFORM-linux-gnu/libsnappy*.so* /usr/lib/$PLATFORM-linux-gnu/
COPY --from=builder /usr/lib/$PLATFORM-linux-gnu/libprotobuf* /usr/lib/$PLATFORM-linux-gnu/libfmt*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libre2*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libutf8proc*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libevent*.so*  \
    /usr/lib/$PLATFORM-linux-gnu/libltdl*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libopen-pal*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libunwind*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libhwloc*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libopen-rte*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libcrypto*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libboost_thread*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libboost_filesystem*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libboost_program_options*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libmpi*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libyaml-cpp*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libglog*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libgflags*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libicudata.so* \
    /usr/lib/$PLATFORM-linux-gnu/

RUN sudo rm -rf /usr/lib/$PLATFORM-linux-gnu/libLLVM*.so* && sudo rm -rf /opt/flex/lib/libseastar.a && \
    sudo rm -rf /usr/lib/$PLATFORM-linux-gnu/libcuda.so && \
    sudo rm -rf /usr/lib/$PLATFORM-linux-gnu/libcudart.so

RUN sudo ln -sf /opt/flex/bin/* /usr/local/bin/ \
    && sudo ln -sfn /opt/flex/include/* /usr/local/include/ \
    && sudo ln -sf -r /opt/flex/lib/* /usr/local/lib \
    && sudo ln -sf /opt/flex/lib64/*so* /usr/local/lib64 \
    && chmod +x /opt/flex/bin/*

RUN find /opt/flex/lib/ -name "*.a" -type f -delete

# Add graphscope user with user id 1001
RUN useradd -m graphscope -u 1001 && \
    echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers && \
    chown -R graphscope:graphscope /opt/flex

# set home to graphscope user
ENV HOME=/home/graphscope
USER graphscope
WORKDIR /home/graphscope