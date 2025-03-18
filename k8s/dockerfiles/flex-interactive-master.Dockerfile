ARG PLATFORM=x86_64
ARG ARCH=amd64
ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG VINEYARD_VERSION=latest
FROM $REGISTRY/graphscope/graphscope-dev:$VINEYARD_VERSION-$ARCH AS builder

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
    pip3 install /opt/flex/wheel/*.whl && apt-get remove -y git && \
    apt-get clean -y && rm -rf /var/lib/apt/lists/*

# Add graphscope user with user id 1001
RUN useradd -m graphscope -u 1001 && \
    echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers && \
    chown -R graphscope:graphscope /opt/flex

# set home to graphscope user
ENV HOME=/home/graphscope
USER graphscope
WORKDIR /home/graphscope