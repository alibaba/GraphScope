# Analytical engine

ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG BASE_VERSION=v0.10.2
FROM $REGISTRY/graphscope/graphscope-dev:$BASE_VERSION AS builder

ADD . /home/graphscope/GraphScope

RUN sudo chown -R graphscope:graphscope /home/graphscope/GraphScope
RUN cd /home/graphscope/GraphScope/ \
    && mkdir /home/graphscope/install \
    && make gae-install ENABLE_JAVA_SDK=OFF INSTALL_PREFIX=/home/graphscope/install

############### RUNTIME: GAE #######################
FROM $REGISTRY/graphscope/vineyard-dev:$BASE_VERSION AS analytical

COPY --from=builder /home/graphscope/install /opt/graphscope/

ENV GRAPHSCOPE_HOME=/opt/graphscope LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/graphscope/lib

USER graphscope
WORKDIR /home/graphscope

############### RUNTIME: GAE-JAVA #######################
FROM $REGISTRY/graphscope/graphscope-dev:$BASE_VERSION AS builder

ADD . /home/graphscope/GraphScope

RUN sudo chown -R graphscope:graphscope /home/graphscope/GraphScope
RUN cd /home/graphscope/GraphScope/ \
    && mkdir /home/graphscope/install \
    && mkdir /home/graphscope/install-with-java \
    && make gae-install ENABLE_JAVA_SDK=ON INSTALL_PREFIX=/home/graphscope/install

FROM vineyardcloudnative/manylinux-llvm:2014-11.0.0 AS llvm

FROM $REGISTRY/graphscope/vineyard-dev:$BASE_VERSION AS analytical-java

COPY --from=builder /home/graphscope/install /opt/graphscope/

ENV GRAPHSCOPE_HOME=/opt/graphscope LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/graphscope/lib

COPY --from=llvm /opt/llvm11.0.0 /opt/llvm11
ENV LLVM11_HOME=/opt/llvm11
ENV LIBCLANG_PATH=$LLVM11_HOME/lib LLVM_CONFIG_PATH=$LLVM11_HOME/bin/llvm-config

# Installed size: 200M
RUN yum install -y java-1.8.0-openjdk-devel \
    && yum clean all \
    && rm -rf /var/cache/yum

USER graphscope
WORKDIR /home/graphscope
