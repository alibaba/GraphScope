# Analytical engine

ARG BASE_VERSION=v0.10.2
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:$BASE_VERSION AS builder

ARG profile=release
ENV profile=$profile
ADD . /home/graphscope/GraphScope

RUN sudo chown -R graphscope:graphscope /home/graphscope/GraphScope
RUN cd /home/graphscope/GraphScope/ \
    && mkdir /home/graphscope/install \
    && make gae-install ENABLE_JAVA_SDK=OFF INSTALL_PREFIX=/home/graphscope/install \
    && make clean \
    && mkdir /home/graphscope/install-with-java \
    && make gae-install ENABLE_JAVA_SDK=ON INSTALL_PREFIX=/home/graphscope/install-with-java

############### RUNTIME: GAE #######################
FROM registry.cn-hongkong.aliyuncs.com/graphscope/vineyard-runtime:$BASE_VERSION AS analytical

COPY --from=builder /home/graphscope/install /opt/graphscope/

USER graphscope
WORKDIR /home/graphscope

############### RUNTIME: GAE-JAVA #######################
FROM vineyardcloudnative/manylinux-llvm:2014-11.0.0 AS llvm
FROM registry.cn-hongkong.aliyuncs.com/graphscope/vineyard-runtime:$BASE_VERSION AS analytical-java

COPY --from=builder /home/graphscope/install-with-java /opt/graphscope/
COPY --from=llvm /opt/llvm11.0.0 /opt/llvm11

# Installed size: 200M
RUN yum install -y java-1.8.0-openjdk-devel \
    && yum clean all \
    && rm -rf /var/cache/yum

ENV JAVA_HOME /usr/lib/jvm/java

ENV LLVM11_HOME=/opt/llvm11
ENV LIBCLANG_PATH=/opt/llvm11/lib
ENV LLVM_CONFIG_PATH=/opt/llvm11/bin/llvm-config

USER graphscope
WORKDIR /home/graphscope
