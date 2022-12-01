# Interactive engine

ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG BASE_VERSION=latest
FROM $REGISTRY/graphscope/graphscope-dev:$BASE_VERSION AS builder

ARG profile=release
ENV profile=$profile
ADD . /home/graphscope/GraphScope

RUN sudo chown -R graphscope:graphscope /home/graphscope/GraphScope
ENV PATH=$PATH:/opt/maven/apache-maven-3.8.6/bin
RUN cd /home/graphscope/GraphScope/ \
    && mkdir /home/graphscope/install \
    && make interactive-install BUILD_TYPE="$profile" INSTALL_PREFIX=/home/graphscope/install \
    && strip /home/graphscope/install/bin/gaia_executor

############### RUNTIME: frontend #######################
FROM centos:7.9.2009 AS frontend

COPY --from=builder /home/graphscope/install/bin/giectl /opt/graphscope/bin/giectl
# vineyard.frontend.properties, log configuration files
COPY --from=builder /home/graphscope/install/conf /opt/graphscope/conf
# jars, libir_core.so
COPY --from=builder /home/graphscope/install/lib /opt/graphscope/lib

ENV GRAPHSCOPE_HOME=/opt/graphscope LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/graphscope/lib


RUN yum install -y java-1.8.0-openjdk \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER graphscope
WORKDIR /home/graphscope

############### RUNTIME: executor #######################
FROM $REGISTRY/graphscope/vineyard-runtime:$BASE_VERSION AS executor

# gaia_executor, giectl
COPY --from=builder /home/graphscope/install/bin /opt/graphscope/bin
# vineyard.executor.properties, log configuration files
COPY --from=builder /home/graphscope/install/conf /opt/graphscope/conf

ENV GRAPHSCOPE_HOME=/opt/graphscope LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/graphscope/lib

ENV RUST_BACKTRACE=1

USER graphscope
WORKDIR /home/graphscope
