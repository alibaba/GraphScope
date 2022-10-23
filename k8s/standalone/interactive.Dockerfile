# Interactive engine

ARG BASE_VERSION=v0.9.0
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:$BASE_VERSION AS builder

ARG profile=release
ENV profile=$profile
ADD . /home/graphscope/GraphScope

ENV PATH="/home/graphscope/.cargo/bin:$PATH"

RUN sudo chown -R graphscope:graphscope /home/graphscope/GraphScope
RUN cd /home/graphscope/GraphScope/ \
    && mkdir /home/graphscope/install \
    && make gie BUILD_TYPE="$profile" INSTALL_PREFIX=/home/graphscope/install

############### RUNTIME: frontend #######################
FROM centos:7.9.2009 AS frontend

COPY --from=builder /home/graphscope/install/bin/giectl /opt/graphscope/bin/giectl
# vineyard.frontend.properties, log configuration files
COPY --from=builder /home/graphscope/install/conf /opt/graphscope/conf
# jars, libir_core.so
COPY --from=builder /home/graphscope/install/lib /opt/graphscope/lib

RUN yum install -y java-1.8.0-openjdk-devel \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER graphscope
WORKDIR /home/graphscope

############### RUNTIME: executor #######################
FROM registry.cn-hongkong.aliyuncs.com/graphscope/vineyard-runtime:$BASE_VERSION AS executor

# gaia_executor, giectl
COPY --from=builder /home/graphscope/install/bin /opt/graphscope/bin
# vineyard.executor.properties, log configuration files
COPY --from=builder /home/graphscope/install/conf /opt/graphscope/conf

ENV RUST_BACKTRACE=1

USER graphscope
WORKDIR /home/graphscope
