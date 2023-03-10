# Interactive engine

ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG BUILDER_VERSION=latest
ARG RUNTIME_VERSION=latest
FROM $REGISTRY/graphscope/graphscope-dev:$BUILDER_VERSION AS builder

ARG CI=false

ARG profile=release
ENV profile=$profile
COPY --chown=graphscope:graphscope . /home/graphscope/GraphScope

RUN cd /home/graphscope/GraphScope/ && \
    if [ "${CI}" == "true" ]; then \
        cp -r artifacts/interactive /home/graphscope/install; \
    else \
        mkdir /home/graphscope/install; \
        source /home/graphscope/.graphscope_env; \
        make interactive-install BUILD_TYPE="$profile" INSTALL_PREFIX=/home/graphscope/install; \
    fi

############### RUNTIME: frontend #######################
FROM centos:7.9.2009 AS frontend

ENV GRAPHSCOPE_HOME=/opt/graphscope
ENV PATH=$PATH:$GRAPHSCOPE_HOME/bin LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$GRAPHSCOPE_HOME/lib

RUN yum install -y java-1.8.0-openjdk sudo \
    && yum clean all \
    && rm -rf /var/cache/yum

COPY --from=builder /home/graphscope/install/bin/giectl /opt/graphscope/bin/giectl
# vineyard.frontend.properties, log configuration files
COPY --from=builder /home/graphscope/install/conf /opt/graphscope/conf
# jars, libir_core.so
COPY --from=builder /home/graphscope/install/lib /opt/graphscope/lib

RUN chmod +x /opt/graphscope/bin/giectl

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

RUN sudo mkdir -p /var/log/graphscope \
  && sudo chown -R graphscope:graphscope /var/log/graphscope
RUN sudo chmod a+wrx /tmp

USER graphscope
WORKDIR /home/graphscope

############### RUNTIME: executor #######################
FROM $REGISTRY/graphscope/vineyard-runtime:$RUNTIME_VERSION AS executor

ENV GRAPHSCOPE_HOME=/opt/graphscope
ENV PATH=$PATH:$GRAPHSCOPE_HOME/bin LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$GRAPHSCOPE_HOME/lib
ENV RUST_BACKTRACE=1

# gaia_executor, giectl
COPY --from=builder /home/graphscope/install/bin /opt/graphscope/bin
# vineyard.executor.properties, log configuration files
COPY --from=builder /home/graphscope/install/conf /opt/graphscope/conf

RUN sudo chmod +x /opt/graphscope/bin/*
RUN sudo chmod a+wrx /tmp

USER graphscope
WORKDIR /home/graphscope
