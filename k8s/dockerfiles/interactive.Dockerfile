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
    if [ "${CI}" = "true" ]; then \
        cp -r artifacts/interactive /home/graphscope/install; \
    else \
        mkdir /home/graphscope/install; \
        . /home/graphscope/.graphscope_env; \
        make interactive-install BUILD_TYPE="$profile" INSTALL_PREFIX=/home/graphscope/install; \
    fi

############### RUNTIME: frontend #######################
FROM ubuntu:22.04 AS frontend

ENV GRAPHSCOPE_HOME=/opt/graphscope
ENV PATH=$PATH:$GRAPHSCOPE_HOME/bin LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$GRAPHSCOPE_HOME/lib

RUN apt-get update -y && \
    apt-get install -y sudo default-jdk && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

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
FROM registry.cn-hongkong.aliyuncs.com/graphscope/manylinux2014:20230407-ext AS ext
FROM $REGISTRY/graphscope/vineyard-runtime:$RUNTIME_VERSION AS executor

ENV RUST_BACKTRACE=1

RUN sudo apt-get update -y && \
    sudo apt-get install -y python3-pip default-jdk curl && \
    sudo apt-get clean -y && \
    sudo rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java HADOOP_HOME=/opt/hadoop-3.3.0
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
ENV HADOOP_YARN_HOME=$HADOOP_HOME HADOOP_MAPRED_HOME=$HADOOP_HOME
COPY --from=ext /opt/hadoop-3.3.0 /opt/hadoop-3.3.0
RUN sudo chmod +x /opt/hadoop-3.3.0/bin/*

# set the CLASSPATH for hadoop, must run after install java
RUN bash -l -c 'echo export CLASSPATH="$($HADOOP_HOME/bin/hdfs classpath --glob)" >> /home/graphscope/.profile'

RUN sudo curl -L -o /usr/bin/kubectl https://storage.googleapis.com/kubernetes-release/release/v1.19.2/bin/linux/amd64/kubectl
RUN sudo chmod +x /usr/bin/kubectl

# gaia_executor, giectl
COPY --from=builder /home/graphscope/install/bin /opt/graphscope/bin
# vineyard.executor.properties, log configuration files
COPY --from=builder /home/graphscope/install/conf /opt/graphscope/conf

COPY ./k8s/utils/graphctl.py /usr/local/bin/graphctl.py

RUN sudo chmod +x /opt/graphscope/bin/*
RUN sudo chmod a+wrx /tmp /var/tmp

RUN python3 -m pip install --no-cache-dir vineyard vineyard-io --user

USER graphscope
WORKDIR /home/graphscope


