# The vineyard-dev image including all vineyard-related dependencies
# that could compile graphscope analytical engine.
ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
FROM $REGISTRY/graphscope/manylinux2014:20230407-ext AS ext
FROM ubuntu:22.04

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo '$TZ' > /etc/timezone

ENV GRAPHSCOPE_HOME=/opt/graphscope
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:$GRAPHSCOPE_HOME/lib:$GRAPHSCOPE_HOME/lib64

ENV JAVA_HOME=/usr/lib/jvm/default-java HADOOP_HOME=/opt/hadoop-3.3.0
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
ENV HADOOP_YARN_HOME=$HADOOP_HOME HADOOP_MAPRED_HOME=$HADOOP_HOME
ENV PATH=$PATH:$GRAPHSCOPE_HOME/bin:$HADOOP_HOME/bin:/home/graphscope/.local/bin

# Copy hadoop
COPY --from=ext /opt/hadoop-3.3.0 /opt/hadoop-3.3.0

RUN apt-get update && \
    apt-get install -y sudo default-jre && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

RUN mkdir /opt/graphscope /opt/vineyard && chown -R graphscope:graphscope /opt/graphscope /opt/vineyard
# For output logs
RUN mkdir -p /var/log/graphscope && chown -R graphscope:graphscope /var/log/graphscope

USER graphscope
WORKDIR /home/graphscope

COPY ./gs ./gs
ARG VINEYARD_VERSION=main
RUN sudo chmod a+wrx /tmp && \
    ./gs install-deps dev --for-analytical --v6d-version=$VINEYARD_VERSION -j 2 && \
    sudo apt-get clean -y && \
    sudo rm -rf /var/lib/apt/lists/*

RUN python3 -m pip --no-cache install pyyaml --user

# set the CLASSPATH for hadoop, must run after install java
RUN bash -l -c 'echo export CLASSPATH="$($HADOOP_HOME/bin/hdfs classpath --glob)" >> /home/graphscope/.profile'
