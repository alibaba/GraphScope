# The vineyard-dev image including all vineyard-related dependencies
# that could graphscope analytical engine.
ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
FROM $REGISTRY/graphscope/manylinux2014:2022-12-12-ext AS ext
FROM centos:7.9.2009

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo '$TZ' > /etc/timezone

# for programming output
RUN localedef -c -f UTF-8 -i en_US en_US.UTF-8
ENV LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8 LANGUAGE=en_US.UTF-8

ENV GRAPHSCOPE_HOME=/opt/graphscope
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:$GRAPHSCOPE_HOME/lib:$GRAPHSCOPE_HOME/lib64

ENV JAVA_HOME=/usr/lib/jvm/java HADOOP_HOME=/opt/hadoop-3.3.0 
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
ENV HADOOP_YARN_HOME=$HADOOP_HOME HADOOP_MAPRED_HOME=$HADOOP_HOME
ENV PATH=$PATH:/opt/rh/devtoolset-8/root/usr/bin:/opt/rh/rh-python38/root/usr/local/bin
ENV PATH=$PATH:$GRAPHSCOPE_HOME/bin:$HADOOP_HOME/bin:/home/graphscope/.local/bin

# Copy the thirdparty c++ dependencies, maven, and hadoop
COPY --from=ext /opt/graphscope /opt/graphscope
COPY --from=ext /opt/openmpi /opt/openmpi
COPY --from=ext /opt/hadoop-3.3.0 /opt/hadoop-3.3.0
RUN chmod +x /opt/graphscope/bin/* /opt/openmpi/bin/* /opt/hadoop-3.3.0/bin/*

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

RUN mkdir -p /var/log/graphscope
RUN chown -R graphscope:graphscope /var/log/graphscope /opt/graphscope /opt/openmpi
RUN mkdir /opt/vineyard && chown -R graphscope:graphscope /opt/vineyard

RUN yum install -y sudo && \
    yum clean all -y --enablerepo='*' && \
    rm -rf /var/cache/yum

USER graphscope
WORKDIR /home/graphscope

COPY ./gs ./gs
ARG VINEYARD_VERSION=main
RUN sudo chmod a+wrx /tmp && \
    ./gs install-deps dev --for-analytical --v6d-version=$VINEYARD_VERSION && \
    sudo yum clean all -y && \
    sudo rm -fr /var/cache/yum

# set the CLASSPATH for hadoop, must run after install java
RUN bash -l -c 'echo export CLASSPATH="$($HADOOP_HOME/bin/hdfs classpath --glob)" >> /home/graphscope/.profile'

# Enable rh-python, devtoolsets-8 binary
ENTRYPOINT ["/bin/bash", "-c", "source scl_source enable devtoolset-8 rh-python38 && $0 $@"]
