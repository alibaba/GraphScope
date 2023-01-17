# The vineyard-dev image including all vineyard-related dependencies
# that could graphscope analytical engine.

FROM centos:7.9.2009

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo '$TZ' > /etc/timezone

# Install java for hadoop
RUN yum install -y centos-release-scl-rh epel-release perl which sudo wget git libunwind-devel java-1.8.0-openjdk && \
    INSTALL_PKGS="devtoolset-10-gcc-c++ rh-python38-python-pip rh-python38-python-devel rapidjson-devel msgpack-devel" && \
    yum install -y --setopt=tsflags=nodocs $INSTALL_PKGS && \
    yum clean all -y --enablerepo='*' && \
    rm -rf /var/cache/yum

SHELL [ "/usr/bin/scl", "enable", "devtoolset-10", "rh-python38" ]
ENV PATH=${PATH}:/opt/rh/devtoolset-10/root/usr/bin:/opt/rh/rh-python38/root/usr/local/bin
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64

# COPY ./download /download
RUN mkdir /download

COPY build_scripts/build_vineyard_dependencies.sh /build_scripts/build_vineyard_dependencies.sh
RUN export WORKDIR=/download && bash /build_scripts/build_vineyard_dependencies.sh

RUN python3 -m pip install --no-cache-dir libclang wheel

ARG VINEYARD_VERSION=main
COPY build_scripts/build_vineyard.sh /build_scripts/build_vineyard.sh
RUN export WORKDIR=/download && \
    export VINEYARD_VERSION=$VINEYARD_VERSION && \
    bash /build_scripts/build_vineyard.sh
RUN rm -rf /build_scripts /download

# install hadoop for processing hadoop data source
RUN cd /tmp && \
    curl -LO https://archive.apache.org/dist/hadoop/core/hadoop-2.8.4/hadoop-2.8.4.tar.gz && \
    tar zxf hadoop-2.8.4.tar.gz -C /usr/local && \
    rm -rf /usr/local/hadoop-2.8.4/share/doc/ && \
    rm -rf hadoop-2.8.4.tar.gz

ENV JAVA_HOME=/usr/lib/jvm/jre-1.8.0 HADOOP_HOME=/usr/local/hadoop-2.8.4 
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
ENV PATH=$PATH:$HADOOP_HOME/bin

# for programming output
RUN localedef -c -f UTF-8 -i en_US en_US.UTF-8
ENV LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8 LANGUAGE=en_US.UTF-8

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

USER graphscope

# set the CLASSPATH for hadoop
RUN bash -l -c 'echo export CLASSPATH="$($HADOOP_HOME/bin/hdfs classpath --glob)" >> /home/graphscope/.profile'
ENV PATH=${PATH}:/home/graphscope/.local/bin

RUN sudo mkdir -p /var/log/graphscope \
  && sudo chown -R graphscope:graphscope /var/log/graphscope

# Enable rh-python, devtoolsets-10 binary
ENTRYPOINT ["/bin/bash", "-c", "source scl_source enable devtoolset-10 rh-python38 && $0 $@"]

