# the graphscope-dev-base image is based on manylinux2014, including all necessary
# dependencies except vineyard for graphscope's wheel package.

FROM centos:7 AS builder

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
ENV LC_ALL=en_US.utf-8
ENV LANG=en_US.utf-8
RUN yum install python3-pip -y && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo '$TZ' > /etc/timezone

COPY --chown=graphscope:graphscope . /home/graphscope/GraphScope
<<<<<<< HEAD
RUN cd /home/graphscope/GraphScope && \
    make client && \
    gsctl install-deps dev --cn --for-analytical --no-v6d && \
    cd /home/graphscope && sudo rm -rf /home/graphscope/GraphScope
=======
RUN cd /home/graphscope/GraphScope/python && \
    pip3 install click && pip3 install --editable .&& \
    cd /home/graphscope/GraphScope && \
    gsctl install-deps dev --cn --for-analytical --no-v6d
>>>>>>> d955a407 (modified version)

# install hadoop for processing hadoop data source
RUN if [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
      curl -sS -LO https://archive.apache.org/dist/hadoop/common/hadoop-3.3.0/hadoop-3.3.0-aarch64.tar.gz; \
      tar xzf hadoop-3.3.0-aarch64.tar.gz -C /opt/; \
    else \
      curl -sS -LO https://archive.apache.org/dist/hadoop/common/hadoop-3.3.0/hadoop-3.3.0.tar.gz; \
      tar xzf hadoop-3.3.0.tar.gz -C /opt/; \
    fi && \
    rm -rf hadoop-3.3.0* && \
    cd /opt/hadoop-3.3.0/share/ && \
    rm -rf doc hadoop/client  hadoop/mapreduce  hadoop/tools  hadoop/yarn

FROM centos:7

COPY --from=builder /opt/graphscope /opt/graphscope
COPY --from=builder /opt/openmpi /opt/openmpi
COPY --from=builder /opt/hadoop-3.3.0 /opt/hadoop-3.3.0