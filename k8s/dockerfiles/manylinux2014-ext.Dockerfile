# the graphscope-dev-base image is based on manylinux2014, including all necessary
# dependencies except vineyard for graphscope's wheel package.

FROM centos:7 AS builder


# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN yum install sudo -y && \
    yum update glibc-common -y  && \
    sudo localedef -i en_US -f UTF-8 en_US.UTF-8 && \    
    yum install python3-pip -y && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo '$TZ' > /etc/timezone

ENV LC_ALL=en_US.utf-8
ENV LANG=en_US.utf-8

COPY gsctl ./gsctl
RUN cd ./gsctl && \
    python3 -m pip install click && \ 
    python3 gsctl.py install-deps dev --cn --for-analytical --no-v6d  -j $(nproc) && \
    rm -fr /root/gsctl

# install hadoop for processing hadoop data source
RUN if [ "$(uname -m)" = "aarch64" ]; then \
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
