# The vineyard-runtime image including all vineyard-related
# dependencies that could graphscope interactive engine.

ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG BUILDER_VERSION=latest
FROM $REGISTRY/graphscope/vineyard-dev:$BUILDER_VERSION AS builder

FROM ubuntu:22.04 AS runtime

ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib:/usr/local/lib64:/opt/graphscope/lib:/opt/graphscope/lib64

RUN apt-get update -y && \
    apt-get install --no-install-recommends -y sudo \
                       libunwind-dev \
                       libgomp1 \
                       openmpi-bin \
                       libgflags-dev \
                       libgoogle-glog-dev \
                       libboost-filesystem-dev \
                       wget \
                       ca-certificates && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update -y && \
    # shellcheck disable=SC2046,SC2019,SC2018
    wget -c https://apache.jfrog.io/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-jammy.deb \
        -P /tmp/ --no-check-certificate && \
    sudo apt-get install -y -V /tmp/apache-arrow-apt-source-latest-jammy.deb && \
    sudo apt-get update -y && \
    sudo apt-get install -y libarrow-dev && \
    rm /tmp/apache-arrow-apt-source-latest-*.deb && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

ENV GRAPHSCOPE_HOME=/opt/graphscope
ENV PATH=$PATH:$GRAPHSCOPE_HOME/bin:/opt/vineyard/bin:/home/graphscope/.local/bin
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib:/usr/local/lib64
ENV OMPI_MCA_plm_rsh_agent=/usr/local/bin/kube_ssh

COPY ./utils/kube_ssh /usr/local/bin/kube_ssh

RUN chmod a+wrx /tmp

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

RUN mkdir -p /var/log/graphscope && chown -R graphscope:graphscope /var/log/graphscope
COPY --from=builder /opt/graphscope /opt/graphscope
COPY --from=builder /opt/vineyard /opt/vineyard

USER graphscope
WORKDIR /home/graphscope
