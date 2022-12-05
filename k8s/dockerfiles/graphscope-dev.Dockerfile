# the graphscope-dev image is based on graphscope-dev-base, and will install
# libgrape-lite, vineyard, as well as necessary IO dependencies (e.g., hdfs, oss)

ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG BASE_VERSION=latest
FROM $REGISTRY/graphscope/graphscope-dev-base:$BASE_VERSION

USER root

COPY build_scripts/build_vineyard.sh /build_scripts/build_vineyard.sh

# COPY ./download /download
RUN mkdir /download

ARG VINEYARD_VERSION=main
RUN export WORKDIR=/download && \
    export VINEYARD_VERSION=$VINEYARD_VERSION && \
    bash /build_scripts/build_vineyard.sh

RUN rm -rf /build_scripts /download

USER graphscope
