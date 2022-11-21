# the graphscope-dev image is based on graphscope-dev-base, and will install
# libgrape-lite, vineyard, as well as necessary IO dependencies (e.g., hdfs, oss)

ARG BASE_VERSION=latest
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev-base:$BASE_VERSION

USER root

# COPY ./download /download
RUN mkdir /download
COPY build_scripts/build_vineyard.sh /build_scripts/build_vineyard.sh

RUN export WORKDIR=/download && bash /build_scripts/build_vineyard.sh

RUN rm -rf /build_scripts /download

USER graphscope
