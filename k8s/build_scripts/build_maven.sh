#!/bin/bash
set -euxo pipefail

echo "Working directory is ${WORKDIR:="/tmp"}"

cd ${WORKDIR} && mkdir /opt/maven && \
    curl -fsSLO https://dlcdn.apache.org/maven/maven-3/3.8.6/binaries/apache-maven-3.8.6-bin.tar.gz && \
    tar xzf ${WORKDIR}/apache-maven-3.8.6-bin.tar.gz -C /opt/ && \
    rm -f ${WORKDIR}/apache-maven-3.8.6-bin.tar.gz
