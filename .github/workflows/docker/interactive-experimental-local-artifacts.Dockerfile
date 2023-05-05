# build gie image with artifacts in local directory, skip the compile phase
############### RUNTIME: frontend && executor #######################
FROM ubuntu:22.04

ADD artifacts/artifacts.tar.gz /opt/graphscope/

RUN apt-get update -y && \
    apt-get install -y sudo default-jdk && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

RUN chown -R graphscope:graphscope /opt/graphscope

USER graphscope
WORKDIR /home/graphscope
