FROM ubuntu:22.04

RUN apt-get update -y && \
    apt-get install -y sudo default-jdk && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

ADD artifacts/groot.tar.gz /usr/local

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER graphscope
WORKDIR /home/graphscope

# init log directory
RUN sudo mkdir /var/log/graphscope \
  && sudo chown -R $(id -u):$(id -g) /var/log/graphscope

ENV GRAPHSCOPE_HOME=/usr/local
