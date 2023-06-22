FROM ubuntu:20.04
ARG CI=false

# change bash as default
SHELL ["/bin/bash", "-c"]


RUN apt update && apt -y install locales && locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

#install protobuf
RUN apt-get install -y protobuf-compiler libprotobuf-dev

# install java and maven
RUN apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install default-jdk -y && \
    apt-get install maven -y

# install rust
RUN curl -sf -L https://static.rust-lang.org/rustup.sh | sh -s -- -y --profile minimal && \
    source "$HOME/.cargo/env"