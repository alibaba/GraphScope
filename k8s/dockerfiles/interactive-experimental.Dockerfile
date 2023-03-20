# Interactive engine which uses experimental storage

ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG BUILDER_VERSION=latest
FROM $REGISTRY/graphscope/graphscope-dev:$BUILDER_VERSION AS builder

COPY --chown=graphscope:graphscope . /home/graphscope/GraphScope

RUN cd /home/graphscope/GraphScope/interactive_engine/compiler \
    && . /home/graphscope/.graphscope_env \
    && make build rpc.target=start_rpc_server_k8s

############### RUNTIME: frontend && executor #######################
FROM ubuntu:22.04 AS experimental

COPY --from=builder /home/graphscope/GraphScope/interactive_engine/compiler/target/libs /opt/graphscope/interactive_engine/compiler/target/libs
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/compiler/target/compiler-1.0-SNAPSHOT.jar /opt/graphscope/interactive_engine/compiler/target/compiler-1.0-SNAPSHOT.jar
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/compiler/conf /opt/graphscope/interactive_engine/compiler/conf
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/compiler/set_properties.sh /opt/graphscope/interactive_engine/compiler/set_properties.sh
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/executor/ir/target/release/libir_core.so /opt/graphscope/interactive_engine/executor/ir/target/release/libir_core.so
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/executor/ir/target/release/start_rpc_server_k8s /opt/graphscope/interactive_engine/executor/ir/target/release/start_rpc_server_k8s

RUN sudo apt-get update -y && \
    sudo apt-get install -y default-jdk && \
    sudo apt-get clean -y && \
    sudo rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN sudo chmod a+wrx /tmp

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

RUN sudo chown -R graphscope:graphscope /opt/graphscope

USER graphscope
WORKDIR /home/graphscope

