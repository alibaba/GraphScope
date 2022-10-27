# Interactive engine which uses experimental storage

ARG BASE_VERSION=v0.9.0
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:$BASE_VERSION AS builder

ARG profile=release
ENV profile=$profile
ADD . /home/graphscope/GraphScope

ENV PATH="/home/graphscope/.cargo/bin:$PATH"

RUN sudo chown -R graphscope:graphscope /home/graphscope/GraphScope
RUN cd /home/graphscope/GraphScope/interactive_engine/compiler \
    && make build rpc.target=start_rpc_server_k8s

############### RUNTIME: frontend && executor #######################
FROM centos:7.9.2009 AS experimental

COPY --from=builder /home/graphscope/GraphScope/interactive_engine/compiler/target/libs /opt/GraphScope/interactive_engine/compiler/target/libs
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/compiler/target/compiler-1.0-SNAPSHOT.jar /opt/GraphScope/interactive_engine/compiler/target/compiler-1.0-SNAPSHOT.jar
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/compiler/conf /opt/GraphScope/interactive_engine/compiler/conf
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/compiler/set_properties.sh /opt/GraphScope/interactive_engine/compiler/set_properties.sh
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/executor/ir/target/release/libir_core.so /opt/GraphScope/interactive_engine/executor/ir/target/release/libir_core.so
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/executor/ir/target/release/start_rpc_server_k8s /opt/GraphScope/interactive_engine/executor/ir/target/release/start_rpc_server_k8s

RUN yum install -y sudo java-1.8.0-openjdk-devel \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER graphscope

RUN sudo chown -R graphscope:graphscope /opt/GraphScope

WORKDIR /home/graphscope

