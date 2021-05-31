FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest
RUN mkdir -p /home/maxgraph
# copy binary from builder
COPY ./interactive_engine/src/assembly/target/0.0.1-SNAPSHOT.tar.gz /home/maxgraph/
COPY ./interactive_engine/src/instance-manager/target/0.0.1-SNAPSHOT.tar.gz /home/maxgraph/instance-0.0.1-SNAPSHOT.tar.gz
# copy start script from builder
COPY ./interactive_engine/deploy/docker/dockerfile/coordinator-entrypoint.sh /home/maxgraph/coordinator-entrypoint.sh
RUN mkdir -p /home/maxgraph/config
COPY ./interactive_engine/deploy/docker/dockerfile/coordinator.application.properties /home/maxgraph/config/coordinator.application.properties

COPY ./interactive_engine/deploy/docker/dockerfile/frontend-entrypoint.sh /home/maxgraph/frontend-entrypoint.sh
COPY ./interactive_engine/deploy/docker/dockerfile/frontend.vineyard.properties /home/maxgraph/config/frontend.application.properties
COPY ./interactive_engine/deploy/docker/dockerfile/manager-entrypoint.sh /home/maxgraph/manager-entrypoint.sh

COPY ./interactive_engine/deploy/docker/dockerfile/create_maxgraph_instance.sh  /root/maxgraph/create_maxgraph_instance.sh
COPY ./interactive_engine/deploy/docker/dockerfile/delete_maxgraph_instance.sh /root/maxgraph/delete_maxgraph_instance.sh
COPY ./interactive_engine/deploy/docker/dockerfile/kill_process.sh /root/maxgraph/kill_process.sh
COPY ./interactive_engine/deploy/docker/dockerfile/set_config.sh /root/maxgraph/set_config.sh
COPY ./interactive_engine/deploy/docker/dockerfile/func.sh /root/maxgraph/func.sh
COPY ./interactive_engine/deploy/docker/dockerfile/pod.yaml /root/maxgraph/pod.yaml
COPY ./interactive_engine/deploy/docker/dockerfile/expose_gremlin_server.sh /root/maxgraph/expose_gremlin_server.sh

