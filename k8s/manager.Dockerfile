# # # # # # # # # # # # # # # # # # # # # #
# COMPILE
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:debug AS builder

# copy maxgraph code to container
ADD . /root/maxgraph/
# compile maxgraph java
# RUN cd /root/maxgraph/ && mvn clean -T 1 install -DskipTests -P java-release
COPY ./deploy/docker/dockerfile/maven.settings.xml /root/.m2/settings.xml
RUN cd /root/maxgraph/ && \
    mvn clean package -DskipTests -Pjava-release --quiet

# # # # # # # # # # # # # # # # # # # # # #
# RUNTIME: manager 
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:debug AS manager
RUN mkdir -p /home/maxgraph
# copy binary from builder
COPY --from=builder /root/maxgraph/src/assembly/target/0.0.1-SNAPSHOT.tar.gz /home/maxgraph/
COPY --from=builder /root/maxgraph/src/instance-manager/target/0.0.1-SNAPSHOT.tar.gz /home/maxgraph/instance-0.0.1-SNAPSHOT.tar.gz
# copy start script from builder
COPY ./deploy/docker/dockerfile/coordinator-entrypoint.sh /home/maxgraph/coordinator-entrypoint.sh
RUN mkdir -p /home/maxgraph/config
COPY ./deploy/docker/dockerfile/coordinator.application.properties /home/maxgraph/config/coordinator.application.properties

# copy binary from builder
# COPY --from=builder /root/maxgraph/src/api/loader/target/maxgraph-loader-0.0.1-SNAPSHOT-jar-with-dependencies.jar /home/maxgraph/
# TODO: copy benchmark
# COPY --from=builder /root/maxgraph/benchmark/target/benchmark-0.0.1-SNAPSHOT-dist.tar.gz /home/maxgraph/
# copy start script from builder
COPY ./deploy/docker/dockerfile/frontend-entrypoint.sh /home/maxgraph/frontend-entrypoint.sh
COPY ./deploy/docker/dockerfile/frontend.vineyard.properties /home/maxgraph/config/frontend.application.properties
COPY ./deploy/docker/dockerfile/manager-entrypoint.sh /home/maxgraph/manager-entrypoint.sh

COPY ./deploy/docker/dockerfile/create_maxgraph_instance.sh  /root/maxgraph/create_maxgraph_instance.sh
COPY ./deploy/docker/dockerfile/delete_maxgraph_instance.sh /root/maxgraph/delete_maxgraph_instance.sh
COPY ./deploy/docker/dockerfile/kill_process.sh /root/maxgraph/kill_process.sh
COPY ./deploy/docker/dockerfile/set_config.sh /root/maxgraph/set_config.sh
COPY ./deploy/docker/dockerfile/func.sh /root/maxgraph/func.sh
COPY ./deploy/docker/dockerfile/pod.yaml /root/maxgraph/pod.yaml
COPY ./deploy/docker/dockerfile/expose_gremlin_server.sh /root/maxgraph/expose_gremlin_server.sh

WORKDIR /home/maxgraph/
