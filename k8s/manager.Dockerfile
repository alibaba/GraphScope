# # # # # # # # # # # # # # # # # # # # # #
# COMPILE
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest AS builder

# copy maxgraph code to container
ADD . /root/maxgraph/
# compile maxgraph java
# RUN cd /root/maxgraph/ && mvn clean -T 1 install -DskipTests -P java-release
COPY ./interactive_engine/deploy/docker/dockerfile/maven.settings.xml /root/.m2/settings.xml
RUN cd /root/maxgraph/interactive_engine && \
    mvn clean package -DskipTests -Pjava-release --quiet

# # # # # # # # # # # # # # # # # # # # # #
# RUNTIME: manager
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest AS manager
RUN mkdir -p /home/maxgraph/{bin,conf}
# copy binary and script from builder
COPY --from=builder /root/maxgraph/interactive_engine/src/assembly/target/0.0.1-SNAPSHOT.tar.gz /home/maxgraph/
COPY --from=builder /root/maxgraph/interactive_engine/src/instance-manager/target/0.0.1-SNAPSHOT.tar.gz /home/maxgraph/instance-0.0.1-SNAPSHOT.tar.gz
RUN tar -xf /home/maxgraph/0.0.1-SNAPSHOT.tar.gz -C /home/maxgraph
RUN tar -xf /home/maxgraph/instance-0.0.1-SNAPSHOT.tar.gz -C /home/maxgraph
COPY --from=builder /root/maxgraph/interactive_engine/bin/giectl /home/maxgraph/bin
COPY --from=builder /root/maxgraph/interactive_engine/conf/* /home/maxgraph/conf/

# copy binary from builder
# COPY --from=builder /root/maxgraph/src/api/loader/target/maxgraph-loader-0.0.1-SNAPSHOT-jar-with-dependencies.jar /home/maxgraph/
# TODO: copy benchmark
# COPY --from=builder /root/maxgraph/benchmark/target/benchmark-0.0.1-SNAPSHOT-dist.tar.gz /home/maxgraph/

ENV GRAPHSCOPE_HOME=/home/maxgraph
ENV GRAPHSCOPE_RUNTIME=/tmp/graphscope

WORKDIR /home/maxgraph/
