ARG BASE_VERSION=v0.2.1
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:$BASE_VERSION as builder

ARG CI=true
ENV CI=$CI

ARG NETWORKX=ON
ENV NETWORKX=$NETWORKX

ARG profile=debug
ENV profile=$profile

COPY . /root/gs
COPY ./interactive_engine/deploy/docker/dockerfile/maven.settings.xml /root/.m2/settings.xml

RUN source ~/.bashrc \
    && echo "build with profile: $profile" \
    && cd /root/gs/interactive_engine \
    && export CMAKE_PREFIX_PATH=/opt/graphscope \
    && export LIBRARY_PATH=$LIBRARY_PATH:/opt/graphscope/lib \
    && export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/graphscope/lib \
    && if [ "$profile" = "release" ]; then \
           echo "release mode" && mvn clean package -DskipTests -Drust.compile.mode=release; \
       else \
           echo "debug mode" && mvn clean package -DskipTests -Drust.compile.mode=debug ; \
       fi

FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest

COPY --from=builder /opt/graphscope /usr/local/
COPY --from=builder /root/gs/interactive_engine/distribution/target/maxgraph.tar.gz /tmp/maxgraph.tar.gz
RUN mkdir -p /home/maxgraph \
    && tar -zxf /tmp/maxgraph.tar.gz -C /home/maxgraph ;

WORKDIR /home/maxgraph/

