ARG BASE_VERSION=v0.2.6
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:$BASE_VERSION as builder

ARG CI=true
ENV CI=$CI

ARG NETWORKX=ON
ENV NETWORKX=$NETWORKX

ARG profile=debug
ENV profile=$profile

COPY . /home/graphscope/gs

RUN wget --no-verbose https://golang.org/dl/go1.15.5.linux-amd64.tar.gz && \
    sudo tar -C /usr/local -xzf go1.15.5.linux-amd64.tar.gz && \
    curl -sf -L https://static.rust-lang.org/rustup.sh | \
        sh -s -- -y --profile minimal --default-toolchain 1.54.0 && \
    echo "source ~/.cargo/env" >> ~/.bashrc \
    && source ~/.bashrc \
    && rustup component add rustfmt \
    && echo "build with profile: $profile" \
    && cd /home/graphscope/gs/interactive_engine \
    && export CMAKE_PREFIX_PATH=/opt/graphscope \
    && export LIBRARY_PATH=$LIBRARY_PATH:/opt/graphscope/lib \
    && export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/graphscope/lib \
    && if [ "$profile" = "release" ]; then \
           echo "release mode" && mvn clean package -Pv2 -DskipTests -Drust.compile.mode=release; \
       else \
           echo "debug mode" && mvn clean package -Pv2 -DskipTests -Drust.compile.mode=debug ; \
       fi

FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest

COPY ./k8s/ready_probe.sh /tmp/ready_probe.sh
COPY --from=builder /opt/graphscope /usr/local/
COPY --from=builder /home/graphscope/gs/interactive_engine/distribution/target/maxgraph.tar.gz /tmp/maxgraph.tar.gz
RUN sudo tar -zxf /tmp/maxgraph.tar.gz -C /usr/local
COPY --from=builder /home/graphscope/gs/interactive_engine/bin/giectl /usr/local/bin/giectl
COPY --from=builder /home/graphscope/gs/interactive_engine/conf/* /usr/local/conf/

ENV GRAPHSCOPE_HOME=/usr/local
