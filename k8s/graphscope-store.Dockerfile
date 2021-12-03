ARG BASE_VERSION=v0.3.11
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:$BASE_VERSION as builder

ARG CI=true
ENV CI=$CI

ARG NETWORKX=ON
ENV NETWORKX=$NETWORKX

ARG profile=debug
ENV profile=$profile

COPY . /home/graphscope/gs
COPY ./interactive_engine/deploy/docker/dockerfile/maven.settings.xml /home/graphscope/.m2/settings.xml

RUN sudo chown -R $(id -u):$(id -g) /home/graphscope/gs /home/graphscope/.m2 && \
    wget --no-verbose https://golang.org/dl/go1.15.5.linux-amd64.tar.gz && \
    sudo tar -C /usr/local -xzf go1.15.5.linux-amd64.tar.gz && \
    curl -sf -L https://static.rust-lang.org/rustup.sh | \
        sh -s -- -y --profile minimal --default-toolchain 1.54.0 && \
    echo "source ~/.cargo/env" >> ~/.bashrc \
    && source ~/.bashrc \
    && rustup component add rustfmt \
    && echo "install cppkafka" \
    && sudo yum update -y && sudo yum install -y librdkafka-devel \
    && git clone -b 0.4.0 --single-branch --depth=1 https://github.com/mfontanini/cppkafka.git /tmp/cppkafka \
    && cd /tmp/cppkafka && git submodule update --init \
    && mkdir -p build && cd build \
    && cmake .. && make -j && sudo make install \
    && rm -fr /tmp/cppkafka \
    && echo "build with profile: $profile" \
    && cd /home/graphscope/gs/interactive_engine \
    && if [ "$profile" = "release" ]; then \
          echo "release mode"; \
          for i in {1..5}; do mvn clean package -Pv2 -DskipTests --quiet -Drust.compile.mode=release && break || sleep 60; done; \
       else \
          echo "debug mode"; \
          for i in {1..5}; do mvn clean package -Pv2 -DskipTests --quiet -Drust.compile.mode=debug && break || sleep 60; done; \
       fi

FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest

COPY --from=builder /opt/vineyard/ /usr/local/

COPY ./k8s/ready_probe.sh /tmp/ready_probe.sh
COPY --from=builder /home/graphscope/gs/interactive_engine/distribution/target/maxgraph.tar.gz /tmp/maxgraph.tar.gz
RUN sudo tar -zxf /tmp/maxgraph.tar.gz -C /usr/local

# init log directory
RUN sudo mkdir /var/log/graphscope \
  && sudo chown -R $(id -u):$(id -g) /var/log/graphscope

ENV GRAPHSCOPE_HOME=/usr/local
