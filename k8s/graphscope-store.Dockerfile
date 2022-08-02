ARG BASE_VERSION=v0.6.0
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:$BASE_VERSION as builder

ARG CI=false
ENV CI=$CI

ARG profile=debug
ENV profile=$profile

COPY . /home/graphscope/gs
COPY ./interactive_engine/assembly/conf/maven.settings.xml /home/graphscope/.m2/settings.xml

USER graphscope

RUN sudo chown -R $(id -u):$(id -g) /home/graphscope/gs /home/graphscope/.m2 && \
    cd /home/graphscope/gs && \
    if [ "${CI}" == "true" ]; then \
        mv artifacts/maxgraph.tar.gz ./maxgraph.tar.gz; \
    else \
        echo "install cppkafka" \
        && sudo yum update -y && sudo yum install -y librdkafka-devel \
        && git clone -b 0.4.0 --single-branch --depth=1 https://github.com/mfontanini/cppkafka.git /tmp/cppkafka \
        && cd /tmp/cppkafka && git submodule update --init \
        && cmake . && make -j && sudo make install \
        && echo "build with profile: $profile" \
        && source ~/.cargo/env \
        && cd /home/graphscope/gs/interactive_engine \
        && mvn clean package -Pv2 -DskipTests --quiet -Drust.compile.mode="$profile" \
        && mv /home/graphscope/gs/interactive_engine/distribution/target/maxgraph.tar.gz /home/graphscope/gs/maxgraph.tar.gz; \
    fi

FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest

COPY --from=builder /opt/vineyard/ /usr/local/

COPY ./k8s/ready_probe.sh /tmp/ready_probe.sh
COPY --from=builder /home/graphscope/gs/maxgraph.tar.gz /tmp/maxgraph.tar.gz
RUN sudo tar -zxf /tmp/maxgraph.tar.gz -C /usr/local
RUN rm /tmp/maxgraph.tar.gz

# init log directory
RUN sudo mkdir /var/log/graphscope \
  && sudo chown -R $(id -u):$(id -g) /var/log/graphscope

ENV GRAPHSCOPE_HOME=/usr/local
