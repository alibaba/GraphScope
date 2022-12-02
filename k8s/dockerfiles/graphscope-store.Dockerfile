ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG BASE_VERSION=latest
FROM $REGISTRY/graphscope/graphscope-dev:$BASE_VERSION as builder

ARG CI=false
ENV CI=$CI

ARG profile=debug
ENV profile=$profile

COPY . /home/graphscope/gs
COPY ./interactive_engine/assembly/src/conf/maven.settings.xml /home/graphscope/.m2/settings.xml

USER graphscope

RUN sudo chown -R $(id -u):$(id -g) /home/graphscope/gs /home/graphscope/.m2 && \
    cd /home/graphscope/gs && \
    echo "install cppkafka" \
    && sudo yum update -y && sudo yum install -y librdkafka-devel \
    && git clone -b 0.4.0 --single-branch --depth=1 https://github.com/mfontanini/cppkafka.git /tmp/cppkafka \
    && cd /tmp/cppkafka && git submodule update --init \
    && cmake . && make -j && sudo make install \
    && echo "build with profile: $profile" \
    && source ~/.cargo/env \
    && cd /home/graphscope/gs/interactive_engine \
    && mvn clean package -P groot,groot-assembly -DskipTests --quiet -Drust.compile.mode="$profile" \
    && mv /home/graphscope/gs/interactive_engine/assembly/target/groot.tar.gz /home/graphscope/gs/groot.tar.gz

FROM centos:7.9.2009

RUN yum install -y sudo java-1.8.0-openjdk bind-utils \
    && yum clean all \
    && rm -rf /var/cache/yum

COPY ./k8s/utils/ready_probe.sh /tmp/ready_probe.sh

COPY --from=builder /home/graphscope/gs/groot.tar.gz /tmp/groot.tar.gz
RUN tar -zxf /tmp/groot.tar.gz -C /usr/local && rm /tmp/groot.tar.gz

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER graphscope
WORKDIR /home/graphscope

# init log directory
RUN sudo mkdir /var/log/graphscope \
  && sudo chown -R $(id -u):$(id -g) /var/log/graphscope

ENV GRAPHSCOPE_HOME=/usr/local
