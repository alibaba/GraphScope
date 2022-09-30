ARG BASE_VERSION=v0.9.0
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:$BASE_VERSION as builder

ARG CI=false
ENV CI=$CI

ARG profile=debug
ENV profile=$profile

COPY . /home/graphscope/gs
COPY ./interactive_engine/assembly/src/conf/maven.settings.xml /home/graphscope/.m2/settings.xml

USER graphscope

RUN sudo chown -R $(id -u):$(id -g) /home/graphscope/gs /home/graphscope/.m2 && \
    cd /home/graphscope/gs && \
    if [ "${CI}" == "true" ]; then \
        mv artifacts/groot.tar.gz ./groot.tar.gz; \
    else \
        echo "install cppkafka" \
        && sudo yum update -y && sudo yum install -y librdkafka-devel \
        && git clone -b 0.4.0 --single-branch --depth=1 https://github.com/mfontanini/cppkafka.git /tmp/cppkafka \
        && cd /tmp/cppkafka && git submodule update --init \
        && cmake . && make -j && sudo make install \
        && echo "build with profile: $profile" \
        && source ~/.cargo/env \
        && cd /home/graphscope/gs/interactive_engine \
        && mvn clean package -P groot,groot-assembly -DskipTests --quiet -Drust.compile.mode="$profile" \
        && mv /home/graphscope/gs/interactive_engine/assembly/target/groot.tar.gz /home/graphscope/gs/groot.tar.gz; \
    fi

FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest

COPY --from=builder /opt/vineyard/ /usr/local/

COPY ./k8s/ready_probe.sh /tmp/ready_probe.sh
COPY --from=builder /home/graphscope/gs/groot.tar.gz /tmp/groot.tar.gz
RUN sudo tar -zxf /tmp/groot.tar.gz -C /usr/local
RUN rm /tmp/groot.tar.gz

# init log directory
RUN sudo mkdir /var/log/graphscope \
  && sudo chown -R $(id -u):$(id -g) /var/log/graphscope

ENV GRAPHSCOPE_HOME=/usr/local
