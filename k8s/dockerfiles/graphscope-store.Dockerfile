ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG BUILDER_VERSION=latest
FROM $REGISTRY/graphscope/graphscope-dev:$BUILDER_VERSION as builder

ARG CI=false

ARG profile=debug
ENV profile=$profile

COPY --chown=graphscope:graphscope . /home/graphscope/graphscope

COPY --chown=graphscope:graphscope ./interactive_engine/assembly/src/conf/maven.settings.xml /home/graphscope/.m2/settings.xml

USER graphscope

RUN cd /home/graphscope/graphscope \
    && source ~/.graphscope_env \
    && cd /home/graphscope/graphscope/interactive_engine \
    && mvn clean package -P groot,groot-assembly -DskipTests --quiet -Drust.compile.mode="$profile" \
    && mv /home/graphscope/graphscope/interactive_engine/assembly/target/groot.tar.gz /home/graphscope/graphscope/groot.tar.gz

FROM centos:7.9.2009

RUN yum install -y sudo java-1.8.0-openjdk bind-utils \
    && yum clean all \
    && rm -rf /var/cache/yum

COPY --from=builder /home/graphscope/graphscope/groot.tar.gz /tmp/groot.tar.gz
RUN tar -zxf /tmp/groot.tar.gz -C /usr/local && rm /tmp/groot.tar.gz

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
RUN sudo chmod a+wrx /tmp

USER graphscope
WORKDIR /home/graphscope

# init log directory
RUN sudo mkdir /var/log/graphscope \
  && sudo chown -R $(id -u):$(id -g) /var/log/graphscope

ENV GRAPHSCOPE_HOME=/usr/local
