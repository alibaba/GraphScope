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
    && tar /home/graphscope/graphscope/interactive_engine/assembly/target/groot.tar.gz -C /home/graphscope/

FROM centos:7.9.2009

RUN yum install -y sudo java-1.8.0-openjdk bind-utils \
    && yum clean all \
    && rm -rf /var/cache/yum

COPY --from=builder /home/graphscope/groot/bin /usr/local/groot/bin
COPY --from=builder /home/graphscope/groot/conf /usr/local/groot/conf
COPY --from=builder /home/graphscope/groot/lib /usr/local/groot/lib
COPY --from=builder /home/graphscope/groot/native /usr/local/groot/native

RUN curl -L -o /usr/bin/kubectl https://storage.googleapis.com/kubernetes-release/release/v1.19.2/bin/linux/amd64/kubectl
RUN chmod +x /usr/bin/kubectl

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
RUN sudo chmod a+wrx /tmp

USER graphscope
WORKDIR /home/graphscope

# init log directory
RUN sudo mkdir /var/log/graphscope \
  && sudo chown -R $(id -u):$(id -g) /var/log/graphscope

ENV GRAPHSCOPE_HOME=/usr/local
