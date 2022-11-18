FROM centos:7.9.2009

RUN yum install -y sudo java-1.8.0-openjdk-devel bind-utils \
    && yum clean all \
    && rm -rf /var/cache/yum

COPY k8s/ready_probe.sh /tmp/ready_probe.sh
ADD artifacts/groot.tar.gz /usr/local

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER graphscope
WORKDIR /home/graphscope

# init log directory
RUN sudo mkdir /var/log/graphscope \
  && sudo chown -R $(id -u):$(id -g) /var/log/graphscope

ENV GRAPHSCOPE_HOME=/usr/local
