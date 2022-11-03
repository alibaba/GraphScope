# build gie image with artifacts in local directory, skip the compile phase
############### RUNTIME: frontend && executor #######################
FROM centos:7.9.2009 AS experimental

ARG ARTIFACTS_DIR
ADD ${ARTIFACTS_DIR}/gie-artifacts.tar.gz /opt/GraphScope/

RUN yum install -y sudo java-1.8.0-openjdk-devel \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER graphscope

RUN sudo chown -R graphscope:graphscope /opt/GraphScope

WORKDIR /home/graphscope
