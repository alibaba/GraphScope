# Coordinator of graphscope engines

ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG BASE_VERSION=v0.10.2
FROM $REGISTRY/graphscope/graphscope-dev:$BASE_VERSION AS builder

ADD . /home/graphscope/GraphScope

RUN sudo chown -R graphscope:graphscope /home/graphscope/GraphScope
RUN cd /home/graphscope/GraphScope \
    && mkdir /home/graphscope/install \
    && make learning-install INSTALL_PREFIX=/home/graphscope/install \
    && python3 -m pip install "numpy==1.18.5" "pandas<1.5.0" "grpcio<=1.43.0,>=1.40.0" "grpcio-tools<=1.43.0,>=1.40.0" wheel \
    && cd /home/graphscope/GraphScope/python \
    && python3 setup.py bdist_wheel \
    && cp dist/*.whl /home/graphscope/install/ \
    && cd /home/graphscope/GraphScope/coordinator \
    && package_name=gs-coordinator python3 setup.py bdist_wheel \
    && cp dist/*.whl /home/graphscope/install/

############### RUNTIME: Coordinator #######################

FROM centos:7.9.2009 AS coordinator

COPY --from=builder /home/graphscope/install /opt/graphscope/

RUN yum install -y centos-release-scl-rh sudo && \
    INSTALL_PKGS="rh-python38-python-pip" && \
    yum install -y --setopt=tsflags=nodocs $INSTALL_PKGS && \
    rpm -V $INSTALL_PKGS && \
    yum -y clean all --enablerepo='*' && \
    rm -rf /var/cache/yum

SHELL [ "/usr/bin/scl", "enable", "rh-python38" ]

RUN python3 -m pip install /opt/graphscope/*.whl && rm -rf /opt/graphscope

COPY ./k8s/utils/kube_ssh /usr/local/bin/kube_ssh

# kubectl v1.19.2
RUN curl -L -o /usr/local/bin/kubectl https://storage.googleapis.com/kubernetes-release/release/v1.19.2/bin/linux/amd64/kubectl
RUN chmod +x /usr/local/bin/kubectl

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

USER graphscope
WORKDIR /home/graphscope

ENTRYPOINT [ "/usr/bin/scl", "enable", "rh-python38", "bash" ]