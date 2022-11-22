# Learning engine

ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG BASE_VERSION=v0.10.2
FROM $REGISTRY/graphscope/graphscope-dev:$BASE_VERSION AS builder

ADD . /home/graphscope/GraphScope

RUN sudo chown -R graphscope:graphscope /home/graphscope/GraphScope
RUN cd /home/graphscope/GraphScope/ \
    && mkdir /home/graphscope/install \
    && make gle-install INSTALL_PREFIX=/home/graphscope/install \
    && python3 -m pip install "numpy==1.18.5" "pandas<1.5.0" "grpcio<=1.43.0,>=1.40.0" "grpcio-tools<=1.43.0,>=1.40.0" wheel \
    && cd /home/graphscope/GraphScope/python \
    && python3 setup.py bdist_wheel \
    && cp dist/*.whl /home/graphscope/install/ \
    && cd /home/graphscope/GraphScope/coordinator \
    && python3 setup.py bdist_wheel \
    && cp dist/*.whl /home/graphscope/install/

############### RUNTIME: GLE #######################
FROM $REGISTRY/graphscope/vineyard-runtime:$BASE_VERSION AS learning

COPY --from=builder /home/graphscope/install /opt/graphscope

USER root

RUN yum install -y centos-release-scl-rh sudo && \
    INSTALL_PKGS="rh-python38-python-pip" && \
    yum install -y --setopt=tsflags=nodocs $INSTALL_PKGS && \
    rpm -V $INSTALL_PKGS && \
    yum -y clean all --enablerepo='*' && \
    rm -rf /var/cache/yum

SHELL [ "/usr/bin/scl", "enable", "rh-python38" ]

RUN python3 -m pip install /opt/graphscope/*.whl && rm -rf /opt/graphscope/*.whl

ENV GRAPHSCOPE_HOME=/opt/graphscope LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/graphscope/lib

USER graphscope
WORKDIR /home/graphscope

ENTRYPOINT [ "/usr/bin/scl", "enable", "rh-python38", "bash" ]
