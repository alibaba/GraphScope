# Coordinator of graphscope engines

ARG BASE_VERSION=v0.9.0
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:$BASE_VERSION AS builder

ADD . /home/graphscope/GraphScope

RUN sudo chown -R graphscope:graphscope /home/graphscope/GraphScope
RUN source $HOME/.bashrc \
    && cd /home/graphscope/GraphScope/coordinator \
    && export PATH=/opt/python/cp38-cp38/bin:$PATH \
    && python3 setup.py bdist_wheel

############### RUNTIME: Coordinator #######################

FROM centos:7.9.2009 AS coordinator

COPY --from=builder /home/graphscope/Graphscope/coordinator/dist/ /opt/graphscope/dist

RUN yum install -y centos-release-scl-rh sudo && \
    INSTALL_PKGS="devtoolset-10-gcc-c++ rh-python38-python-pip" && \
    yum install -y --setopt=tsflags=nodocs $INSTALL_PKGS && \
    rpm -V $INSTALL_PKGS && \
    yum -y clean all --enablerepo='*' && \
    rm -rf /var/cache/yum

SHELL [ "/usr/bin/scl", "enable", "devtoolset-10", "rh-python38" ]

RUN python3 -m pip install /opt/graphscope/dist/*.whl

RUN rm -rf /opt/graphscope/dist

# Enable rh-python, devtoolsets-10
COPY entrypoint.sh /usr/bin/entrypoint.sh
RUN chmod +x /usr/bin/entrypoint.sh

# kubectl v1.19.2
RUN curl -L -o /usr/local/bin/kubectl https://storage.googleapis.com/kubernetes-release/release/v1.19.2/bin/linux/amd64/kubectl

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo '$TZ' > /etc/timezone

# for programming output
RUN localedef -c -f UTF-8 -i en_US en_US.UTF-8
ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

USER graphscope
WORKDIR /home/graphscope

ENTRYPOINT [ "/usr/bin/entrypoint.sh" ]