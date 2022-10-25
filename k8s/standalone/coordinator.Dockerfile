# Coordinator of graphscope engines

ARG BASE_VERSION=v0.9.0
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:$BASE_VERSION AS builder

ADD . /home/graphscope/GraphScope

RUN sudo chown -R graphscope:graphscope /home/graphscope/GraphScope
RUN cd /home/graphscope/GraphScope \
    && git submodule update --init \
    && cd learning_engine/graph-learn \
    && git submodule update --init third_party/pybind11 \
	&& rm -rf cmake-build \
	&& mkdir -p cmake-build \
	&& cd cmake-build \
	&& cmake -DWITH_VINEYARD=ON .. \
	&& make graphlearn_shared -j`nproc` \
	&& export LD_LIBRARY_PATH=`pwd`/built/lib:$LD_LIBRARY_PATH \
    && cd /home/graphscope/GraphScope/python \
    && export PATH=/opt/python/cp38-cp38/bin:$PATH \
    && python3 -m pip install -U pip \
    && python3 -m pip install "numpy==1.18.5" "pandas<1.5.0" "grpcio<=1.43.0,>=1.40.0" "grpcio-tools<=1.43.0,>=1.40.0" wheel \
    && python3 setup.py bdist_wheel \
    && cd /home/graphscope/GraphScope/coordinator \
    && package_name=gs-coordinator python3 setup.py bdist_wheel

############### RUNTIME: Coordinator #######################

FROM centos:7.9.2009 AS coordinator

COPY --from=builder /home/graphscope/GraphScope/coordinator/dist/ /opt/graphscope/dist/
COPY --from=builder /home/graphscope/GraphScope/python/dist/* /opt/graphscope/dist/

RUN yum install -y centos-release-scl-rh sudo && \
    INSTALL_PKGS="devtoolset-10-gcc-c++ rh-python38-python-pip" && \
    yum install -y --setopt=tsflags=nodocs $INSTALL_PKGS && \
    rpm -V $INSTALL_PKGS && \
    yum -y clean all --enablerepo='*' && \
    rm -rf /var/cache/yum

SHELL [ "/usr/bin/scl", "enable", "devtoolset-10", "rh-python38" ]

RUN python3 -m pip install /opt/graphscope/dist/graphscope_client*.whl
RUN python3 -m pip install /opt/graphscope/dist/gs_coordinator*.whl

RUN rm -rf /opt/graphscope/dist

# Enable rh-python, devtoolsets-10
COPY ./k8s/standalone/entrypoint.sh /usr/bin/entrypoint.sh
RUN chmod +x /usr/bin/entrypoint.sh

COPY ./k8s/kube_ssh /usr/local/bin/kube_ssh

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
