# Coordinator of graphscope engines

ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG BUILDER_VERSION=latest
FROM $REGISTRY/graphscope/graphscope-dev:$BUILDER_VERSION AS builder

ARG CI=false

COPY --chown=graphscope:graphscope . /home/graphscope/GraphScope

RUN cd /home/graphscope/GraphScope/ && \
    if [ "${CI}" == "true" ]; then \
        cp -r artifacts/learning /home/graphscope/install; \
    else \
        mkdir /home/graphscope/install; \
        make learning-install INSTALL_PREFIX=/home/graphscope/install; \
        source /home/graphscope/.graphscope_env; \
        python3 -m pip install "numpy==1.18.5" "pandas<1.5.0" "grpcio<=1.43.0,>=1.40.0" "grpcio-tools<=1.43.0,>=1.40.0" wheel; \
        cd /home/graphscope/GraphScope/python; \
        python3 setup.py bdist_wheel; \
        export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/graphscope/GraphScope/learning_engine/graph-learn/graphlearn/built/lib; \
        auditwheel repair --plat=manylinux2014_x86_64 dist/*.whl; \
        cp wheelhouse/*.whl /home/graphscope/install/; \
        cd /home/graphscope/GraphScope/coordinator; \
        python3 setup.py bdist_wheel; \
        cp dist/*.whl /home/graphscope/install/; \
    fi

############### RUNTIME: Coordinator #######################

FROM centos:7.9.2009 AS coordinator

RUN yum install -y centos-release-scl-rh sudo && \
    INSTALL_PKGS="rh-python38-python-pip" && \
    yum install -y --setopt=tsflags=nodocs $INSTALL_PKGS && \
    rpm -V $INSTALL_PKGS && \
    yum -y clean all --enablerepo='*' && \
    rm -rf /var/cache/yum

SHELL [ "/usr/bin/scl", "enable", "rh-python38" ]

ENV GRAPHSCOPE_HOME=/opt/graphscope
ENV PATH=$PATH:/opt/openmpi/bin
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/openmpi/lib

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

RUN sudo mkdir -p /var/log/graphscope \
  && sudo chown -R graphscope:graphscope /var/log/graphscope

# kubectl v1.19.2
RUN curl -L -o /usr/local/bin/kubectl https://storage.googleapis.com/kubernetes-release/release/v1.19.2/bin/linux/amd64/kubectl
RUN chmod +x /usr/local/bin/kubectl

COPY --from=builder /home/graphscope/install /opt/graphscope/
RUN python3 -m pip install --no-cache-dir /opt/graphscope/*.whl && rm -rf /opt/graphscope/
COPY --from=builder /opt/openmpi /opt/openmpi

COPY ./interactive_engine/assembly/src/bin/graphscope/giectl /opt/graphscope/bin/giectl
COPY ./k8s/utils/kube_ssh /usr/local/bin/kube_ssh

USER graphscope
WORKDIR /home/graphscope

ENTRYPOINT ["/bin/bash", "-c", "source scl_source enable rh-python38 && $0 $@"]
