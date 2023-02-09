# Learning engine

ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG BUILDER_VERSION=latest
ARG RUNTIME_VERSION=latest
FROM $REGISTRY/graphscope/graphscope-dev:$BUILDER_VERSION AS builder

ARG CI=false

COPY --chown=graphscope:graphscope . /home/graphscope/GraphScope

RUN cd /home/graphscope/GraphScope/ && \
    if [ "${CI}" == "true" ]; then \
        cp -r artifacts/learning /home/graphscope/install; \
    else \
        mkdir /home/graphscope/install; \
        make learning-install INSTALL_PREFIX=/home/graphscope/install; \
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

############### RUNTIME: GLE #######################
FROM $REGISTRY/graphscope/vineyard-runtime:$RUNTIME_VERSION AS learning

USER root

RUN yum install -y centos-release-scl-rh sudo && \
    INSTALL_PKGS="rh-python38-python-pip" && \
    yum install -y --setopt=tsflags=nodocs $INSTALL_PKGS && \
    rpm -V $INSTALL_PKGS && \
    yum -y clean all --enablerepo='*' && \
    rm -rf /var/cache/yum

SHELL [ "/usr/bin/scl", "enable", "rh-python38" ]

ENV GRAPHSCOPE_HOME=/opt/graphscope
ENV PATH=$PATH:$GRAPHSCOPE_HOME/bin LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$GRAPHSCOPE_HOME/lib

COPY --from=builder /home/graphscope/install /opt/graphscope
RUN python3 -m pip install --no-cache-dir /opt/graphscope/*.whl && rm -rf /opt/graphscope/*.whl

USER graphscope
WORKDIR /home/graphscope

ENTRYPOINT ["/bin/bash", "-c", "source scl_source enable rh-python38 && $0 $@"]
