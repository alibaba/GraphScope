# Coordinator of graphscope engines

ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG BUILDER_VERSION=latest
FROM $REGISTRY/graphscope/graphscope-dev:$BUILDER_VERSION AS builder

ARG CI=false

COPY --chown=graphscope:graphscope . /home/graphscope/GraphScope

RUN cd /home/graphscope/GraphScope/ && \
    if [ "${CI}" = "true" ]; then \
        cp -r artifacts/learning /home/graphscope/install; \
    else \
        . /home/graphscope/.graphscope_env; \
        mkdir /home/graphscope/install; \
        make learning-install INSTALL_PREFIX=/home/graphscope/install; \
        cd python; \
        python3 -m pip install --user -r requirements.txt; \
        python3 setup.py bdist_wheel; \
        export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/graphscope/GraphScope/learning_engine/graph-learn/graphlearn/built/lib; \
        auditwheel repair dist/*.whl; \
        python3 -m pip install wheelhouse/*.whl; \
        cp wheelhouse/*.whl /home/graphscope/install/; \
        cd ../coordinator; \
        python3 setup.py bdist_wheel; \
        cp dist/*.whl /home/graphscope/install/; \
    fi

############### RUNTIME: Coordinator #######################

FROM ubuntu:22.04 AS coordinator

RUN apt-get update -y && \
    apt-get install -y sudo python3-pip openmpi-bin curl && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

ENV GRAPHSCOPE_HOME=/opt/graphscope

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

RUN sudo mkdir -p /var/log/graphscope \
  && sudo chown -R graphscope:graphscope /var/log/graphscope

RUN curl -L -o /usr/bin/kubectl https://storage.googleapis.com/kubernetes-release/release/v1.24.0/bin/linux/amd64/kubectl
RUN chmod +x /usr/bin/kubectl

COPY ./interactive_engine/assembly/src/bin/graphscope/giectl /opt/graphscope/bin/giectl
COPY ./k8s/utils/kube_ssh /usr/local/bin/kube_ssh
RUN sudo chmod a+wrx /tmp

USER graphscope
WORKDIR /home/graphscope

ENV PATH ${PATH}:/home/graphscope/.local/bin:/opt/graphscope/bin

COPY --from=builder /home/graphscope/install /opt/graphscope/
RUN python3 -m pip install --user --no-cache-dir /opt/graphscope/*.whl && sudo rm -rf /opt/graphscope/*.whl
