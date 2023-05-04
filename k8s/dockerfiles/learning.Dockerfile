# Learning engine

ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG BUILDER_VERSION=latest
ARG RUNTIME_VERSION=latest
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
        cd /home/graphscope/GraphScope/python; \
        python3 setup.py bdist_wheel; \
        export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/graphscope/GraphScope/learning_engine/graph-learn/graphlearn/built/lib; \
        auditwheel repair dist/*.whl; \
        python3 -m pip install wheelhouse/*.whl; \
        cp wheelhouse/*.whl /home/graphscope/install/; \
        cd /home/graphscope/GraphScope/coordinator; \
        python3 setup.py bdist_wheel; \
        cp dist/*.whl /home/graphscope/install/; \
    fi

############### RUNTIME: GLE #######################
FROM $REGISTRY/graphscope/vineyard-runtime:$RUNTIME_VERSION AS learning

RUN sudo apt-get update -y && \
    sudo apt-get install -y python3-pip && \
    sudo apt-get clean -y && \
    sudo rm -rf /var/lib/apt/lists/*

RUN sudo chmod a+wrx /tmp

COPY --from=builder /home/graphscope/install /opt/graphscope/
RUN python3 -m pip install --no-cache-dir /opt/graphscope/*.whl && sudo rm -rf /opt/graphscope/*.whl
