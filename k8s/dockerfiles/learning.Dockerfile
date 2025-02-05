# Learning engine

ARG ARCH=amd64
ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG VINEYARD_VERSION=latest
FROM $REGISTRY/graphscope/graphscope-dev:$VINEYARD_VERSION-$ARCH AS builder

ARG CI=false

COPY --chown=graphscope:graphscope . /home/graphscope/GraphScope

# Those commands has been removed from the Dockerfile, since we require no manylinux2014 wheel in this image.
# ---
# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/graphscope/GraphScope/learning_engine/graph-learn/graphlearn/built/lib && \
# auditwheel repair dist/*.whl && \
# ---
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
        cp dist/*.whl /home/graphscope/install/; \
        cd ../coordinator; \
        python3 setup.py bdist_wheel; \
        cp dist/*.whl /home/graphscope/install/; \
    fi

############### RUNTIME: GLE #######################
FROM $REGISTRY/graphscope/vineyard-runtime:$VINEYARD_VERSION-$ARCH AS learning

RUN sudo apt-get update -y && \
    sudo apt-get install -y python3-pip && \
    sudo apt-get clean -y && \
    sudo rm -rf /var/lib/apt/lists/*

RUN sudo chmod a+wrx /tmp

#to make sure neo4j==5.10.0 can be installed
RUN pip3 install pip==20.3.4 

COPY --from=builder /home/graphscope/install /opt/graphscope/
RUN python3 -m pip install --no-cache-dir /opt/graphscope/*.whl && sudo rm -rf /opt/graphscope/*.whl

ENV PATH=${PATH}:/home/graphscope/.local/bin
