# Graphlearn-torch engine

ARG ARCH=amd64
ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG VINEYARD_VERSION=latest
FROM $REGISTRY/graphscope/graphscope-dev:$VINEYARD_VERSION-$ARCH AS builder

COPY --chown=graphscope:graphscope . /home/graphscope/GraphScope

RUN cd /home/graphscope/GraphScope/; \
    mkdir /home/graphscope/install; \
    make analytical-install INSTALL_PREFIX=/home/graphscope/install; \
    make learning-install INSTALL_PREFIX=/home/graphscope/install; \
    WITH_GLTORCH=ON make client INSTALL_PREFIX=/home/graphscope/install; \
    cd python; \
    python3 -m pip install --user -r requirements.txt; \
    python3 setup.py bdist_wheel; \
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/graphscope/GraphScope/learning_engine/graph-learn/graphlearn/built/lib; \
    auditwheel repair dist/*.whl --exclude libtorch_cpu.so --exclude libc10.so --exclude libtorch_python.so --exclude libtorch.so; \
    python3 -m pip install wheelhouse/*.whl; \
    cp wheelhouse/*.whl /home/graphscope/install/; \
    cd ../coordinator; \
    python3 setup.py bdist_wheel; \
    cp dist/*.whl /home/graphscope/install/

############### RUNTIME: GLE #######################
FROM $REGISTRY/graphscope/vineyard-runtime:$VINEYARD_VERSION-$ARCH AS graphlearn-torch

RUN sudo apt-get update -y && \
    sudo apt-get install -y python3-pip && \
    sudo apt-get clean -y && \
    sudo rm -rf /var/lib/apt/lists/*

RUN sudo chmod a+wrx /tmp

RUN pip3 install pip==20.3.4 
RUN python3 -m pip install torch==2.2.1 --index-url https://download.pytorch.org/whl/cpu
RUN python3 -m pip install --user ogb torch_geometric pyg_lib torch_scatter torch_sparse torch_cluster torch_spline_conv -f https://data.pyg.org/whl/torch-2.2.0+cpu.html

COPY --from=builder /home/graphscope/install /opt/graphscope/
RUN python3 -m pip install --no-cache-dir /opt/graphscope/*.whl && sudo rm -rf /opt/graphscope/*.whl

ENV PATH=${PATH}:/home/graphscope/.local/bin
