# the graphscope image is built from graphscope-vineyard, and is assembled based on
# graphscope-runtime.
#
# the result image includes all runtime stuffs of graphscope, with analytical engine,
# learning engine and interactive engine installed.

ARG BASE_VERSION=v0.2.7
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:$BASE_VERSION as builder

SHELL ["/usr/bin/scl", "enable", "devtoolset-7"]

ARG CI=true
ENV CI=$CI

ARG NETWORKX=ON
ENV NETWORKX=$NETWORKX

ARG profile=release
ENV profile=$profile

COPY ./k8s/kube_ssh /opt/graphscope/bin/kube_ssh
COPY ./k8s/pre_stop.py /opt/graphscope/bin/pre_stop.py
COPY ./k8s/ready_probe.sh /tmp/ready_probe.sh
COPY . /home/graphscope/gs

# build & install graph-learn library
RUN sudo chown -R graphscope:graphscope ${HOME}/gs && \
    cd ${HOME}/gs/learning_engine && \
    cd graph-learn/ && \
    git submodule update --init third_party/pybind11 && \
    mkdir cmake-build && \
    cd cmake-build && \
    cmake -DCMAKE_PREFIX_PATH=/opt/graphscope \
          -DCMAKE_INSTALL_PREFIX=/opt/graphscope \
          -DWITH_VINEYARD=ON \
          -DTESTING=OFF .. && \
    make graphlearn_shared -j && \
    make install

# build analytical engine
RUN export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/graphscope/lib:/opt/graphscope/lib64 && \
    cd ${HOME}/gs/analytical_engine && \
    mkdir -p build && \
    cd build && \
    cmake .. -DCMAKE_PREFIX_PATH=/opt/graphscope \
             -DCMAKE_INSTALL_PREFIX=/opt/graphscope \
             -DNETWORKX=$NETWORKX && \
    make gsa_cpplint && \
    make -j`nproc` && \
    make install && \
    rm -fr CMake* && \
    echo "Build and install analytical_engine done."

# patch auditwheel
RUN sudo sed -i 's/p.error/logger.warning/g' /usr/local/lib/python3.6/site-packages/auditwheel/main_repair.py

# build python bdist_wheel
RUN export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/graphscope/lib:/opt/graphscope/lib64 && \
    export WITH_LEARNING_ENGINE=ON && \
    cd ${HOME}/gs/python && \
    pip3 install -U setuptools && \
    pip3 install -r requirements.txt -r requirements-dev.txt && \
    python3 setup.py bdist_wheel && \
    cd ./dist && \
    auditwheel repair --plat=manylinux2014_x86_64 ./*.whl || true && \
    cp ./wheelhouse/* /opt/graphscope/dist/ && \
    cd ${HOME}/gs/coordinator && \
    pip3 install -r requirements.txt -r requirements-dev.txt && \
    python3 setup.py bdist_wheel && \
    cp ./dist/* /opt/graphscope/dist/ && \
    echo "Build python bdist_wheel done."

# build maxgraph engine: compile maxgraph rust
ARG profile=$profile

RUN wget --no-verbose https://golang.org/dl/go1.15.5.linux-amd64.tar.gz && \
    sudo tar -C /usr/local -xzf go1.15.5.linux-amd64.tar.gz && \
    curl -sf -L https://static.rust-lang.org/rustup.sh | \
        sh -s -- -y --profile minimal --default-toolchain 1.54.0 && \
    echo "source ~/.cargo/env" >> ~/.bashrc && \
    export PATH=${PATH}::/usr/local/go/bin && \
    go get github.com/etcd-io/zetcd/cmd/zetcd && \
    cp $(go env GOPATH)/bin/zetcd /tmp/zetcd

RUN source ~/.bashrc \
    && rustup component add rustfmt \
    && mkdir -p /opt/graphscope/conf \
    && cp ${HOME}/gs/interactive_engine/conf/* /opt/graphscope/conf/ \
    && echo "build with profile: $profile" \
    && cd ${HOME}/gs/interactive_engine/src/executor \
    && export CMAKE_PREFIX_PATH=/opt/graphscope \
    && export LIBRARY_PATH=$LIBRARY_PATH:/opt/graphscope/lib \
    && export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/graphscope/lib \
    && if [ "$profile" = "release" ]; then \
           echo "release mode" && ./exec.sh cargo build --all --release; \
       elif [ "$profile" = "nightly" ]; then \
           echo "nightly mode" && export RUSTFLAGS=-Zsanitizer=address && \
                rustup toolchain install nightly && \
                ./exec.sh cargo +nightly build --target x86_64-unknown-linux-gnu --all; \
       else \
           echo "debug mode" && ./exec.sh cargo build --all; \
       fi

# gie frontend / coordinator
RUN cd ${HOME}/gs/interactive_engine \
    && mvn clean package -DskipTests -Pjava-release --quiet


# # # # # # # # # # # # # # # # # # # # # #
# generate final runtime image
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest

ARG profile=release

COPY --from=builder /opt/graphscope /usr/local/
RUN cd /usr/local/dist/ && pip3 install ./*.whl
COPY --from=builder /home/graphscope/gs/k8s/precompile.py /tmp/precompile.py
RUN python3 /tmp/precompile.py && rm /tmp/precompile.py

COPY --from=builder /home/graphscope/gs/interactive_engine/src/executor/target/$profile/executor /usr/local/bin/executor
COPY --from=builder /home/graphscope/gs/interactive_engine/src/executor/target/$profile/gaia_executor /usr/local/bin/gaia_executor
COPY --from=builder /home/graphscope/gs/interactive_engine/bin/giectl /usr/local/bin/giectl
COPY --from=builder /tmp/zetcd /usr/local/bin/zetcd

# install mars
RUN pip3 install git+https://github.com/mars-project/mars.git@35b44ed56e031c252e50373b88b85bd9f454332e#egg=pymars[distributed]

# enable debugging
ENV RUST_BACKTRACE=1

COPY --from=builder /home/graphscope/gs/interactive_engine/src/assembly/target/0.0.1-SNAPSHOT.tar.gz /opt/graphscope/0.0.1-SNAPSHOT.tar.gz
RUN sudo tar -xf /opt/graphscope/0.0.1-SNAPSHOT.tar.gz -C /usr/local

ENV GRAPHSCOPE_HOME=/usr/local 
