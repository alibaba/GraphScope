# the graphscope image is built from graphscope-vineyard, and is assembled based on
# graphscope-runtime.
#
# the result image includes all runtime stuffs of graphscope, with analytical engine,
# learning engine and interactive engine installed.

ARG BASE_VERSION=v0.3.1
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:$BASE_VERSION as builder

ARG CI=true
ENV CI=$CI

ARG NETWORKX=ON
ENV NETWORKX=$NETWORKX

ARG profile=release
ENV profile=$profile

COPY . /home/graphscope/gs

# build & install graph-learn library
RUN sudo mkdir -p /opt/graphscope && \
    sudo chown -R $(id -u):$(id -g) ${HOME}/gs /opt/graphscope && \
    cd ${HOME}/gs/learning_engine && \
    cd graph-learn/ && \
    git submodule update --init third_party/pybind11 && \
    mkdir cmake-build && \
    cd cmake-build && \
    cmake -DCMAKE_INSTALL_PREFIX=/opt/graphscope \
          -DWITH_VINEYARD=ON \
          -DTESTING=OFF .. && \
    make graphlearn_shared -j && \
    make install

# build analytical engine
RUN cd ${HOME}/gs/analytical_engine && \
    mkdir -p build && \
    cd build && \
    cmake .. -DCMAKE_INSTALL_PREFIX=/opt/graphscope \
             -DNETWORKX=$NETWORKX && \
    make gsa_cpplint && \
    make -j`nproc` && \
    make install && \
    rm -fr CMake* && \
    echo "Build and install analytical_engine done."

# build python bdist_wheel
RUN export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/graphscope/lib:/opt/graphscope/lib64 && \
    export WITH_LEARNING_ENGINE=ON && \
    cd ${HOME}/gs/python && \
    pip3 install -U setuptools && \
    pip3 install -r requirements.txt -r requirements-dev.txt && \
    python3 setup.py bdist_wheel && \
    cd ./dist && \
    auditwheel repair --plat=manylinux2014_x86_64 ./*.whl || true && \
    mkdir -p /opt/graphscope/dist && cp ./wheelhouse/* /opt/graphscope/dist/ && \
    pip3 install ./wheelhouse/*.whl || true && \
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
    && sudo yum install -y clang-devel \
    && export LIBCLANG_PATH=$(dirname $(python3 -c "import clang; print(clang.__file__)"))/native \
    && rustup component add rustfmt \
    && echo "build with profile: $profile" \
    && cd ${HOME}/gs/interactive_engine/executor \
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

# install vineyard into /usr/local
COPY --from=builder /opt/vineyard/ /usr/local/
COPY --from=builder /opt/graphscope /opt/graphscope
COPY --from=builder /tmp/zetcd /opt/graphscope/bin/zetcd
COPY --from=builder /home/graphscope/gs/k8s/precompile.py /tmp/precompile.py
COPY --from=builder /home/graphscope/gs/k8s/kube_ssh /opt/graphscope/bin/kube_ssh
COPY --from=builder /home/graphscope/gs/k8s/pre_stop.py /opt/graphscope/bin/pre_stop.py
COPY --from=builder /home/graphscope/gs/interactive_engine/executor/target/$profile/executor /opt/graphscope/bin/executor
COPY --from=builder /home/graphscope/gs/interactive_engine/executor/target/$profile/gaia_executor /opt/graphscope/bin/gaia_executor
COPY --from=builder /home/graphscope/gs/interactive_engine/assembly/target/maxgraph-assembly-0.0.1-SNAPSHOT.tar.gz /opt/graphscope/maxgraph-assembly-0.0.1-SNAPSHOT.tar.gz

# install mars
RUN pip3 install git+https://github.com/mars-project/mars.git@d09e1e4c3e32ceb05f42d0b5b79775b1ebd299fb#egg=pymars

RUN sudo tar -xf /opt/graphscope/maxgraph-assembly-0.0.1-SNAPSHOT.tar.gz --strip-components 1 -C /opt/graphscope \
  && cd /usr/local/dist && pip3 install ./*.whl \
  && cd /opt/graphscope/dist && pip3 install ./*.whl \
  && sudo ln -sf /opt/graphscope/bin/* /usr/local/bin/ \
  && sudo ln -sfn /opt/graphscope/include/graphscope /usr/local/include/graphscope \
  && sudo ln -sf /opt/graphscope/lib/*so* /usr/local/lib \
  && sudo ln -sf /opt/graphscope/lib64/*so* /usr/local/lib64 \
  && sudo ln -sfn /opt/graphscope/lib64/cmake/graphscope-analytical /usr/local/lib64/cmake/graphscope-analytical \
  && python3 /tmp/precompile.py && sudo rm -fr /tmp/precompile.py /usr/local/dist /opt/graphscope/dist/*.whl

# enable debugging
ENV RUST_BACKTRACE=1
ENV GRAPHSCOPE_HOME=/opt/graphscope
