# Coordinator of graphscope engines

ARG PLATFORM=x86_64
ARG ARCH=amd64
ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG VINEYARD_VERSION=latest
FROM $REGISTRY/graphscope/graphscope-dev:$VINEYARD_VERSION-$ARCH AS builder
ARG ENABLE_COORDINATOR="false"
ARG OPTIMIZE_FOR_HOST=OFF
ARG ENABLE_OPENTELMETRY=false
ARG PARALLEL=8

RUN sudo mkdir -p /opt/flex && sudo chown -R graphscope:graphscope /opt/flex/
USER graphscope
WORKDIR /home/graphscope

# change bash as default
SHELL ["/bin/bash", "-c"]

RUN if [ "${ENABLE_OPENTELMETRY}" = "true" ]; then \
        cd /tmp && git clone -b v1.14.2 --single-branch https://github.com/open-telemetry/opentelemetry-cpp && cd opentelemetry-cpp && \
        cmake . -DCMAKE_INSTALL_PREFIX=/opt/flex -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=17 -DCMAKE_POSITION_INDEPENDENT_CODE=ON  -DBUILD_SHARED_LIBS=ON -DWITH_OTLP_HTTP=ON -DWITH_OTLP_GRPC=OFF -DWITH_ABSEIL=OFF -DWITH_PROMETHEUS=OFF -DBUILD_TESTING=OFF -DWITH_EXAMPLES=OFF && \
        make -j  && make install && rm -rf /tmp/opentelemetry-cpp; \
    fi

COPY --chown=graphscope:graphscope . /home/graphscope/GraphScope

# install flex
RUN . ${HOME}/.cargo/env  && cd ${HOME}/GraphScope/flex && \
    git submodule update --init && mkdir build && cd build && cmake .. -DCMAKE_INSTALL_PREFIX=/opt/flex -DBUILD_DOC=OFF -DBUILD_TEST=OFF -DOPTIMIZE_FOR_HOST=${OPTIMIZE_FOR_HOST} && make -j ${PARALLEL} && make install && \
    cd ~/GraphScope/interactive_engine/ && mvn clean package -Pexperimental -DskipTests && \
    cd ~/GraphScope/interactive_engine/compiler && cp target/compiler-0.0.1-SNAPSHOT.jar /opt/flex/lib/ && \
    cp target/libs/*.jar /opt/flex/lib/ && \
    ls ~/GraphScope/interactive_engine/executor/ir && \
    cp ~/GraphScope/interactive_engine/executor/ir/target/release/libir_core.so /opt/flex/lib/

# build coordinator
RUN mkdir -p /opt/flex/wheel
RUN if [ "${ENABLE_COORDINATOR}" = "true" ]; then \
        export PATH=${HOME}/.local/bin:${PATH} && \
        cd ${HOME}/GraphScope/flex/interactive/sdk && \
        ./generate_sdk.sh -g python && cd python && \
        python3 -m pip install --upgrade pip && python3 -m pip install -r requirements.txt && \
        python3 setup.py build_proto && python3 setup.py bdist_wheel && \
        cp dist/*.whl /opt/flex/wheel/ && \
        cd ${HOME}/GraphScope/python && \
        export WITHOUT_LEARNING_ENGINE=ON && python3 setup.py bdist_wheel && \
        cp dist/*.whl /opt/flex/wheel/ && \
        cd ${HOME}/GraphScope/coordinator && \
        python3 setup.py bdist_wheel && \
        cp dist/*.whl /opt/flex/wheel/; \
    fi


########################### RUNTIME IMAGE ###########################

from ubuntu:22.04 as runtime
ARG PLATFORM=x86_64
ARG ENABLE_COORDINATOR="false"

ENV DEBIAN_FRONTEND=noninteractive

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
# g++ + jre 500MB
RUN apt-get update && apt-get -y install sudo locales g++ cmake openjdk-11-jre-headless tzdata && \
    locale-gen en_US.UTF-8 && apt-get clean -y && rm -rf /var/lib/apt/lists/* && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# python3
RUN if [ "${ENABLE_COORDINATOR}" = "true" ]; then \
      apt-get update && apt-get -y install python3 python3-pip && \
      apt-get clean -y && sudo rm -rf /var/lib/apt/lists/*; \
    fi

RUN mkdir /opt/vineyard/

# copy builder's /opt/flex to final image
COPY --from=builder /opt/flex/bin/interactive_server /opt/flex/bin/bulk_loader \
    /opt/flex/bin/gen_code_from_plan  /opt/flex/bin/load_plan_and_gen.sh /opt/flex/bin/

# copy wheel 
COPY --from=builder /opt/flex/wheel/ /opt/flex/wheel/

# lib 
COPY --from=builder /opt/flex/lib/ /opt/flex/lib/
# remove .a files
RUN find /opt/flex/lib/ -name "*.a" -type f -delete

# include 
COPY --from=builder /opt/flex/include/ /opt/graphscope/include/ /opt/vineyard/include/ /opt/flex/include/
COPY --from=builder /opt/graphscope/lib/libgrape-lite.so /opt/flex/lib/

# copy the builtin graph, modern_graph
RUN mkdir -p /opt/flex/share/gs_interactive_default_graph/
COPY --from=builder /home/graphscope/GraphScope/flex/interactive/examples/modern_graph/* /opt/flex/share/gs_interactive_default_graph/
COPY --from=builder /home/graphscope/GraphScope/flex/tests/hqps/interactive_config_test.yaml /opt/flex/share/interactive_config.yaml
COPY --from=builder /home/graphscope/GraphScope/k8s/dockerfiles/interactive-entrypoint.sh /opt/flex/bin/entrypoint.sh
RUN sed -i 's/name: modern_graph/name: gs_interactive_default_graph/g' /opt/flex/share/gs_interactive_default_graph/graph.yaml && \
    sed -i 's/default_graph: modern_graph/default_graph: gs_interactive_default_graph/g' /opt/flex/share/interactive_config.yaml

# remove bin/run_app
RUN rm -rf /opt/flex/bin/run_app

COPY --from=builder /usr/lib/$PLATFORM-linux-gnu/libsnappy*.so* /usr/lib/$PLATFORM-linux-gnu/
COPY --from=builder /usr/include/arrow /usr/include/arrow
COPY --from=builder /usr/include/yaml-cpp /usr/include/yaml-cpp
COPY --from=builder /usr/include/boost /usr/include/boost
COPY --from=builder /usr/include/google /usr/include/google
COPY --from=builder /usr/include/glog /usr/include/glog
COPY --from=builder /usr/include/gflags /usr/include/gflags
COPY --from=builder /usr/include/rapidjson /usr/include/rapidjson
COPY --from=builder /usr/lib/$PLATFORM-linux-gnu/openmpi/include/ /opt/flex/include


COPY --from=builder /usr/lib/$PLATFORM-linux-gnu/libprotobuf* /usr/lib/$PLATFORM-linux-gnu/libfmt*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libre2*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libutf8proc*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libevent*.so*  \
    /usr/lib/$PLATFORM-linux-gnu/libltdl*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libltdl*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libopen-pal*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libunwind*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libhwloc*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libopen-rte*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libcrypto*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libboost_thread*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libboost_filesystem*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libboost_program_options*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libmpi*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libyaml-cpp*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libglog*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libgflags*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libicudata.so* \
    /usr/lib/$PLATFORM-linux-gnu/

RUN sudo rm -rf /usr/lib/$PLATFORM-linux-gnu/libLLVM*.so* && sudo rm -rf /opt/flex/lib/libseastar.a && \
    sudo rm -rf /usr/lib/$PLATFORM-linux-gnu/libcuda.so && \
    sudo rm -rf /usr/lib/$PLATFORM-linux-gnu/libcudart.so

# strip all .so in /opt/flex/lib
RUN sudo find /opt/flex/lib/ -name "*.so" -type f -exec strip {} \;
# strip all binary in /opt/flex/bin
RUN sudo strip /opt/flex/bin/bulk_loader /opt/flex/bin/interactive_server /opt/flex/bin/gen_code_from_plan

RUN sudo ln -sf /opt/flex/bin/* /usr/local/bin/ \
  && sudo ln -sfn /opt/flex/include/* /usr/local/include/ \
  && sudo ln -sf -r /opt/flex/lib/* /usr/local/lib \
  && sudo ln -sf /opt/flex/lib64/*so* /usr/local/lib64 \
  && chmod +x /opt/flex/bin/*

RUN if [ "${ENABLE_COORDINATOR}" = "true" ]; then \
      pip3 install --upgrade pip && \
      pip3 install "numpy<2.0.0" && \
      pip3 install /opt/flex/wheel/*.whl; \
      rm -rf ~/.cache/pip; \
    fi

ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/opt/flex/lib/:/usr/lib/:/usr/local/lib/
# flex solution
ENV SOLUTION=INTERACTIVE

# Add graphscope user with user id 1001
RUN useradd -m graphscope -u 1001 && \
    echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers && \
    chown -R graphscope:graphscope /opt/flex

# set home to graphscope user
ENV HOME=/home/graphscope
USER graphscope
WORKDIR /home/graphscope

ENTRYPOINT ["/opt/flex/bin/entrypoint.sh"]
