ARG ARCH=x86_64
FROM registry.cn-hongkong.aliyuncs.com/graphscope/interactive-base:v0.0.4 AS builder

ARG ARCH
ARG ENABLE_COORDINATOR="false"

COPY --chown=graphscope:graphscope . /home/graphscope/GraphScope

# change bash as default
SHELL ["/bin/bash", "-c"]

# install arrow
RUN cd /tmp && sudo apt-get update && sudo apt-get install -y -V ca-certificates lsb-release wget libcurl4-openssl-dev && \
    curl -o apache-arrow-apt-source-latest.deb https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    sudo apt-get install -y ./apache-arrow-apt-source-latest.deb && \
    sudo apt-get update && sudo apt-get install -y libarrow-dev=8.0.0-1

RUN curl -sf -L https://static.rust-lang.org/rustup.sh | \
  sh -s -- -y --profile minimal --default-toolchain=1.70.0 && \
  chmod +x "$HOME/.cargo/env" && \
  echo "$source $HOME/.cargo/env" >> ~/.bashrc && \
  . ${HOME}/.cargo/env && \
  cargo --version

# install opentelemetry
RUN cd /tmp && git clone -b v1.14.2 --single-branch https://github.com/open-telemetry/opentelemetry-cpp && cd opentelemetry-cpp && \
cmake . -DCMAKE_INSTALL_PREFIX=/opt/flex -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=17 \
-DCMAKE_POSITION_INDEPENDENT_CODE=ON  -DBUILD_SHARED_LIBS=ON \
-DWITH_OTLP_HTTP=ON -DWITH_OTLP_GRPC=OFF \
-DWITH_ABSEIL=OFF -DWITH_PROMETHEUS=OFF \
-DBUILD_TESTING=OFF -DWITH_EXAMPLES=OFF && make -j  && make install && rm -rf /tmp/opentelemetry-cpp

# install flex
RUN . ${HOME}/.cargo/env  && cd ${HOME}/GraphScope/flex && \
    git submodule update --init && mkdir build && cd build && cmake .. -DCMAKE_INSTALL_PREFIX=/opt/flex -DBUILD_DOC=OFF -DBUILD_TEST=OFF && make -j && make install && \
    cd ~/GraphScope/interactive_engine/ && mvn clean package -Pexperimental -DskipTests && \
    cd ~/GraphScope/interactive_engine/compiler && cp target/compiler-0.0.1-SNAPSHOT.jar /opt/flex/lib/ && \
    cp target/libs/*.jar /opt/flex/lib/ && \
    ls ~/GraphScope/interactive_engine/executor/ir && \
    cp ~/GraphScope/interactive_engine/executor/ir/target/release/libir_core.so /opt/flex/lib/

# build coordinator
RUN if [ "${ENABLE_COORDINATOR}" = "true" ]; then \
        cd ${HOME}/GraphScope/coordinator && \
        python3 setup.py bdist_wheel && \
        mkdir -p /opt/flex/wheel && cp dist/*.whl /opt/flex/wheel/; \
    fi


########################### RUNTIME IMAGE ###########################

from ubuntu:22.04 as final_image
ARG ARCH
ARG ENABLE_COORDINATOR="false"

ENV DEBIAN_FRONTEND=noninteractive

# g++ + jre 500MB
RUN apt-get update && apt-get -y install sudo locales g++ cmake openjdk-11-jre-headless && \
    locale-gen en_US.UTF-8 && apt-get clean -y && rm -rf /var/lib/apt/lists/* 

# python3
RUN if [ "${ENABLE_COORDINATOR}" = "true" ]; then \
      apt-get update && apt-get -y install python3 python3-pip && \
      apt-get clean -y && sudo rm -rf /var/lib/apt/lists/*; \
    fi

ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# copy builder's /opt/flex to final image
COPY --from=builder /opt/flex /opt/flex

# copy the builtin graph, modern_graph
RUN mkdir -p /opt/flex/share/gs_interactive_default_graph/
COPY --from=builder /home/graphscope/GraphScope/flex/interactive/examples/modern_graph/* /opt/flex/share/gs_interactive_default_graph/
COPY --from=builder /home/graphscope/GraphScope/flex/tests/hqps/engine_config_test.yaml /opt/flex/share/engine_config.yaml
COPY --from=builder /home/graphscope/GraphScope/flex/interactive/docker/entrypoint.sh /opt/flex/bin/entrypoint.sh
COPY --from=builder /home/graphscope/GraphScope/flex/third_party/nlohmann-json/single_include/* /opt/flex/include/
RUN sed -i 's/name: modern_graph/name: gs_interactive_default_graph/g' /opt/flex/share/gs_interactive_default_graph/graph.yaml
# change the default graph name.
RUN sed -i 's/default_graph: ldbc/default_graph: gs_interactive_default_graph/g' /opt/flex/share/engine_config.yaml

# remove bin/run_app
RUN rm -rf /opt/flex/bin/run_app

COPY --from=builder /usr/lib/$ARCH-linux-gnu/libsnappy*.so* /usr/lib/$ARCH-linux-gnu/
COPY --from=builder /usr/include/arrow /usr/include/arrow
COPY --from=builder /usr/include/yaml-cpp /usr/include/yaml-cpp
COPY --from=builder /usr/lib/$ARCH-linux-gnu/libgflags*.so* /usr/lib/$ARCH-linux-gnu/
COPY --from=builder /usr/lib/$ARCH-linux-gnu/libglog*.so* /usr/lib/$ARCH-linux-gnu/
COPY --from=builder /usr/lib/$ARCH-linux-gnu/libyaml-cpp*.so* /usr/lib/$ARCH-linux-gnu/
COPY --from=builder /usr/lib/$ARCH-linux-gnu/libmpi*.so* /usr/lib/$ARCH-linux-gnu/
COPY --from=builder /usr/lib/$ARCH-linux-gnu/libboost_program_options*.so* /usr/lib/$ARCH-linux-gnu/
COPY --from=builder /usr/lib/$ARCH-linux-gnu/libboost_filesystem*.so* /usr/lib/$ARCH-linux-gnu/
COPY --from=builder /usr/lib/$ARCH-linux-gnu/libboost_thread*.so* /usr/lib/$ARCH-linux-gnu/
COPY --from=builder /usr/lib/$ARCH-linux-gnu/libcrypto*.so* /usr/lib/$ARCH-linux-gnu/
COPY --from=builder /usr/lib/$ARCH-linux-gnu/libopen-rte*.so* /usr/lib/$ARCH-linux-gnu/
COPY --from=builder /usr/lib/$ARCH-linux-gnu/libhwloc*.so* /usr/lib/$ARCH-linux-gnu/

# libunwind for arm64 seems not installed here, and seems not needed for aarch64(tested)
COPY --from=builder /usr/lib/$ARCH-linux-gnu/libunwind*.so* /usr/lib/$ARCH-linux-gnu/
COPY --from=builder /usr/lib/$ARCH-linux-gnu/libarrow.so* /usr/lib/$ARCH-linux-gnu/
COPY --from=builder /usr/lib/$ARCH-linux-gnu/libopen-pal*.so* /usr/lib/$ARCH-linux-gnu/
COPY --from=builder /usr/lib/$ARCH-linux-gnu/libltdl*.so* /usr/lib/$ARCH-linux-gnu/
COPY --from=builder /usr/lib/$ARCH-linux-gnu/libevent*.so* /usr/lib/$ARCH-linux-gnu/
COPY --from=builder /usr/lib/$ARCH-linux-gnu/libutf8proc*.so* /usr/lib/$ARCH-linux-gnu/
COPY --from=builder /usr/lib/$ARCH-linux-gnu/libre2*.so* /usr/lib/$ARCH-linux-gnu/
COPY --from=builder /usr/include/glog /usr/include/glog
COPY --from=builder /usr/include/gflags /usr/include/gflags
COPY --from=builder /usr/lib/$ARCH-linux-gnu/libprotobuf* /usr/lib/$ARCH-linux-gnu/
COPY --from=builder /usr/lib/$ARCH-linux-gnu/libfmt*.so* /usr/lib/$ARCH-linux-gnu/

COPY --from=builder /usr/lib/$ARCH-linux-gnu/openmpi/include/ /opt/flex/include
COPY --from=builder /usr/include/boost /usr/include/boost
COPY --from=builder /usr/include/google /usr/include/google
COPY --from=builder /usr/include/yaml-cpp /usr/include/yaml-cpp

RUN sudo rm -rf /usr/lib/$ARCH-linux-gnu/libLLVM*.so* && sudo rm -rf /opt/flex/lib/libseastar.a && \
    sudo rm -rf /usr/lib/$ARCH-linux-gnu/lib/libcuda.so && \
    sudo rm -rf /usr/lib/$ARCH-linux-gnu/lib/libcudart.so && \
    sudo rm -rf /usr/lib/$ARCH-linux-gnu/lib/libicudata.so*

RUN sudo ln -sf /opt/flex/bin/* /usr/local/bin/ \
  && sudo ln -sfn /opt/flex/include/* /usr/local/include/ \
  && sudo ln -sf -r /opt/flex/lib/* /usr/local/lib \
  && sudo ln -sf /opt/flex/lib64/*so* /usr/local/lib64

RUN chmod +x /opt/flex/bin/*

RUN if [ "${ENABLE_COORDINATOR}" = "true" ]; then \
      pip3 install --upgrade pip && \
      pip3 install /opt/flex/wheel/*.whl; \
    fi

ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/opt/flex/lib/:/usr/lib/:/usr/local/lib/
# flex solution
ENV SOLUTION=INTERACTIVE

# Add graphscope user with user id 1001
RUN useradd -m graphscope -u 1001 && \
    echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

RUN chown -R graphscope:graphscope /opt/flex

# set home to graphscope user
ENV HOME=/home/graphscope
USER graphscope
WORKDIR /home/graphscope

ENTRYPOINT ["/opt/flex/bin/entrypoint.sh"]
