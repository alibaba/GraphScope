FROM ubuntu:22.04
ARG CI=false
ARG ENABLE_OPENTELMETRY=false

# change bash as default
SHELL ["/bin/bash", "-c"]

ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
ENV LD_LIBRARY_PATH /usr/local/lib:$LD_LIBRARY_PATH
# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# g++ + jre 500MB
RUN apt-get update && apt-get -y install sudo locales g++ cmake openjdk-11-jre-headless && \
    locale-gen en_US.UTF-8 && apt-get clean -y && rm -rf /var/lib/apt/lists/* 


RUN apt-get update && apt-get install -y protobuf-compiler libprotobuf-dev maven git vim curl \
    wget python3 make libc-ares-dev doxygen python3-pip net-tools curl default-jdk rapidjson-dev \
    libgoogle-glog-dev libopenmpi-dev libboost-all-dev libyaml-cpp-dev libcrypto++-dev openssl libcurl4-openssl-dev && \
    apt-get clean -y && rm -rf /var/lib/apt/lists/*

# install libgrape-lite
RUN cd /tmp && \
    git clone https://github.com/alibaba/libgrape-lite.git -b v0.3.2 --single-branch && cd libgrape-lite && \
    mkdir build && cd build && cmake .. -DBUILD_LIBGRAPELITE_TESTS=OFF && make -j && make install && rm -rf /tmp/libgrape-lite

RUN apt-get update && apt-get -y install pkg-config ninja-build xfslibs-dev ragel libpciaccess-dev libxml2-dev libgnutls28-dev liblz4-dev libsctp-dev systemtap-sdt-dev stow libfmt-dev valgrind && \
    apt-get clean -y && rm -rf /var/lib/apt/lists/* 

# install hiactor
RUN cd /tmp && git clone https://github.com/alibaba/hiactor.git -b v0.1.1 --single-branch && cd hiactor && \
    git submodule update --init --recursive && ./seastar/seastar/install-dependencies.sh && mkdir build && cd build && \
    cmake -DHiactor_DEMOS=OFF -DHiactor_TESTING=OFF -DHiactor_DPDK=OFF -DHiactor_CXX_DIALECT=gnu++17 -DSeastar_CXX_FLAGS="-DSEASTAR_DEFAULT_ALLOCATOR -mno-avx512" .. && \
    make -j && make install && rm -rf /tmp/hiactor

# install arrow
RUN cd /tmp && sudo apt-get update && sudo apt-get install -y -V ca-certificates lsb-release wget libcurl4-openssl-dev && \
    curl -o apache-arrow-apt-source-latest.deb https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    sudo apt-get install -y ./apache-arrow-apt-source-latest.deb && \
    sudo apt-get update && sudo apt-get install -y libarrow-dev=15.0.2-1

# install boost
RUN pushd /tmp/ && curl -L -o boost_1_75_0.tar.gz "https://graphscope.oss-cn-beijing.aliyuncs.com/dependencies/boost_1_75_0.tar.gz" && \
    tar -xzf boost_1_75_0.tar.gz && pushd boost_1_75_0 && ./bootstrap.sh --with-libraries=system,filesystem,context,atomic,program_options,regex,thread,chrono,date_time,test && \
    ./b2 install link=shared runtime-link=shared variant=release threading=multi && \
    popd && rm -rf boost_1_75_0 && rm boost_1_75_0.tar.gz

# install opentelemetry
RUN if [ "${ENABLE_OPENTELMETRY}" = "true" ]; then \
        cd /tmp && git clone -b v1.14.2 --single-branch https://github.com/open-telemetry/opentelemetry-cpp && cd opentelemetry-cpp && \
        cmake . -DCMAKE_INSTALL_PREFIX=/opt/flex -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=17 -DCMAKE_POSITION_INDEPENDENT_CODE=ON  -DBUILD_SHARED_LIBS=ON -DWITH_OTLP_HTTP=ON -DWITH_OTLP_GRPC=OFF -DWITH_ABSEIL=OFF -DWITH_PROMETHEUS=OFF -DBUILD_TESTING=OFF -DWITH_EXAMPLES=OFF && \
        make -j  && make install && rm -rf /tmp/opentelemetry-cpp; \
    fi

# Add graphscope user with user id 1001
RUN useradd -m graphscope -u 1001 && \
    echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers && \
    chown -R graphscope:graphscope /usr/local/bin/hiactor_codegen/

# Change to graphscope user
USER graphscope
WORKDIR /home/graphscope
# set HOME
ENV HOME /home/graphscope

RUN curl -sf -L https://static.rust-lang.org/rustup.sh | \
  sh -s -- -y --profile minimal --default-toolchain=1.70.0 && \
  chmod +x "$HOME/.cargo/env" && \
  echo "$source $HOME/.cargo/env" >> ~/.bashrc && \
  source "$HOME/.cargo/env" && \
  echo "1.70.0" > rust-toolchain && \
  bash -c "rustup component add rustfmt"
