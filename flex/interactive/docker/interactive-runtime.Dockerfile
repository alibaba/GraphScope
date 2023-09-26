FROM interactive-base:latest AS builder

ARG CI=false

# change bash as default
SHELL ["/bin/bash", "-c"]

# install arrow
RUN cd /tmp && sudo apt-get install -y -V ca-certificates lsb-release wget && \
    curl -o apache-arrow-apt-source-latest.deb https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    sudo apt-get install -y ./apache-arrow-apt-source-latest.deb && \
    sudo apt-get update && sudo apt-get install -y libarrow-dev=6.0.1-1

RUN curl -sf -L https://static.rust-lang.org/rustup.sh | \
  sh -s -- -y --profile minimal --default-toolchain=1.70.0 && \
  chmod +x "$HOME/.cargo/env" && \
  echo "$source $HOME/.cargo/env" >> ~/.bashrc && \
  . ${HOME}/.cargo/env && \
  cargo --version

# install flex
RUN . ${HOME}/.cargo/env  &&  git clone https://github.com/alibaba/GraphScope.git -b main --single-branch && cd GraphScope/flex && \
    mkdir build && cd build && cmake .. -DCMAKE_INSTALL_PREFIX=/opt/flex -DBUILD_DOC=OFF && make -j && make install && \
    cd ~/GraphScope/interactive_engine/ && mvn clean package -Pexperimental -DskipTests && \
    cd ~/GraphScope/interactive_engine/compiler && cp target/compiler-0.0.1-SNAPSHOT.jar /opt/flex/lib/ && \
    cp target/libs/*.jar /opt/flex/lib/ && \
    ls ~/GraphScope/interactive_engine/executor/ir && \
    cp ~/GraphScope/interactive_engine/executor/ir/target/release/libir_core.so /opt/flex/lib/ && \
    rm -rf ~/GraphScope 

from ubuntu:20.04 as final_image

RUN apt-get update && apt-get install -y sudo

# Add graphscope user with user id 1001
RUN useradd -m graphscope -u 1001 && \
    echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# g++ + jre 500MB
RUN apt-get update && apt-get -y install locales g++-9 cmake openjdk-11-jre-headless && \
    ln -sf /usr/bin/g++-9 /usr/bin/g++ && locale-gen en_US.UTF-8 && apt-get clean -y && sudo rm -rf /var/lib/apt/lists/* 
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# copy builder's /opt/flex to final image
COPY --from=builder /opt/flex /opt/flex
# remov bin/run_app
RUN rm -rf /opt/flex/bin/run_app

COPY --from=builder /usr/lib/x86_64-linux-gnu/libprotobuf*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libsnappy*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/include/arrow /usr/include/arrow
COPY --from=builder /usr/lib/x86_64-linux-gnu/libgflags*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libglog*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libyaml-cpp*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libmpi*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libboost_program_options*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libboost_thread*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libcrypto*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libopen-rte*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libhwloc*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libunwind*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libarrow.so.600 /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libopen-pal*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libltdl*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libevent*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libutf8proc*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libre2*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/include/glog /usr/include/glog
COPY --from=builder /usr/include/gflags /usr/include/gflags

COPY --from=builder /usr/lib/x86_64-linux-gnu/openmpi/include/ /opt/flex/include
COPY --from=builder /usr/include/boost /usr/include/boost
COPY --from=builder /usr/include/google /usr/include/google
COPY --from=builder /usr/include/yaml-cpp /usr/include/yaml-cpp

RUN sudo rm -rf /usr/lib/x86_64-linux-gnu/libLLVM*.so* && sudo rm -rf /opt/flex/lib/libseastar.a && \
    sudo rm -rf /usr/lib/x86_64-linux-gnu/lib/libcuda.so && \
    sudo rm -rf /usr/lib/x86_64-linux-gnu/lib/libcudart.so && \
    sudo rm -rf /usr/lib/x86_64-linux-gnu/lib/libicudata.so*

RUN sudo ln -sf /opt/flex/bin/* /usr/local/bin/ \
  && sudo ln -sfn /opt/flex/include/* /usr/local/include/ \
  && sudo ln -sf -r /opt/flex/lib/* /usr/local/lib \
  && sudo ln -sf /opt/flex/lib64/*so* /usr/local/lib64

RUN chmod +x /opt/flex/bin/*

ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/opt/flex/lib/:/usr/lib/

