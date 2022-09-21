# Analytical engine

ARG BASE_VERSION=v0.9.0
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:$BASE_VERSION AS builder

ARG profile=release
ENV profile=$profile
ADD . /home/graphscope/GraphScope

RUN sudo chown -R graphscope:graphscope /home/graphscope/GraphScope
RUN source $HOME/.bashrc \
    && cd /home/graphscope/GraphScope/ \
    && mkdir /home/graphscope/install \
    && make gae ENABLE_JAVA_SDK=OFF INSTALL_PREFIX=/home/graphscope/install \
    && mkdir /home/graphscope/install-with-java \
    && make gae ENABLE_JAVA_SDK=ON INSTALL_PREFIX=/home/graphscope/install-with-java

############### RUNTIME: GAE #######################
FROM registry.cn-hongkong.aliyuncs.com/graphscope/vineyard-runtime:$BASE_VERSION AS analytical

COPY --from=builder /home/graphscope/install /opt/graphscope/

USER graphscope
WORKDIR /home/graphscope

############### RUNTIME: GAE-JAVA #######################
FROM registry.cn-hongkong.aliyuncs.com/graphscope/vineyard-runtime:$BASE_VERSION AS analytical-java

COPY --from=builder /home/graphscope/install-with-java /opt/graphscope/

# Installed size: 200M
RUN yum install -y java-1.8.0-openjdk-devel \
    && yum clean all \
    && rm -rf /var/cache/yum

# install clang-11 with gold optimizer plugin, depends on header include/plugin-api.h
# Installed size: 1.5G
RUN cd /tmp && \
    mkdir -p binutils/include && \
    cd binutils/include && \
    wget -q https://raw.githubusercontent.com/bminor/binutils-gdb/binutils-2_37-branch/include/plugin-api.h && \
    cd /tmp && \
    wget -q https://github.com/llvm/llvm-project/archive/refs/tags/llvmorg-11.1.0.tar.gz && \
    tar zxf /tmp/llvmorg-11.1.0.tar.gz -C /tmp/ && \
    cd llvm-project-llvmorg-11.1.0/ && \
    cmake -G "Unix Makefiles" -DLLVM_ENABLE_PROJECTS='clang;lld' \
                              -DCMAKE_INSTALL_PREFIX=/opt/llvm11 \
                              -DCMAKE_BUILD_TYPE=Release \
                              -DLLVM_TARGETS_TO_BUILD=X86 \
                              -DLLVM_BINUTILS_INCDIR=/tmp/binutils/include \
                              ./llvm && \
    make install -j`nproc` && \
    rm -rf /tmp/llvm-project-llvmorg-11.1.0 /tmp/llvmorg-11.1.0.tar.gz /tmp/binutils

ENV JAVA_HOME /usr/lib/jvm/java

ENV LLVM11_HOME=/opt/llvm11
ENV LIBCLANG_PATH=/opt/llvm11/lib
ENV LLVM_CONFIG_PATH=/opt/llvm11/bin/llvm-config

USER graphscope
WORKDIR /home/graphscope
