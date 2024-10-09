# build `graphscope/graphscope-dev:wheel-{v6d_version}-{arch}` image based on manylinux,
# including all dependencies for building graphscope wheel package.

ARG ARCH=amd64
ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
FROM $REGISTRY/graphscope/manylinux2014:$ARCH AS builder

# build form https://github.com/sighingnow/manylinux/tree/dyn-rebase
# usually we don't need to change this image unless the underlying python needs to be updated
FROM $REGISTRY/graphscope/manylinux2014:20230407

# change the source
RUN sed -i "s/mirror.centos.org/vault.centos.org/g" /etc/yum.repos.d/*.repo && \
    sed -i "s/^#.*baseurl=http/baseurl=http/g" /etc/yum.repos.d/*.repo && \
    sed -i "s/^mirrorlist=http/#mirrorlist=http/g" /etc/yum.repos.d/*.repo

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo '$TZ' > /etc/timezone

# for programming output
RUN localedef -c -f UTF-8 -i en_US en_US.UTF-8
ENV LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8 LANGUAGE=en_US.UTF-8

ENV GRAPHSCOPE_HOME=/opt/graphscope

ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:$GRAPHSCOPE_HOME/lib:$GRAPHSCOPE_HOME/lib64
ENV PATH=$PATH:$GRAPHSCOPE_HOME/bin:/home/graphscope/.local/bin:/home/graphscope/.cargo/bin

ENV JAVA_HOME=/usr/lib/jvm/java
ENV RUST_BACKTRACE=1

# install clang-11 with gold optimizer plugin, depends on header include/plugin-api.h
# COPY --from=llvm /opt/llvm11.0.0 /opt/llvm11
# ENV LLVM11_HOME=/opt/llvm11
# ENV LIBCLANG_PATH=$LLVM11_HOME/lib LLVM_CONFIG_PATH=$LLVM11_HOME/bin/llvm-config

# Copy the thirdparty c++ dependencies, maven, and hadoop
COPY --from=builder /opt/graphscope /opt/graphscope
COPY --from=builder /opt/openmpi /opt/openmpi
RUN chmod +x /opt/graphscope/bin/* /opt/openmpi/bin/*

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# Install jdk-11
RUN yum install -y sudo vim && \
    yum install python3-pip -y && \
    yum remove java-1.8.0-openjdk-devel java-1.8.0-openjdk java-1.8.0-openjdk-headless -y && \
    yum install java-11-openjdk-devel -y && \
    yum clean all -y --enablerepo='*' && \
    rm -rf /var/cache/yum

RUN mkdir -p /opt/graphscope /opt/vineyard && chown -R graphscope:graphscope /opt/graphscope /opt/vineyard

USER graphscope
WORKDIR /home/graphscope

COPY --chown=graphscope:graphscope . /home/graphscope/GraphScope
ARG VINEYARD_VERSION=main
RUN cd /home/graphscope/GraphScope && \
    python3 -m pip install click --user && \
    python3 gsctl.py install-deps dev --v6d-version=$VINEYARD_VERSION && \
    sudo rm -rf /home/graphscope/GraphScope && \
    sudo yum clean all -y && \
    sudo rm -fr /var/cache/yum
RUN echo ". /home/graphscope/.graphscope_env" >> ~/.bashrc

SHELL [ "/usr/bin/scl", "enable", "rh-python38" ]

RUN python3 -m pip --no-cache install pyyaml --user
# Uncomment this line will results in a weird error when using the image together with commands, like
# docker run --rm graphscope/graphscope-dev:latest bash -c 'echo xxx && ls -la'
# The output of `ls -la` would not be shown.
# ENTRYPOINT ["/bin/bash", "-c", ". scl_source enable devtoolset-8 rh-python38 && $0 $@"]
