# the graphscope-dev-base image is based on manylinux2014, including all necessary
# dependencies except vineyard for graphscope's wheel package.
ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
FROM vineyardcloudnative/manylinux-llvm:2014-11.0.0 AS llvm

FROM $REGISTRY/graphscope/manylinux2014:2022-08-16-53df7cb

# yum install dependencies
RUN yum install -y perl which sudo wget libunwind-devel vim zip java-1.8.0-openjdk-devel msgpack-devel rapidjson-devel libuuid-devel && \
    yum clean all -y && \
    rm -fr /var/cache/yum

ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib:/usr/local/lib64
ENV JAVA_HOME=/usr/lib/jvm/java

# install clang-11 with gold optimizer plugin, depends on header include/plugin-api.h
COPY --from=llvm /opt/llvm11.0.0 /opt/llvm11
ENV LLVM11_HOME=/opt/llvm11
ENV LIBCLANG_PATH=$LLVM11_HOME/lib LLVM_CONFIG_PATH=$LLVM11_HOME/bin/llvm-config

COPY build_scripts /build_scripts

# COPY ./download /download
RUN mkdir /download

RUN export WORKDIR=/download && bash /build_scripts/build_vineyard_dependencies.sh
RUN export WORKDIR=/download && bash /build_scripts/build_patchelf.sh
RUN export WORKDIR=/download && bash /build_scripts/build_maven.sh
ENV PATH=$PATH:/opt/apache-maven-3.8.6/bin
RUN rm -rf /build_scripts /download

# install python3.8 deps for all
RUN /opt/python/cp38-cp38/bin/pip3 install -U pip && \
    /opt/python/cp38-cp38/bin/pip3 --no-cache-dir install auditwheel==5.0.0 daemons etcd-distro gremlinpython \
        hdfs3 fsspec oss2 s3fs ipython kubernetes libclang networkx==2.4 numpy pandas parsec pycryptodome \
        pyorc pytest scipy scikit_learn wheel && \
    /opt/python/cp38-cp38/bin/pip3 --no-cache-dir install Cython --pre -U
ENV PATH=/opt/python/cp38-cp38/bin:$PATH

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo '$TZ' > /etc/timezone

# for programming output
RUN localedef -c -f UTF-8 -i en_US en_US.UTF-8
ENV LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8 LANGUAGE=en_US.UTF-8

# change user: graphscope
RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

USER graphscope
WORKDIR /home/graphscope
ENV PATH=/home/graphscope/.local/bin:$PATH

# Rust
RUN sudo chown -R $(id -u):$(id -g) /tmp
RUN curl -sf -L https://static.rust-lang.org/rustup.sh | \
        sh -s -- -y --profile minimal --default-toolchain stable && \
    echo "source /home/graphscope/.cargo/env" >> ~/.bashrc && \
    source /home/graphscope/.cargo/env && \
    rustup component add rustfmt
ENV PATH=/home/graphscope/.cargo/bin:$PATH
ENV RUST_BACKTRACE=1
