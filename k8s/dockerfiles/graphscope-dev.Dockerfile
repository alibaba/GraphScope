# the graphscope-dev-base image is based on manylinux2014, including all necessary
# dependencies except vineyard for graphscope's wheel package.
ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
#FROM vineyardcloudnative/manylinux-llvm:2014-11.0.0 AS llvm
FROM $REGISTRY/graphscope/manylinux2014:2022-12-12-ext AS ext

FROM $REGISTRY/graphscope/manylinux2014:2022-12-12

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
#COPY --from=llvm /opt/llvm11.0.0 /opt/llvm11
#ENV LLVM11_HOME=/opt/llvm11
#ENV LIBCLANG_PATH=$LLVM11_HOME/lib LLVM_CONFIG_PATH=$LLVM11_HOME/bin/llvm-config

# Copy the thirdparty c++ dependencies, maven, and hadoop
COPY --from=ext /opt/graphscope /opt/graphscope
COPY --from=ext /opt/openmpi /opt/openmpi
RUN chmod +x /opt/graphscope/bin/* /opt/openmpi/bin/*

# change user: graphscope
RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

RUN chown -R graphscope:graphscope /opt/graphscope /opt/openmpi
RUN yum install -y sudo vim && \
    yum clean all -y --enablerepo='*' && \
    rm -rf /var/cache/yum
RUN mkdir /opt/vineyard && chown -R graphscope:graphscope /opt/vineyard
USER graphscope
WORKDIR /home/graphscope

COPY ./gs ./gs
ARG VINEYARD_VERSION=main
RUN ./gs install-deps dev --v6d-version=$VINEYARD_VERSION -j 2 && \
    sudo yum clean all -y && \
    sudo rm -fr /var/cache/yum

SHELL [ "/usr/bin/scl", "enable", "rh-python38" ]

RUN python3 -m pip --no-cache install yaml --user
ENTRYPOINT ["/bin/bash", "-c", "source scl_source enable devtoolset-8 rh-python38 && $0 $@"]
