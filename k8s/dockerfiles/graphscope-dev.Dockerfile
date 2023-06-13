FROM ubuntu:22.04

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo '$TZ' > /etc/timezone

ENV GRAPHSCOPE_HOME=/opt/graphscope
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:$GRAPHSCOPE_HOME/lib:$GRAPHSCOPE_HOME/lib64
ENV PATH=$PATH:$GRAPHSCOPE_HOME/bin:/home/graphscope/.local/bin:/home/graphscope/.cargo/bin

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV RUST_BACKTRACE=1

RUN apt-get update && \
    apt-get install -y sudo vim && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

RUN mkdir -p /opt/graphscope /opt/vineyard && chown -R graphscope:graphscope /opt/graphscope /opt/vineyard
USER graphscope
WORKDIR /home/graphscope

COPY ./gs ./gs
ARG VINEYARD_VERSION=main
RUN ./gs install-deps dev --v6d-version=$VINEYARD_VERSION --cn -j $(nproc) && \
    sudo apt-get clean -y && \
    sudo rm -rf /var/lib/apt/lists/*
RUN echo ". /home/graphscope/.graphscope_env" >> ~/.bashrc
RUN python3 -m pip --no-cache install pyyaml --user
RUN rm ./gs
