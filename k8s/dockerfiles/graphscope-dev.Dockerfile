FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

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
    apt-get install -y sudo vim python3-pip tzdata tmux && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

RUN mkdir -p /opt/graphscope /opt/vineyard && chown -R graphscope:graphscope /opt/graphscope /opt/vineyard
USER graphscope
WORKDIR /home/graphscope

COPY --chown=graphscope:graphscope . /home/graphscope/GraphScope
ARG VINEYARD_VERSION=main

RUN cd /home/graphscope/GraphScope && \
    python3 -m pip install click && \
    python3 gsctl.py install-deps dev --v6d-version=$VINEYARD_VERSION --cn && \
    cd /home/graphscope && \
    rm -fr GraphScope

RUN echo ". /home/graphscope/.graphscope_env" >> ~/.bashrc
RUN python3 -m pip --no-cache install pyyaml ipython --user
