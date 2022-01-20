FROM ubuntu:20.04

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone 

# apt dependencies
RUN apt update -y && apt install -y curl wget sudo git build-essential host

# change user: graphscope
RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# install rust
RUN cd /tmp && \
  wget --no-verbose https://golang.org/dl/go1.15.5.linux-amd64.tar.gz && \
  tar -C /usr/local -xzf go1.15.5.linux-amd64.tar.gz

# kubectl v1.19.2
RUN cd /tmp && export KUBE_VER=v1.19.2 && \
    curl -LO https://storage.googleapis.com/kubernetes-release/release/${KUBE_VER}/bin/linux/amd64/kubectl && \
    chmod +x ./kubectl && \
    cd /tmp && \
    mv ./kubectl /usr/local/bin/kubectl

USER graphscope
WORKDIR /home/graphscope
ENV PATH=${PATH}:/home/graphscope/.local/bin

RUN curl -sf -L https://static.rust-lang.org/rustup.sh | \
  sh -s -- -y --profile minimal --default-toolchain 1.48.0 && \
  echo "$source $HOME/.cargo/env" >> ~/.bashrc && \
  bash -c "rustup component add rustfmt"

ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib
