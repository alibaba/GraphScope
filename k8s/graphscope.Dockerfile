# The graphscope image includes all runtime stuffs of graphscope, with analytical engine,
# learning engine and interactive engine installed.

FROM ubuntu:20.04

# Install wheel package from current directory if pass "CI=true" as build options.
# Otherwise, exec `pip install graphscope` from Pypi.
# Example:
#     sudo docker build --build-arg CI=${CI} .
ARG CI=false

# change bash as default
SHELL ["/bin/bash", "-c"]

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# install python3 java8
RUN sed -i 's/archive.ubuntu.com/mirrors.ustc.edu.cn/g' /etc/apt/sources.list && \
    sed -i 's/security.ubuntu.com/mirrors.ustc.edu.cn/g' /etc/apt/sources.list && \
    cat /etc/apt/sources.list && \
    apt update -y && apt install -y \
      curl git openjdk-8-jdk python3-pip sudo && \
    apt clean && rm -fr /var/lib/apt/lists/*

# kubectl v1.19.2
RUN cd /tmp && export KUBE_VER=v1.19.2 && \
    curl -LO https://storage.googleapis.com/kubernetes-release/release/${KUBE_VER}/bin/linux/amd64/kubectl && \
    chmod +x ./kubectl && \
    cd /tmp && \
    mv ./kubectl /usr/local/bin/kubectl

# Add graphscope user with user id 1001
RUN useradd -m graphscope -u 1001 && \
    echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# Change to graphscope user
USER graphscope
WORKDIR /home/graphscope

ENV PATH=${PATH}:/home/graphscope/.local/bin

COPY . /home/graphscope/gs

RUN cd /home/graphscope/gs && \
    if [ "${CI}" == "true" ]; then \
        pushd artifacts/python/dist/wheelhouse; \
        for f in * ; do python3 -m pip install --no-cache-dir $f; done || true; \
        popd; \
        pushd artifacts/coordinator/dist/wheelhouse; \
        python3 -m pip install --no-cache-dir ./*.whl; \
        popd; \
        pushd artifacts/coordinator/dist; \
        python3 -m pip install --no-cache-dir ./*.whl; \
        popd; \
    else \
        python3 -m pip install --no-cache-dir graphscope; \
    fi && \
    sudo rm -fr /home/graphscope/gs && cd ${HOME} && \
    python3 -m pip install --no-cache-dir git+https://github.com/mars-project/mars.git@d09e1e4c3e32ceb05f42d0b5b79775b1ebd299fb#egg=pymars

# init log directory
RUN sudo mkdir /var/log/graphscope \
  && sudo chown -R $(id -u):$(id -g) /var/log/graphscope
