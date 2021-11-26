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

# change apt source to aliyun
# install python3 java8
RUN sed -i 's/archive.ubuntu.com/mirrors.ustc.edu.cn/g' /etc/apt/sources.list && \
    sed -i 's/security.ubuntu.com/mirrors.ustc.edu.cn/g' /etc/apt/sources.list && \
    cat /etc/apt/sources.list && \
    apt update -y && apt install -y \
      git openjdk-8-jdk python3-pip sudo && \
    apt clean && rm -fr /var/lib/apt/lists/*

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
        for f in * ; do python3 -m pip install $f; done || true; \
        popd; \
        pushd artifacts/coordinator/dist/wheelhouse; \
        python3 -m pip install ./*.whl; \
        popd; \
        pushd artifacts/coordinator/dist; \
        python3 -m pip install ./*.whl; \
    else \
        python3 -m pip install graphscope; \
    fi && \
    pip3 install git+https://github.com/mars-project/mars.git@d09e1e4c3e32ceb05f42d0b5b79775b1ebd299fb#egg=pymars
