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

RUN sed -i 's/archive.ubuntu.com/mirrors.ustc.edu.cn/g' /etc/apt/sources.list && \
    sed -i 's/security.ubuntu.com/mirrors.ustc.edu.cn/g' /etc/apt/sources.list && \
    cat /etc/apt/sources.list && \
    apt update -y && apt install -y \
      gcc python3-pip openssh-server sudo telnet zip && \
    apt clean && rm -fr /var/lib/apt/lists/*

# Add graphscope user with user id 1001
RUN useradd -m graphscope -u 1001 && \
  echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# Change to graphscope user
USER graphscope
WORKDIR /home/graphscope
ENV PATH=${PATH}:/home/graphscope/.local/bin

COPY . /home/graphscope/gs

# Install graphscope client
RUN cd /home/graphscope/gs && \
    if [ "${CI}" == "true" ]; then \
        pushd artifacts/python/dist/wheelhouse; \
        for f in * ; do python3 -m pip install --no-cache-dir $f; done || true; \
        popd; \
    else \
        python3 -m pip install --no-cache-dir graphscope_client -U; \
    fi && \
    python3 -m pip install --no-cache-dir jupyterlab && \
    sudo rm -fr /home/graphscope/gs

CMD ["jupyter", "lab", "--port=8888", "--no-browser", "--ip=0.0.0.0"]
