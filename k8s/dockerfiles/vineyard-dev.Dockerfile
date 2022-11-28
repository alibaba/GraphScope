# The vineyard-dev image including all vineyard-related dependencies
# that could graphscope analytical engine.

FROM centos:7.9.2009

RUN yum install -y centos-release-scl-rh perl which sudo wget git libunwind-devel && \
    INSTALL_PKGS="devtoolset-10-gcc-c++ rh-python38-python-pip rh-python38-python-devel" && \
    yum install -y --setopt=tsflags=nodocs $INSTALL_PKGS && \
    yum clean all -y --enablerepo='*' && \
    rm -rf /var/cache/yum

SHELL [ "/usr/bin/scl", "enable", "devtoolset-10", "rh-python38" ]

RUN python3 -m pip install --no-cache-dir libclang etcd-distro

ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64

# COPY ./download /download
RUN mkdir /download

COPY build_scripts/build_vineyard_dependencies.sh /build_scripts/build_vineyard_dependencies.sh
RUN export WORKDIR=/download && bash /build_scripts/build_vineyard_dependencies.sh

COPY build_scripts/build_vineyard.sh /build_scripts/build_vineyard.sh
RUN export WORKDIR=/download && bash /build_scripts/build_vineyard.sh
RUN rm -rf /build_scripts /download

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo '$TZ' > /etc/timezone

# for programming output
RUN localedef -c -f UTF-8 -i en_US en_US.UTF-8
ENV LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8 LANGUAGE=en_US.UTF-8

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# Enable rh-python, devtoolsets-10 binary
ENTRYPOINT [ "/usr/bin/scl", "enable", "devtoolset-10", "rh-python38", "bash" ]