FROM graphscope/manylinux2014:20230407-ext AS ext
FROM graphscope/manylinux2014:20230407

ENV TZ=Asia/Shanghai

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo '$TZ' > /etc/timezone
RUN localedef -c -f UTF-8 -i en_US en_US.UTF-8

ENV LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8 LANGUAGE=en_US.UTF-8
ENV GRAPHSCOPE_HOME=/opt/graphscope

ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:$GRAPHSCOPE_HOME/lib:$GRAPHSCOPE_HOME/lib64
ENV PATH=$PATH:$GRAPHSCOPE_HOME/bin:/home/graphscope/.local/bin:/home/graphscope/.cargo/bin

ENV JAVA_HOME=/usr/lib/jvm/java
ENV RUST_BACKTRACE=1

COPY --from=ext /opt/graphscope /opt/graphscope
COPY --from=ext /opt/openmpi /opt/openmpi

RUN chmod +x /opt/graphscope/bin/* /opt/openmpi/bin/*
RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
RUN yum install -y sudo vim && \
    yum clean all -y --enablerepo='*' && \
    rm -rf /var/cache/yum
RUN mkdir -p /opt/graphscope /opt/vineyard && chown -R graphscope:graphscope /opt/graphscope /opt/vineyard

USER graphscope

WORKDIR /home/graphscope

COPY ./gs ./gs

ARG VINEYARD_VERSION=main

RUN ./gs install-deps dev --v6d-version=$VINEYARD_VERSION -j 2 && \
    sudo yum clean all -y && \
    sudo rm -fr /var/cache/yum
RUN echo ". /home/graphscope/.graphscope_env" >> ~/.bashrc

SHELL [ "/usr/bin/scl", "enable", "rh-python38" ]

RUN python3 -m pip --no-cache install pyyaml --user

