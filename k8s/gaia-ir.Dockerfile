FROM rust:1.60

# update source and dependence
RUN chmod 777 /tmp && \
    sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list && \
    sed -i 's/security.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list && \
    sed -i 's/http/https/g' /etc/apt/sources.list && \
    apt-get update -y

# set language and encoding
RUN apt-get -y install locales &&\
    sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen
ENV LANG en_US.UTF-8  
ENV LANGUAGE en_US:en  
ENV LC_ALL en_US.UTF-8

# install cmake/clang/protobuf-compiler/rustfmt
RUN apt-get install -y cmake=3.18.4-2+deb11u1 && \
    apt-get install -y clang=1:11.0-51+nmu5 && \
    apt-get install -y protobuf-compiler=3.12.4-1 && \
    rustup component add rustfmt

# install Java Dependencies
RUN apt-get install -y openjdk-11-jdk && \
    apt-get install -y maven=3.6.3-5

# git clone GraphScope to opt and compile
RUN cd /opt && \
    git clone https://github.com/shirly121/GraphScope.git && \
    cd /opt/GraphScope/interactive_engine/executor/ir && \
    cargo build --release
