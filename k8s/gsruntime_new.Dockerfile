
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest

RUN yum remove -y maven \
    && mkdir -p /tmp/maven /usr/share/maven/ref \
    && curl -fsSL -o /tmp/apache-maven.tar.gz https://apache.osuosl.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz \
    && tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 \
    && rm -f /tmp/apache-maven.tar.gz \
    && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn \
    && export LD_LIBRARY_PATH=$(echo "$LD_LIBRARY_PATH" | sed "s/::/:/g") \
    && wget http://mirrors.ustc.edu.cn/gnu/libc/glibc-2.18.tar.gz \
    && tar -zxf glibc-2.18.tar.gz \
    && cd glibc-2.18 \
    && mkdir build && cd build \
    && ../configure --prefix=/usr \
    && make -j4 \
    && make install \
    && wget --no-verbose https://golang.org/dl/go1.15.5.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.15.5.linux-amd64.tar.gz && \
    curl -sf -L https://static.rust-lang.org/rustup.sh | \
        sh -s -- -y --profile minimal --default-toolchain 1.48.0 && \
    echo "source ~/.cargo/env" >> ~/.bashrc

ENV PATH=${PATH}:/usr/local/go/bin
ENV RUST_BACKTRACE=1
ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.282.b08-1.el7_9.x86_64/jre


