ARG BASE_VERSION=v0.2.1
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:$BASE_VERSION as builder

ARG CI=true
ENV CI=$CI

ARG NETWORKX=ON
ENV NETWORKX=$NETWORKX

ARG profile=debug
ENV profile=$profile

RUN yum install -y perl java-1.8.0-openjdk-devel && \
    yum clean all && \
    rm -fr /var/cache/yum && \
    mkdir -p /tmp/maven /usr/share/maven/ref \
      && curl -fsSL -o /tmp/apache-maven.tar.gz https://apache.osuosl.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz \
      && tar -xzf /tmp/apache-maven.tar.gz -C /tmp/maven --strip-components=1 \
      && rm -f /tmp/apache-maven.tar.gz \
      && /tmp/maven/bin/mvn -v \
    && export LD_LIBRARY_PATH=$(echo "$LD_LIBRARY_PATH" | sed "s/::/:/g") \
    && echo $LD_LIBRARY_PATH \
    && wget http://mirrors.ustc.edu.cn/gnu/libc/glibc-2.18.tar.gz \
    && tar -zxf glibc-2.18.tar.gz \
    && cd glibc-2.18 \
    && mkdir build && cd build \
    && ../configure --prefix=/usr \
    && make -j4 \
    && make install ;

# rust & cargo registry
RUN wget --no-verbose https://golang.org/dl/go1.15.5.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.15.5.linux-amd64.tar.gz && \
    curl -sf -L https://static.rust-lang.org/rustup.sh | \
        sh -s -- -y --profile minimal --default-toolchain 1.48.0 && \
    echo "source ~/.cargo/env" >> ~/.bashrc

ENV PATH=${PATH}:/usr/local/go/bin

COPY . /root/gs
COPY ./interactive_engine/deploy/docker/dockerfile/maven.settings.xml /root/.m2/settings.xml

RUN source ~/.bashrc \
    && echo "build with profile: $profile" \
    && cd /root/gs/interactive_engine \
    && export CMAKE_PREFIX_PATH=/opt/graphscope \
    && export LIBRARY_PATH=$LIBRARY_PATH:/opt/graphscope/lib \
    && export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/graphscope/lib \
    && if [ "$profile" = "release" ]; then \
           echo "release mode" && /tmp/maven/bin/mvn clean package -DskipTests -Drust.compile.mode=release; \
       else \
           echo "debug mode" && /tmp/maven/bin/mvn clean package -DskipTests -Drust.compile.mode=debug ; \
       fi

FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest

RUN mkdir -p /home/maxgraph \
    && export LD_LIBRARY_PATH=$(echo "$LD_LIBRARY_PATH" | sed "s/::/:/g") \
    && wget http://mirrors.ustc.edu.cn/gnu/libc/glibc-2.18.tar.gz \
    && tar -zxf glibc-2.18.tar.gz \
    && cd glibc-2.18 \
    && mkdir build && cd build \
    && ../configure --prefix=/usr \
    && make -j4 \
    && make install ;

COPY --from=builder /opt/graphscope /usr/local/
COPY --from=builder /root/gs/interactive_engine/distribution/target/maxgraph.tar.gz /home/maxgraph/maxgraph.tar.gz
ENV RUST_BACKTRACE=1
ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.282.b08-1.el7_9.x86_64/jre


WORKDIR /home/maxgraph/

