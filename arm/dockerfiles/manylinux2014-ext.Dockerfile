FROM centos:7 AS builder

ENV TZ=Asia/Shanghai

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo '$TZ' > /etc/timezone


COPY ./gs ./gs
RUN ./gs install-deps dev --cn --for-analytical --no-v6d  && \
    yum clean all -y --enablerepo='*' && \
    rm -fr /var/cache/yum

RUN if [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
      curl -sS -LO https://archive.apache.org/dist/hadoop/common/hadoop-3.3.0/hadoop-3.3.0-aarch64.tar.gz; \
      tar xzf hadoop-3.3.0-aarch64.tar.gz -C /opt/; \
    else \
      curl -sS -LO https://archive.apache.org/dist/hadoop/common/hadoop-3.3.0/hadoop-3.3.0.tar.gz; \
      tar xzf hadoop-3.3.0.tar.gz -C /opt/; \
    fi && \
    rm -rf hadoop-3.3.0* && \
    cd /opt/hadoop-3.3.0/share/ && \
    rm -rf doc hadoop/client  hadoop/mapreduce  hadoop/tools  hadoop/yarn

FROM centos:7

COPY --from=builder /opt/graphscope /opt/graphscope
COPY --from=builder /opt/openmpi /opt/openmpi
COPY --from=builder /opt/hadoop-3.3.0 /opt/hadoop-3.3.0
