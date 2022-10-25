# Learning engine

ARG BASE_VERSION=v0.9.0
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:$BASE_VERSION AS builder

ADD . /home/graphscope/GraphScope

RUN sudo chown -R graphscope:graphscope /home/graphscope/GraphScope
RUN cd /home/graphscope/GraphScope/ \
    && mkdir /home/graphscope/install \
    && make gle INSTALL_PREFIX=/home/graphscope/install \
    && cd /home/graphscope/GraphScope/coordinator \
    && export PATH=/opt/python/cp38-cp38/bin:$PATH \
    && python3 setup.py bdist_wheel \
    && cp dist/*.whl /home/graphscope/install/

############### RUNTIME: GLE #######################
FROM registry.cn-hongkong.aliyuncs.com/graphscope/vineyard-runtime:$BASE_VERSION AS learning

COPY --from=builder /home/graphscope/install /opt/graphscope

RUN python3 -m pip install /opt/graphscope/*.whl
RUN rm -f /opt/graphscope/*.whl

USER graphscope
WORKDIR /home/graphscope
