FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest

ARG profile=release

COPY ./opt/vineyard/ /usr/local/
COPY ./opt/graphscope /opt/graphscope
COPY ./interactive_engine/conf/* /opt/graphscope/conf/
COPY ./interactive_engine/bin/zetcd /opt/graphscope/bin/zetcd
COPY ./interactive_engine/bin/giectl /opt/graphscope/bin/giectl
COPY ./interactive_engine/src/executor/target/release/executor /opt/graphscope/bin/executor
COPY ./interactive_engine/src/executor/target/release/gaia_executor /opt/graphscope/bin/gaia_executor
COPY ./interactive_engine/src/assembly/target/0.0.1-SNAPSHOT.tar.gz /opt/graphscope/0.0.1-SNAPSHOT.tar.gz

# install mars
# RUN pip3 install git+https://github.com/mars-project/mars.git@35b44ed56e031c252e50373b88b85bd9f454332e#egg=pymars[distributed]

RUN sudo tar -xf /opt/graphscope/0.0.1-SNAPSHOT.tar.gz -C /opt/graphscope \
  && sudo chown -R $(id -u):$(id -g) /opt/graphscope \
  && cd /usr/local/dist && pip3 install ./*.whl \
  && cd /opt/graphscope/dist && pip3 install ./*.whl \
  && sudo ln -sf /opt/graphscope/bin/* /usr/local/bin/ \
  && sudo ln -sfn /opt/graphscope/include/graphscope /usr/local/include/graphscope \
  && sudo ln -sf /opt/graphscope/lib/*so* /usr/local/lib \
  && sudo ln -sf /opt/graphscope/lib64/*so* /usr/local/lib64 \
  && sudo ln -sfn /opt/graphscope/lib64/cmake/graphscope-analytical /usr/local/lib64/cmake/graphscope-analytical

# enable debugging
ENV RUST_BACKTRACE=1
ENV GRAPHSCOPE_HOME=/opt/graphscope
