FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-runtime:latest

ARG profile=release

COPY ./opt/graphscope /usr/local/graphscope
RUN cd /usr/local/graphscope/dist/ && pip3 install ./*.whl

COPY ./interactive_engine/src/executor/target/release/executor /usr/local/graphscope/bin/executor
COPY ./interactive_engine/src/executor/target/release/gaia_executor /usr/local/graphscope/bin/gaia_executor
COPY ./interactive_engine/bin/giectl /usr/local/graphscope/bin/giectl
COPY ./interactive_engine/bin/zetcd /usr/local/graphscope/bin/zetcd
COPY ./interactive_engine/src/assembly/target/0.0.1-SNAPSHOT.tar.gz /tmp/0.0.1-SNAPSHOT.tar.gz
RUN sudo tar -xf /tmp/0.0.1-SNAPSHOT.tar.gz -C /usr/local/graphscope

# install mars
RUN pip3 install git+https://github.com/mars-project/mars.git@35b44ed56e031c252e50373b88b85bd9f454332e#egg=pymars[distributed]

# enable debugging
ENV RUST_BACKTRACE=1

# copy start script from builder
COPY ./interactive_engine/conf/* /usr/local/graphscope/conf/
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/graphscope/lib:/usr/local/graphscope/lib64
ENV GRAPHSCOPE_HOME=/usr/local/graphscope
ENV PATH=/usr/local/graphscope/bin:${PATH}
