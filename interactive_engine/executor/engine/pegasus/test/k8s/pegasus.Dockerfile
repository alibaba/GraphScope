from registry.cn-hongkong.aliyuncs.com/graphscope/pegasus-base:latest

COPY research/engine /home/graphscope
RUN sudo chown -R graphscope:graphscope /home/graphscope/pegasus
USER graphscope
RUN bash -c "cd /home/graphscope/pegasus && source $HOME/.cargo/env && cargo build --release --examples"

WORKDIR /home/graphscope/
