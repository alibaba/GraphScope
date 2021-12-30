from registry.cn-hongkong.aliyuncs.com/graphscope/pegasus-base:latest

COPY research/engine /home/graphscope
RUN sudo chown -R graphscope:graphscope /home/graphscope/pegasus

WORKDIR /home/graphscope/