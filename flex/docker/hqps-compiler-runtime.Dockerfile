FROM hqps-compiler-base:latest

RUN git clone https://github.com/zhanglei1949/GraphScope.git -b ir_hqps_test --single-branch && cd GraphScope/interactive_engine/compiler && \
    make build 
