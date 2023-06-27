FROM hqps-compiler-base:latest

RUN  . "$HOME/.cargo/env"

RUN git clone https://github.com/shirly121/GraphScope.git -b ir_cypher_test --single-branch

RUN cd GraphScope/interactive_engine/compiler && bash -c ". $HOME/.cargo/env && make build"
