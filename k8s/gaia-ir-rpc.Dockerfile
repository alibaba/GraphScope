FROM gaia-ir

# set environment
ENV RUST_LOG info

# start RPC Server
CMD ["/opt/GraphScope/interactive_engine/executor/ir/target/release/start_rpc_server", "--config", "/opt/GraphScope/interactive_engine/executor/ir/integrated/config/"]