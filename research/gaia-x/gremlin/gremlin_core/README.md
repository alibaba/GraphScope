### Start the RPC Server

The start_rpc_server bin is used to start the RPC server for accepting gremlin requests.
You can type `--help` to find the usage as following:
```bash
cargo run --bin start_rpc_server -- --help
An RPC server for accepting jobs.

USAGE:
    start_rpc_server [OPTIONS]

FLAGS:
        --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -c, --config <config>      the path of config file for pegasus [default: ]
    -h, --hosts <hosts>        the path of hosts file for pegasus communication [default: ]
    -p, --port <rpc_port>      the port to accept RPC connections [default: 1234]
    -i, --index <server_id>    the current server id among all servers [default: 0]
```

When you just want to run locally in one node, you can use following command:
```bash
cargo run --bin start_rpc_server
```

You can also run the engine as a distributed service:
```bash
cargo run --bin start_rpc_server -- -p <RPC_PORT> -i <SERVER_ID> -h <HOST_FILE> -c <CONFIG_FILE>
```
Note that the `SERVER_ID` you specified should be matched with the server in the hosts file, 
which means the matched ip should be the address of the current server.

Here are examples when you want to run in two machines:
```bash
# in the first machine
cargo run --bin start_rpc_server -- -p 1234 -i 0 -h ./resource/conf/hosts.toml

#in the second machine
cargo run --bin start_rpc_server -- -p 1235 -i 1 -h ./resource/conf/hosts.toml
```

`hosts.toml` may be like this:
```
[[peers]]
server_id = 0
ip = 'xxx.xxx.xxx.xxx'
port = 12334  # this should be different from RPC port!
[[peers]]
server_id = 1
ip = 'yyy.yyy.yyy.yyy'
port = 12335
```

