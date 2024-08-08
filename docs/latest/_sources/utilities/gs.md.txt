# Command-line Utility `gsctl`

`gsctl` is a command-line utility for GraphScope. It provides a set of functionalities to make it easy to use GraphScope. These functionalities include building and testing binaries, managing sessions and resources, and more.

## Install/Update `gsctl`

```bash
$ pip3 install gsctl
```

In some cases, such as development on `gsctl`, you may want to build it from source.
To do this, navigate to the directory where the source code is located and run the following command:

```bash
$ cd REPO_HOME
# If you want to develop gsctl,
# please note the entry point is located on:
# /python/graphscope/gsctl/gsctl.py
$ make gsctl
```
This will install `gsctl` in an editable mode, which means that any changes you make to the source code will be reflected in the installed version of `gsctl`.

## Commands

With `gsctl`, you can do the following things. Always remember to use `--help` on a command to get more information.

The `gsctl` command-line utility supports two modes of operation: utility scripts and client/server mode. You can switch between these modes using the `gsctl connect` and `gsctl close` commands.

### Utility Scripts

Default, the `gsctl` provide helper functions and utilities that can be run using gsctl alone.
`gsctl` acts as the command-line entrypoint for GraphScope. Some examples of utility scripts are:

- `gsctl install-deps`, install dependencies for building GraphScope.
- `gsctl make`, build GraphScope executable binaries and artifacts.
- `gsctl make-image`, build GraphScope docker images.
- `gsctl test`, trigger test suites.
- `gsctl connect`, connect to the launched coordinator by ~/.gs/config.
- `gsctl close`, Close the connection from the coordinator.
- `gsctl flexbuild`, Build docker image for Interactive, Insight product.

### Client/Server Mode

To switch to the client/server mode, use the `gsctl connect` command. By default, this command connects gsctl to a launched coordinator using the configuration file located at `${HOME}/.gsctl`;  If `--coordinator-endpoint` parameter is specified, it will treat it as current context and override the configuration file.

Once connected, you can use `gsctl` to communicate with the coordinator which serves the specific Flex product behind it.

#### Change scope

In `gsctl`, you can run commands on a global scope or a local scope. When you connect to a coordinator, you are in the global scope. To change to local scope of a graph, run the `gsctl use GRAPH <graph_identifier>` command. You can find the graph identifier with `gsctl ls` command.

```bash
$ gsctl use GRAPH modern_graph
Using GRAPH modern_graph
```

To switch back to the global scope. run `gsctl use GLOBAL` command.

```bash
$ gsctl use GLOBAL
Using GLOBAL
```
Different scopes have different commands. Always remember to use `--help` on a command to get more information.

#### Close the connection

To disconnect from the coordinator and switch back to the utility scripts mode, you can use the `gsctl close` command. This command closes the connection from the coordinator and allows you to use `gsctl` as a standalone utility again.
