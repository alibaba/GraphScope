# Command-line Utility `gsctl`

`gsctl` is a command-line utility for GraphScope. It is shipped with `graphscope-client` and provides a set of functionalities to make it easy to use GraphScope. These functionalities include building and testing binaries, managing sessions and resources, and more.

## Install/Update `gsctl`

Since it is shipped with python package `graphscope-client`, the `gsctl` command will be available in your terminal after installing GraphScope:
```bash
pip install graphscope-client
```

In some cases, such as development on `gsctl`, you may want to build it from source. 
To do this, navigate to the directory where the source code is located and run the following command:

```bash
cd REPO_HOME/python
# If you want to develop gsctl, 
# please note the entry point is located on: 
# /python/graphscope/gsctl/gsctl.py
pip install --editable .
```
This will install `gsctl` in an editable mode, which means that any changes you make to the source code will be reflected in the installed version of `gsctl`.

## Commands

With `gsctl`, you can do the following things. Always remember to use `--help` on a command to get more information.

The `gsctl` command-line utility supports two modes of operation: utility scripts and client/server mode. You can switch between these modes using the
`gsctl connect` and `gsctl close` commands.

### Utility Scripts

Default, the `gsctl` provide helper functions and utilities that can be run using gsctl alone.
`gsctl` acts as the command-line entrypoint for GraphScope. Some examples of utility scripts are:

- `gsctl install-deps`, install dependencies for building GraphScope.
- `gsctl make`, build GraphScope executable binaries and artifacts.
- `gsctl make-image`, build GraphScope docker images.
- `gsctl test`, trigger test suites.
- `gsctl connect`, connect to the launched coordinator by ~/.gs/config.
- `gsctl close`, Close the connection from the coordinator.


### Client/Server Mode

To switch to the client/server mode, use the `gsctl connect` command. This command connects gsctl to a launched coordinator using the configuration file located at ~/.gsconfig.
Once connected, you can use `gsctl` to communicate with the coordinator and send commands that will be executed on the coordinator side.

To disconnect from the coordinator and switch back to the utility scripts mode, you can use the `gsctl close` command. This command closes the connection from the coordinator
and allows you to use `gsctl` as a standalone utility again.
