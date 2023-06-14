# Development Guide

This document describes how to build GraphScope from source code.

## Dev Environment

To build GraphScope from source code, you need to prepare a development environment with many dependencies and 
build toolchains. You using the following ways to prepare the development environment
  - Use our provided [dev container](https://code.visualstudio.com/docs/devcontainers/containers)
  - Install all tools and dependencies on your local machine

We strongly recommend you to use the dev containers to develop and test. 

### Develop with dev containers.

We provided a docker image `graphscope-dev` with all tools and dependencies included.
Additionally, the [devcontainer.json](https://github.com/alibaba/GraphScope/blob/main/.devcontainer/devcontainer.json) allows to quickly setup
a develop environment within Visual Studio Code.

By using dev containers, developers can ensure that their development environment is consistent across different machines and operating systems, 
making it easier to collaborate with others and maintain a stable development environment.

To use the dev containers for GraphScope development, you can follow these steps:

1. Install Docker on your machine if you haven't already. Or install on the remote machine if you prefer to develop remotely.
2. Clone the GraphScope repository.
3. Open Visual Studio Code and navigate to the GraphScope repository.
4. If you have the Remote Development extension installed, you can click on the green icon in the bottom left corner of the window and select "Remote-Containers: Reopen in Container". If you don't have the extension installed, you can install it from the Visual Studio Code marketplace.
5. If you can't see the `Reopen in Container` prompt, you could hit `Command + Shift + P` (macOS) or `Ctrl + Shift + P` (Windows, Linux) and type `Reopen in Container`.
6. Once the container is built, you can open a terminal within Visual Studio Code.
7. You could customize the `devcontainer.json` to suit your own needs. Once you have made your changes, you could rebuild the container with your customizations.



### Install deps on local 

To install all dependencies on your local, use the GraphScope command-line utility [gs](https://github.com/alibaba/GraphScope/blob/main/gs) with the subcommand `install-deps` like this

```bash
./gs install-deps dev

# for more usage, try
# ./gs install-deps -h
```

You could download the `gs` directly or clone the [GraphScope](https://github.com/alibaba/GraphScope) to local, the `gs` is located in the root directory of GraphScope.



## Build All Targets for GraphScope

With `gs` command-line utility, you can build all targets for GraphScope with a single command.

```bash
./gs make all
```

This would build all targets sequentially, here we
You may found the built artifacts in several places according to each components.

- analytical engine: `analytical_engine/build`
- interactive engine: `interactive_engine/assembly/target`
- learning engine: `learning_engine/graph-learn/graphlearn/cmake-build`

And you could install them to one place by

```bash
./gs make install [--prefix=/opt/graphscope]
```

By default it would install all artifacts to `/opt/graphscope`, and you could specify another location by assigning the value of `--prefix`.

## Build Components Individually

GraphScope is composed of several components, and you can build each of them separately. If you only changed some codes in one component or intend to use GraphScope in a disaggregated manner, you can build the affected components alone. 

### Build and Test Engines

You may find the guides for building and testing each engine as below.

- [Build and test analytical engine](../analytical_engine/dev_and_test.md)
- [Build and test interactive engine](../interactive_engine/dev_and_test.md)
- [Build and test learning engine](../learning_engine/dev_and_test.md)

### Build Coordinator

The gscoordinator package is responsible for launching engines, circulating and propagating messages and errors, and scheduling the workload operations.


This will install coordinator package, thus make `import gscoordinator` work

````{tip}
The package would be installed in [editable mode](https://pip.pypa.io/en/stable/cli/pip_install/#cmdoption-e), which means any changed you made in local directory will take effect.
````

```shell
./gs make coordinator
```

### Build Python Client

The `graphscope` package is the entrypoint for playing with GraphScope.

This will install the graphscope-client package, thus make `import graphscope` work.

````{tip}
This package would also be installed in [editable mode](https://pip.pypa.io/en/stable/cli/pip_install/#cmdoption-e)
````

```shell
./gs make client
```
