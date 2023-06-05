# Development Guide

This document describes how to build GraphScope from source code.

## Dev Environment

To build GraphScope from source code, you need to prepare a development environment with many dependencies and 
build toolchains. You have two options to prepare the development environment, install all tools and dependencies 
on your local machine, or build it in our provided docker image.

### Install deps on local 

To install all dependencies on your local, use the GraphScope command-line utility [gs](https://github.com/alibaba/GraphScope/blob/main/gs) with the subcommand `install-deps` like this

```bash
./gs install-deps dev

# for more usage, try
# ./gs install-deps -h
```

You could download the `gs` directly or clone the [GraphScope](https://github.com/alibaba/GraphScope) to local, the `gs` is located in the root directory of GraphScope.

### Dev on docker container

We provided a docker image `graphscope-dev` with all tools and dependices included.
If you want to mount your local repository to the container, then you could choose to mount a volume:

    ```bash
    # Use a mirror in HK aliyun to speed up the download if in need.
    export REGISTRY=registry.cn-hongkong.aliyuncs.com
    docker run --name dev -it --shm-size=4096m -v`pwd`/GraphScope:/work/GraphScope $REGISTRY/graphscope/graphscope-dev:latest
    ```

    The local path is mounted to `/work/GraphScope` in the running container.

Or if you prefer not to mount volume, and clone the repo later:

    ```bash
    export REGISTRY=registry.cn-hongkong.aliyuncs.com
    docker run --name dev -it --shm-size=4096m $REGISTRY/graphscope/graphscope-dev:latest
    ```

    ```bash
    # inside the container
    git clone https://github.com/GraphScope
    ```
Since we are going to build GraphScope and load graphs within the container, here we assign `shm-size` of 4G, which refers to the amount of shared memory
 allocated to a docker container. More options about `docker` command can be found [here](https://docs.docker.com/engine/reference/commandline/cli/).


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
- [Build and test learning engine](../interactive_engine/dev_and_test.md)

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
