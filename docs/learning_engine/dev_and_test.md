# Dev and Test

This document describes how to build and test GraphScope Learning Engine from source code.

## Dev Environment

To build the learning engine from source code, you need to prepare a development environment with many dependencies and
build toolchains. You have two options to prepare the development environment, install all tools and dependencies
on your local machine, or build it in our provided docker image.

### Install deps on local

To install all dependencies on your local, use the GraphScope command-line utility `gs` with the subcommand
`install-deps` like this

```bash
./gs install-deps dev

# for more usage, try
# ./gs install-deps -h
```

### Dev on docker container

We provided a docker image `graphscope-dev` with all tools and dependices included.

```bash
# Use a mirror in HK aliyun to speed up the download if in need.
# export REGISTRY=registry.cn-hongkong.aliyuncs.com/
# TODO(yuansi): make it works

docker run --rm -it --shm-size=4096m REGISTRY/graphscope/graphscope-dev:latest
```

Since we are going to build GraphScope within the container, here we assign `shm-size` of 4G,
which refers to the amount of shared memory allocated to a docker container.
More options about `docker` command can be found [here](https://docs.docker.com/engine/reference/commandline/cli/).

## Build Learning Engine

You can build all targets for GraphScope Learning Engine with a single command.

```bash
make learning
```

## How to Test

Test with the new artifacts.
