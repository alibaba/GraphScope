# Dev and Test

This document describes how to build and test GraphScope Analytical Engine from source code.

## Dev Environment

To build analytcial engine from source code, you need to prepare a development environment with many dependencies and 
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
Since we are going to build GraphScope within the container, here we assign `shm-size` of 4G, which refers to the amount of shared memory
 allocated to a docker container. More options about `docker` command can be found [here](https://docs.docker.com/engine/reference/commandline/cli/).


## Build Analytical Engine

With `gs` command-line utility, you can build all targets for GraphScope with a single command.

```bash
./gs build analytical
```

You may found the built artifacts in....


TODO(yuansi): give a description about the artifacts...

if you need to build the analytical engine with Java support, e.g., if you need to run Giraph/GraphX apps 
on your built engine, or need to write your own applicalitions in Java, you may want to 
build the engine with a option `enable_java`

```bash
./gs build analytcial --enable_java
```

## How to Test

Test with the new artifacts.


