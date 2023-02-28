# Development Guide

This document describes how to build GraphScope from source code.

## Dev Environment

To build GraphScope from source code, you need to prepare a development environment with many dependencies and 
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


## Build All Targets for GraphScope

With `gs` command-line utility, you can build all targets for GraphScope with a single command.

```bash
./gs build
```

You may found the built artifacts in....


TODO(yuansi): give a description about the artifacts...

<<<<<<< HEAD
<<<<<<< HEAD
## Build Components Individually

GraphScope is composed of several components, and you can build each of them separately. If you only changed some codes in one component or intend to use GraphScope in a disaggregated manner, you can build the affected components alone. 

### Build and Test Engines

You may find the guides for building and testing each engine as below.

- [Build and test analytical engine](/analytical_engine/dev_and_test)
- [Build and test interactive engine](https://graphscope.io)
- [Build and test learning engine](https://graphscope.io)

### Build Coordinator

### Build Python Client

=======
## Build Components
=======
## Build Components Individually
>>>>>>> 71c2126a8 (update.)

GraphScope is composed of several components, and you can build each of them separately. If you only changed some codes in one component or intend to use GraphScope in a disaggregated manner, you can build the affected components alone. 

### Build and Test Engines

You may find the guides for building and testing each engine as below.

- [Build and test analytical engine](/analytical_engine/dev_and_test)
- [Build and test interactive engine](https://graphscope.io)
- [Build and test learning engine](https://graphscope.io)

### Build Coordinator

<<<<<<< HEAD
>>>>>>> 20d2b5177 (update docs.)
=======
### Build Python Client

>>>>>>> 71c2126a8 (update.)

