# Dev and Test

This document describes how to build and test GraphScope Interactive Engine from source code.

## Dev Environment

Here we would use a prebuilt docker image with necessary dependencies installed.

```bash
docker run --name dev -it --shm-size=4096m registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:latest
```

Please refer to [Dev Environment](../development/dev_guide.md#dev-environment) to find more options to get a dev environment.

## Build Interactive Engine

With `gs` command-line utility, you can build interactive engine of GraphScope with a single command.

```bash
# Clone a repo if needed
# git clone https://github.com/alibaba/graphscope
# cd graphscope
./gs make interactive
```

You may want to grab a cup of coffee cause this compiling will take a while, which
includes compiling the java code of GIE compiler, and the rust code of GIE engine.
You may found the built artifacts in `interactive_engine/assembly/target/graphscope.tar.gz`.

## How to Test

You could easily test with the new artifacts with a single command:

Here we set the working directory to local repo.
```bash
export GRAPHSCOPE_HOME=`pwd`
# Here the `pwd` is the root path of GraphScope repository
```
See more about `GRAPHSCOPE_HOME` in [run tests](../development/how_to_test.md#run-tests)

```bash
./gs test interactive
```

It would download the test dataset to the `/tmp/gstest` (if not exists) and run multiple algorithms against various graphs, and compare the result with the ground truth.
