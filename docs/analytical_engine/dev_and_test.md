# Dev and Test

This document describes how to build and test GraphScope Analytical Engine from source code.

## Dev Environment

Here we would use a prebuilt docker image with necessary dependencies installed.

```bash
docker run --name dev -it --shm-size=4096m registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:latest
```

Please refer to [Dev Environment](../development/dev_guide.md#dev-environment) to find more options to get a dev environment.

## Build Analytical Engine

With `gs` command-line utility, you can build analytical engine of GraphScope with a single command.

```bash
# Clone a repo if needed
# git clone https://github.com/alibaba/graphscope
# cd graphscope
./gs make analytical
```

You may found the built artifacts in `analytical_engine/build/grape_engine`.

Together with the `grape_engine` are shared libraries, or there may have a bunch of test binaries if you choose to build the tests.

You could install it to a location by

```bash
./gs make analytcial-install --install-prefix /usr/local
```

````{note}
More in-depth view:

These options are repeated items that would be directed forwared as `cmake` options.
The `CMakeLists.txt` of analytical engine is in `analytical_engine/CMakeLists.txt`.

Take a look at this file if you want to investigate more of the analytical engine.
````

## How to Test

You could easily test with the new artifacts with a single command:

Here we set the working directory to local repo.
```bash
export GRAPHSCOPE_HOME=`pwd`
# Here the `pwd` is the root path of GraphScope repository
```
See more about `GRAPHSCOPE_HOME` in [run tests](../development/how_to_test.md#run-tests)

```bash
./gs test analytical
```

It would download the test dataset to the `/tmp/gstest` (if not exists) and run multiple algorithms against various graphs, and compare the result with the ground truth.
