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
./gs make analytical
```

You may found the built artifacts in `analytical_engine/build/grape_engine`.

Together with the `grape_engine` are shared libraries, or there may have a bunch of test binaries if you choose to build the tests.

There are several build options that could be leveraged to control which part of the analytical engiens to be built.

We give description for the most commonly used options:

- Giraph/GraphX application compatiblity

    if you need to build the analytical engine with Java support, e.g., if you need to run Giraph/GraphX apps on your built engine, or need to write your own applicalitions in Java, you may want to build the engine with a option `ENABLE_JAVA_SDK`.

    ```bash
    ./gs make analytcial -DENABLE_JAVA_SDK=ON
    ```

- Networkx compatiblity

    GraphScope has implemented a bunch of networkx-compatible algorithms for users to seamlessly migrate from networkx, while gain the performance optmizations and the ability to process large-scale graphs.

    ```bash
    ./gs make analytcial -DNETWORKX=ON
    ```

- Tests

    Build tests if you want to develop and run tests of the analytical engine.

    ```bash
    ./gs make analytcial -DBUILD_TESTS=ON
    ```

You could also pass several options all at once, for example:

```bash
./gs make analytcial -DENABLE_JAVA_SDK=ON -DNETWORKX=ON -DBUILD_TESTS=ON
```

````{note}
More in-depth view:

These options are repeated items that would be directed forwared as `cmake` options.
The `CMakeLists.txt` of analytical engine is in `analytical_engine/CMakeLists.txt`.

Take a look at this file if you want to investigate more of the analytical engine.
````

## How to Test

You could easily test with the new artifacts with a single command:

```bash
./gs test analytical
```

It would download the test dataset to the `/tmp/gstest` (if not exists) and run multiple algorithms against various graphs, and compare the result with the ground truth.
