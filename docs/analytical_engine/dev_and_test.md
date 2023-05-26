# Dev and Test

This guide will walk you through the process of understanding how the code is organized, 
identifying key functions and important code, and building and testing the analytical engine.

## Setup

For simplicity, we suggest you use a prebuilt docker image with necessary dependencies installed.

```bash
docker run --name dev -it --shm-size=4096m registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:latest
```

Alternatively, you can also manually install all dependencies on your local machine.
Please refer to [Dev Environment](../development/dev_guide.md#dev-environment) to find more options to get a dev environment.

After the environment is prepared, clone the repository and enter the `analatical_engine` directory of the repository.

```bash
git clone https://github.com/alibaba/GraphScope.git
cd analatical_engine
```

## Understanding the Codebase

Since the analytical engine inherits from GRAPE, it requires libgrape-lite as a dependency. 
Please note that the core functionalities of libgrape-lite, such as graph structures, graph partitioners, workers, 
communication between workers, and applications, are heavily reused in the analytical engine of GraphScope.

If you want to fully understand the analytcial engine, it is highly recommaned that you start from libgrape-lite.

TODO: [figure]

The code located in the `analytical_engine` directory functions like extensions to libgrape-lite, 
thereby making it full-fledged with the following enhancements:

- K8s support to enable management by the GraphScope coordinator;
- Many built-in algorithms, while libgrape-lite's only ships with 6 analytical algorithms in LDBC Graphalytics Benchmark;
- Property graphs and their flatten/projected graphs support;
- Java support, thus making it possible to execute applications written for Giraph/GraphX on GraphScope.

The code is organized as follows:

- `apps` contains various built-in algorithms/applications.
- `benchmarks` contains code related to performance testing and benchmarking.
- `cmake` contains CMake scripts for configuring the build.
- `core`: This directory contains the core components of the analytical engine, including the application management (app), communication, context, fragment, input/output (io), object, parallel execution, server, utility functions (utils), vertex mapping, worker threads, and others[oai_citation:4](https://github.com/alibaba/GraphScope/tree/main/analytical_engine/core).

5. `frame`: This directory includes files like `app_frame.cc`, `ctx_wrapper_builder.h`, `cython_app_frame.cc`, `flash_app_frame.cc`, `project_frame.cc`, and `property_graph_frame.cc` that seem to be related to different types of computations or operations in the GraphScope analytical engine[oai_citation:5](https://github.com/alibaba/GraphScope/tree/main/analytical_engine/frame).

6. `java`: This directory contains several subdirectories related to Java implementations in the GraphScope analytical engine. These include grape-annotation, grape-demo, grape-giraph, grape-graphx, grape-jdk, grape-rdd-reader, and grape-runtime[oai_citation:6](https://github.com/alibaba/GraphScope/tree/main/analytical_engine/java).

7. `misc`: The misc directory contains miscellaneous files that don't fit into the other categories. In this case, it contains a Python script for linting C++ code (`cpplint.py`)[oai_citation:7](https://github.com/alibaba/GraphScope/tree/main/analytical_engine/misc).

- `test` contains various test scripts and files.

Please note that this is a general overview. To understand the details of how the code is organized, you would need to read the code and any associated documentation, or possibly reach out to the project maintainers.


    - Overview of the codebase structure, explaining what each main directory and file is for.
    - Deep dive into key directories and files, explaining important classes, functions, and data structures.
    - Include diagrams or flowcharts where they can help illustrate code organization or data flow.

4. **Making Modifications**
    - Guide to creating a new branch for their changes.
    - Instructions on how to navigate the codebase to find the areas relevant to the changes they want to make.
    - Best practices for modifying the code, such as adhering to existing coding styles and conventions, adding comments, and avoiding large changes in a single commit.

5. **Building the Code**
    - Step-by-step guide to building the project, explaining any build scripts or tools used.
    - Instructions for resolving common build errors.

6. **Testing**
    - Explanation of the testing philosophy and framework used in GraphScope.
    - Instructions on how to run existing tests.
    - Guide to writing new tests for their changes, emphasizing the importance of thorough testing.

7. **Submitting Changes**
    - Instructions for committing and pushing their changes.
    - Guide to opening a pull request, including any project-specific guidelines for PR descriptions, linking issues, etc.

8. **Conclusion**
    - Summarize the main points of the tutorial.
    - Encourage the reader to contribute to the project and provide links to any further resources, such as more detailed documentation, community forums, etc.

Remember, this is a very high-level outline, and the specific details will depend on the GraphScope project and the specific changes a developer wants to make. A good tutorial would involve going through the process yourself to ensure all the steps are accurate and clear.



This document describes how to build and test GraphScope Analytical Engine from source code.


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
./gs make analytical-install --install-prefix /usr/local
```

````{note}
More in-depth view:

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
