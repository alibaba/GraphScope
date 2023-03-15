# Deploy GraphScope on Local

This guide will walk you through the process of installing GraphScope on your local machine.

## Prerequisites

- Ubuntu 20.04 or later, CentOS 7 or later, or macOS 11.2.1 (Big Sur) or later
- Python 3.7 ~ 3.11
- OpenJDK 8 or later (If you want to use GIE)

## Installation

### Install from packages
GraphScope is distributed by [Python wheels](https://pypi.org/project/graphscope) , and could be installed by [pip](https://pip.pypa.io/en/stable/) directly.

#### Install stable version of GraphScope
- Use pip to install latest stable graphscope package

    ```bash
    python3 -m pip install graphscope --upgrade
    ```

- Use Aliyun mirror to accelerate downloading if you are in CN

    ```bash
    python3 -m pip install graphscope -i http://mirrors.aliyun.com/pypi/simple/ --trusted-host=mirrors.aliyun.com  --upgrade
    ```

The above command will download all the components required for running GraphScope in standalone mode to your local machine.

#### Install preview version of GraphScope
If you wish to experience the latest features, you can install the preview version, which is built every night.

```bash
python3 -m pip install graphscope --pre
```

For a quick and fresh start, you could use a Docker development environment to get deploy

```bash
docker run --name dev -it --shm-size=4096m ubuntu:latest

# inside the docker
apt-get update -y && apt-get install python3-pip default-jdk -y
python3 -m pip install graphscope ipython tensorflow
```

### Install from source

Build GraphScope from source and install it on Linux and macOS

#### Setup for Linux and macOS

You could use the cli script `gs` to install development dependencies, which is shipped with the source code.

```bash
# Download source code
git clone https://github.com/alibaba/graphscope
cd graphscope
./gs install-deps dev
```

#### Use prebuilt Docker image

GraphScope's Docker development images are an easy way to set up an environment to build  packages from source. These images already contain dependencies required to build GraphScope

```bash
docker pull registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:latest
docker run --name dev -it --shm-size=4096m registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:latest
git clone https://github.com/alibaba/graphscope
```

#### Build and install

- Make all components

```bash
# Inside the root directory of GraphScope source.
make
make install
```

### Notes
- When you explore around using GAE, it may require on-the-fly algorithm compilation, which requires installation of cmake and g++ (if not available). e.g. to install build toolchains on ubuntu:

    ```bash
    apt update -y &&
    apt install cmake build-essential -y
    ```

- GLE doesn't support running on Python 3.11, cause tensorflow lacks supports for the version.

- The support for arm-based platform is in preliminary stage.
