# Install GraphScope on Local

This guide will walk you through the process of installing GraphScope on your local machine.

## Prerequisites

- Ubuntu 20.04 or later, CentOS 7 or later, or macOS 11.2.1 (Big Sur) or later
- Python 3.7 ~ 3.11
- OpenJDK 8 or later (If you want to use GIE)

## Install from packages
GraphScope is distributed via [Python wheels](https://pypi.org/project/graphscope), and could be installed by [pip](https://pip.pypa.io/en/stable/) directly.

### Install stable version of GraphScope
You can use `pip` to install latest stable graphscope package

```bash
python3 -m pip install graphscope --upgrade
```

````{tip}
If you occur a very low downloading speed, try to use a mirror site for the pip.

```bash
python3 -m pip install graphscope --upgrade \
    -i http://mirrors.aliyun.com/pypi/simple/ --trusted-host=mirrors.aliyun.com
```
````

The above command will download all the components required for running GraphScope in standalone mode on your local machine.

### Install preview version of GraphScope
If you wish to experience the latest features, you can install the preview version, which is built and released in a nightly manner.

```bash
python3 -m pip install graphscope --pre
```

To get a clean and tidy environment, you can also use a Docker environment to get started quickly.

```bash
docker run --name dev -it --shm-size=4096m ubuntu:latest

# inside the docker
apt-get update -y && apt-get install python3-pip default-jdk -y
python3 -m pip install graphscope ipython tensorflow
```

## Install from source

Optionally, You can build GraphScope from source and install it on your machine.

### Setup build environment for Linux and macOS

Build GraphScope from source needs many libraries and tools as dependencies. We provide a utility script `gs`
to help you install all the dependencies.

```bash
# Download source code
git clone https://github.com/alibaba/graphscope
cd graphscope

./gs install-deps dev
# use --help to get more usage.
```

### Use dev image with all dependencies installed

To make our life easier, we provide a pre-built docker image with all the dependencies installed. 
You can use pull it and work in a container with this image to build GraphScope.

```bash
docker pull registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:latest
docker run --name dev -it --shm-size=4096m registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:latest

# In the container, download the source code.
git clone https://github.com/alibaba/graphscope
```

### Build and install

After the dependencies are installed on your local or in the container, 
you can build and install GraphScope in root directory of the source code.

```bash
make
make install
```

````{note}
Analytical engine(GAE) may require on-the-fly compilation, which needs clang or g++. Hence additional setup steps may in need to install build-essentials. e.g., on ubuntu: 

```bash
apt update -y &&
apt install cmake build-essential -y
```
````

````{note}
Learning engine (GLE) doesn't support running on Python 3.11 currently, cause TensorFlow lacks supports for this version.
````

````{note}
Currently, the support for arm-based platforms is very preliminary, and not ready for production yet.
````