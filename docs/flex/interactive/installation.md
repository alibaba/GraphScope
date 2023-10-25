# Installation


## Prerequisites
- **Docker Installation**: Ensure you have Docker installed and configured on a Mac or Linux machine. If you haven't installed Docker yet, you can find the installation guide [here](https://docs.docker.com/get-docker/).

- **Download Required Files**: Download the compressed file containing Docker configuration files, example data, and other necessary resources from [interactive-latest](https://interactive-release.oss-cn-hangzhou.aliyuncs.com/interactive-latest.zip). Extract the contents to a suitable location on your machine:
 

## Install Interactive

Just unzip the zip file.

```bash
unzip interactive-latest.zip
cd interactive
# Next we assume that we are always in the folder of `GS_INTERACTIVE_HOME`
export INTERACTIVE_HOME=`pwd` 
```

To explore the usage of GraphScope Interactive, please follow [getting_started](./getting_started).