# Installation

Interactive relies solely on Docker and does not have a strong dependency on the operating system, so in theory, a single installation package could be applicable to all systems. In practical tests, we have successfully tested it on Ubuntu and macOS, as well as on ARM64 and x86_64 architectures.

## Prerequisites
- **Docker Installation**: Ensure you have Docker installed and configured on a Mac or Linux machine. If you haven't installed Docker yet, you can find the installation guide [here](https://docs.docker.com/get-docker/).

- **Download Required Files**: Download the compressed file containing Docker configuration files, example data, and other necessary resources from [interactive-latest](https://interactive-release.oss-cn-hangzhou.aliyuncs.com/interactive-latest.zip). Extract the contents to a suitable location on your machine:
 

## Install Interactive

A command-line tool `gsctl` is provided to help you install and manage Interactive.

```bash
pip3 install gsctl

gsctl instance deploy --type interactive 
# Or you can customize the port number
gsctl instance deploy --type interactive --coordinator-port 8081 --admin-port 7778 \ 
                --cypher-port 7688 --storedproc-port 10001 --gremlin-port 8183
```

The following message will display on your screen to inform you about the available services:

```txt
Coordinator is listening on {coordinator_port} port, you can connect to coordinator by:
    gsctl connect --coordinator-endpoint http://127.0.0.1:{coordinator_port}

Interactive service is ready, you can connect to the interactive service with interactive sdk:
Interactive Admin service is listening at
    http://127.0.0.1{admin_port},
You can connect to admin service with Interactive SDK, with following environment variables declared.

############################################################################################
    export INTERACTIVE_ADMIN_ENDPOINT=http://127.0.0.1:{admin_port}
    export INTERACTIVE_STORED_PROC_ENDPOINT=http://127.0.0.1:{storedproc_port}
    export INTERACTIVE_CYPHER_ENDPOINT=neo4j://127.0.0.1:{cypher_port}
    export INTERACTIVE_GREMLIN_ENDPOINT=ws://127.0.0.1:{gremlin_port}/gremlin
############################################################################################
```

Remember to copy the environment exporting commands and execute them, the Interactive SDKs relies on these environment variables to establish connections to Interactive Services.

To explore the usage of GraphScope Interactive, please follow [getting_started](./getting_started).