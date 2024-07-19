# Installation

Interactive relies solely on Docker and does not have a strong dependency on the operating system, so in theory, a single installation package could be applicable to all systems. In practical tests, we have successfully tested it on Ubuntu and macOS, as well as on ARM64 and x86_64 architectures.

## Prerequisites
- **Docker Installation**: Ensure you have Docker installed and configured on a Mac or Linux machine. If you haven't installed Docker yet, you can find the installation guide [here](https://docs.docker.com/get-docker/).
- **Python Installtion**: Interactive requires `Python` >= `3.8` and `pip` >= `19.3`.

## Install and Deploy Interactive

We offer a command line tool called `gsctl`, which allows you to install, start, and manage Interactive services.

The Interactive service is deployed in a docker container, ensuring compatibility across different runtime environments.

```{note}

```

```bash
pip3 install gsctl
# Deploy the interactive service in local mode
gsctl instance deploy --type interactive 
```


```{note}
In addition to the interactive server, a coordinator server is also deployed. The coordinator server functions similarly to the `ApiServer` for k8s, enabling users to engage with the GraphScope platform via a streamlined and standardized set of APIs.
```

<!-- 2. Gremlin service is disabled by default, To enable it, try specifying the Gremlin port, see [Service-Accessibility](./installation.md#service-accessibility) -->



The following message will display on your screen to inform you about the available services:

```txt
Coordinator is listening on {coordinator_port} port, you can connect to coordinator by:
    gsctl connect --coordinator-endpoint http://127.0.0.1:{coordinator_port}

Interactive service is ready, you can connect to the interactive service with interactive sdk:
Interactive Admin service is listening at
    http://127.0.0.1:7777,
You can connect to Interactive service with Interactive SDK on this host, with following environment variables declared.

############################################################################################
    export INTERACTIVE_ADMIN_ENDPOINT=http://127.0.0.1:7777
    export INTERACTIVE_STORED_PROC_ENDPOINT=http://127.0.0.1:10000
    export INTERACTIVE_CYPHER_ENDPOINT=neo4j://127.0.0.1:7687
############################################################################################

To access the service from another host via the network, replace `127.0.0.1` with the public IP address and ensure that the ports are exposed to the public network.
```

```{note}
If you have customized the ports when deploying Interactive, remember to replace the default ports with your customized ports.
```

Remember to copy the environment exporting commands and execute them, the Interactive SDKs relies on these environment variables to establish connections to Interactive Services.

To explore the usage of GraphScope Interactive, please follow [getting_started](./getting_started).

## Customizing Ports

By default, Interactive will launch Coordinator Service on `8080`, Interactive Meta Service on `7777`, Interactive Cypher service on `7687` and Stored Procedure Service on `10000`. You can customize these ports.

```bash
gsctl instance deploy --type interactive --coordinator-port 8081 --admin-port 7778 \ 
                --cypher-port 7688 --storedproc-port 10001
```

<!-- By default, the gremlin service is disabled. To enable it, add the option `--gremlin-port 8183`.

```bash
gsctl instance deploy --type interactive --coordinator-port 8081 --admin-port 7778 \ 
                --cypher-port 7688 --storedproc-port 10001 --gremlin-port 8183
``` -->

## Service Accessibility

The Interactive service is deployed using containers, and is exposed through port mapping. If you want to access it on the machine where the service is located, just use the local connection endpoint printed by `gsctl deploy`. If you want to connect to the service from another machine over the network, make sure the port exposed by the Interactive service is open to the public internet and not blocked by security policies or firewalls.
