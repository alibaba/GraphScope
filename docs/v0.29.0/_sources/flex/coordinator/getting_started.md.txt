# Getting Started

This guide will walk you through the process of setting up the coordinator, connecting to it via [gsctl](../../utilities/gs.md) and execuing your first command.

## Preparation

````{tip}
Command-Line Tool: GraphScope offers an command-line tool of `gsctl` to provide a set of functionalities to make it easy to use GraphScope, like managing and operating GraphScope Flex instance. For an in-depth guide on how to use this tool, please visit the [doc page](../../utilities/gs.md) of gsctl.
````

Typically, Coordinator is packaged with products under the GraphScope Flex architecture, such as [Interactive](../interactive_intro.md), thus make sure the specific product is installed before proceeding on. If not, please follow the code below to deploy the instance.

```{note}
In local mode, GraphScope will map the host port to the docker container. Remember to use `gsctl instance deploy --help` to get more information.
If you encounter a port binding problem while deploying instance, please destroy the instance first and then redeploy it.
```

```bash
pip3 install gsctl
# Deploy the Interactive instance in local mode
gsctl instance deploy --type interactive
```

See [Installation Guide](../interactive/installation.md) for more detailed information on how to install and deploy Interactive.

## Connect to Coordinator Service(Interactive) via gsctl

You could connect the `coordinator service` via `gsctl`.

```bash
gsctl connect --coordinator-endpoint http://127.0.0.1:8080
# change the port number if you have customized the coordinator port.
```

## Check Service Status

After connecting to the Coordinator Service, you can now view what we have initially for you.

```bash
gsctl ls -l
```

Actually, a builtin graph is provided with name `gs_interactive_default_graph`. Now you can switch to the graph context:

```bash
gsctl use GRAPH gs_interactive_default_graph
gsctl service status # show current service status
```

As seen from the output, the Interactive service is already running on the built-in graph. For more commands, please refer to the specific Flex product.

## Close the connection

If you want to disconnect to coordinator, just type

```bash
gsctl close
```

## Destroy the Interactive Instance

If you want to shutdown and uninstall the Interactive instance,

```bash
gsctl instance destroy --type interactive
```

**This will remove all the graph and data for the Interactive instance.**
