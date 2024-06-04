# The Administrative tool
`gs_interactive` is a command-line tool designed to manage various aspects of GraphScope Interactive. This document provides a comprehensive guide on how to utilize its functionalities.

## Overview
`gs_interactive` is organized into the following sections:

1. **root**: Without any specific option, this is used to initialize and destroy the docker image for GraphScope Interactive.
2. **database**: Handles all data-related operations, such as creating/removing graphs and importing data.
3. **service**: Manages the GraphScope Interactive service.
4. **procedure**: Manages the stored procedures of GraphScope Interactive.

## Detailed Usage

### Root Commands
- `gs_interactive -h (--help)`: Showing help message.
- `gs_interactive init -c (--config) interactive.yaml`: Initializes the docker image for GraphScope Interactive. If the image isn't present, it will be downloaded. This command also configures volume and port mappings.
- `gs_interactive destroy`: Destroys the docker image of GraphScope Interactive. This action will erase all data and is irreversible.

### Service Commands
- `gs_interactive service -h (--help)`: Showing help message.
- `gs_interactive service start [-g (--graph) <graph_name>] [-c (--config) config.yaml]`: Starts the service with a specified graph and optional configurations. If no graph is provided, the default "modern" graph will be used.
- `gs_interactive service restart [-c (--config) config.yaml]`: Restarts the service with optional configurations.
- `gs_interactive service stop`: Stops the service.
- `gs_interactive service status`: Checks the status (the entry) of the service.

### Database Commands
- `gs_interactive database -h (--help)`
- `gs_interactive database create -g (--graph) <graph_name> -c (--config) schema.yaml`: Creates a graph with the given name and schema. The graph name must be unique within GraphScope Interactive.
- `gs_interactive database remove -g (--graph) <graph_name>`: Removes the specified graph. The currently running graph must be stopped before it can be removed.
- `gs_interactive database import -g (--graph) <graph_name> -c (--config) loading.yaml`: Imports raw data for the given graph. The currently running graph must be stopped before data can be imported, as this will overwrite the existing data.

### Procedure Commands
- `gs_interactive procedure -h (--help)`
- `gs_interactive procedure compile -g (--graph) <graph_name> [-n (--name) <proc_name>] [-d (--desc) <description>] -i (--input) <input_file/folder> [--compile_only]`: Compiles (and enables by default) a given input file (or files in the input folder). If -n isn't provided, the file name will be used as the stored procedure's name. Without the -d option, an empty description will be used.
- `gs_interactive procedure enable -g (--graph) <graph_name> -c (--config) stored_procedures.yaml`: Enables the specified stored procedures.
- `gs_interactive procedure disable -g (--graph) <graph_name> [-a (--all)] -c (--config) stored_procedures.yaml`: Disables the provided (or all) stored procedures.
- `gs_interactive procedure show -g (--graph) <graph_name>`: Displays all enabled stored procedures along with their metadata, including names and descriptions.