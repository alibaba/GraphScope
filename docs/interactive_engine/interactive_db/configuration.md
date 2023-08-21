# Configuration

To customize the settings of GraphScope Interactive, adjust the `gs_interactive.yaml` file. Please be aware that any changes to the configurations will only take effect after the service is [restarted](./getting_started.md).


## Available Configurations
For configurations associated with the root directory, we do not accept relative paths to ensure consistency.

### Service configurations

In this following table, we use the `.` notation to represent the hierarchy within the `YAML` structure.


| PropertyName       | Default   | Meaning |  Since Version |
| --------           | --------  | -------- |-----------  |
| version            |  latest   | The version of product version     |    0.0.1     |
| directories.workspace | /home/graphscope/interactive | The workspace of interactive docker container | 0.0.1 |
| directories.subdirs.data | data | The directory to store the graph indices and relate staffs | 0.0.1 |
| directories.subdirs.conf | conf | The directory to store the configuration of the Interactive database. | 0.0.1 |
| directories.subdirs.logs | logs | The directory to store the running log of database. | 0.0.1 |
| log_level     |  INFO   | The level of database log, INFO/DEBUG/ERROR | 0.0.1 |
|default_graph  | modern | The name of default graph on which to start the graph service. | 0.0.1 |
| compute_engine.type | hiactor | The backend query engine of databse, currenly only hiactor is supported. | 0.0.1 |
| compute_engine.hosts |localhost:10000 | The ip:port for the graph server to bind. | 0.0.1 |
| compute_engine.shard_num | 1 | The number of threads will be used to process the queries | 0.0.1 |
| compiler.planner | {"isOn":true,"opt":"RBO","rules":["FilterMatchRule"]} | The configuration of compiler planner | 0.0.1 |
| compiler.endpoint.bolt_connector.enabled | true | Turn on bolt service or not. | 0.0.1 |
| compiler.endpoint.bolt_connector.port | 7687 | The port bolt connector will listen on | 0.0.1 |
     

## A sample file of configurations
```yaml
---
version: 0.0.1
directories:
  workspace: /home/graphscope/interactive/
  subdirs:
    data: data  # by default data, relative to ${workspace}
    conf: conf # by default conf, relative to ${workspace}
    logs: logs  # by default logs, relative to ${workspace}
log_level: INFO # default INFO
default_graph: modern  # configure the graph to be loaded while starting the service, if graph name not specified
  # may include other configuration items of other engines
compute_engine:
  type: hiactor
  hosts:
    - localhost:10000  # currently only one host can be specified
  shared_num: 1  # the number of shared workers, default 1
compiler:
  planner: {"isOn":true,"opt":"RBO","rules":["FilterMatchRule"]} # Confirm这个配置
  endpoint:
    default_listen_address: localhost  # default localhost
    bolt_connector:   # for cypher, there may be other connectors, such as bolt_connector, https_connector
      enabled: true   # default false
      port: 7687
```

