# Configuration
## Configurable Items for Deploying Interactive with gsctl

When deploying Interactive using `gsctl`, various items can be configured. For a given configurable item named `item-name`, you can set its value as follows:

```bash
gsctl instance deploy --type interactive [--item-name=value]
```

Below is a list of all configurable items:

| Item Name         | Default | Description               | Since Version |
|-------------------|---------|---------------------------|---------------|
| coordinator-port  | 8080    | The port of the coordinator service  | v0.3          |
| admin-port       | 7777    | The port of the interactive admin service       | v0.3          |
| storedproc-port   | 10000    | The port of the interactive stored procedure service      | v0.3          |
| cypher-port       | 7687    | The port of the cypher service       | v0.3          |
| gremlin-port       | None    | The port of the gremlin service       | v0.3          |


*Note: The default value for `gremlin-port` is `None`, meaning the Gremlin service will not be initiated by default.

### Default Ports

By default, Interactive will launch the following services on these ports:

- Coordinator Service: 8080
- Interactive Meta Service: 7777
- Interactive Cypher Service: 7687
- Stored Procedure Service: 10000

You can customize these ports as needed. For example:

```bash
gsctl instance deploy --type interactive --coordinator-port 8081 --admin-port 7778 --cypher-port 7688 --storedproc-port 10001
```

### Enabling Gremlin Service

The Gremlin service is disabled by default. To enable it, add the `--gremlin-port` option:

```bash
gsctl instance deploy --type interactive --coordinator-port 8081 --admin-port 7778 --cypher-port 7688 --storedproc-port 10001 --gremlin-port 8183
```


<!-- Those content are commented but not deleted, since we will support those configurations later.
> TODO: Currently `gsctl` doesn't support the following command!

Starting your GraphScope Interactive service can be straightforward, as demonstrated in our [getting_started](./getting_started.md) guide. By default, executing the command:

```bash
gsctl use GRAPH <name>
```

will initialize the service with its default settings. However, GraphScope is designed to be flexible and adaptable to your specific needs. This means you can tailor the service's behavior using custom configurations.

## Customizing Your Service Configuration
To customize the service's settings, you can provide a YAML configuration file. This file allows you to specify various parameters, from directory paths to log levels, ensuring the service aligns with your requirements. To use a custom configuration, simply pass the YAML file to the command as follows:

```bash
gsctl use GRAPH <name> -c ./engine_config.yaml
```

Note: Please be aware that you're not required to configure every option. Simply adjust the settings that are relevant to your needs. Any options left unconfigured will automatically adopt their default values, as detailed in the sections that follow.


If you already have an Interactive service running and wish to apply a new set of configurations, a simple restart with the custom configuration is required. This ensures that the service updates its settings and operates according to your newly specified preferences.

To restart the service with your custom configuration, use the following command:
```bash
gsctl service restart -c ./conf/engine_config.yaml
```
Remember, any changes made in the configuration file will only take effect after the service has been restarted with the updated file.



## Sample Configuration
Here's a glimpse of what a typical YAML configuration file might look like:

```yaml
log_level: INFO # default INFO
compute_engine:
  thread_num_per_worker: 1  # the number of shared workers, default 1
compiler:
  planner:
  is_on: true
  opt: RBO
  rules:
    - FilterMatchRule
    - FilterIntoJoinRule
    - NotExistToAntiJoinRule
  query_timeout: 20000  # query timeout in milliseconds, default 20000
  endpoint:
    default_listen_address: localhost
    bolt_connector: # cypher query endpoint 
      disabled: false # disable cypher endpoint or not.
      port: 7687
    gremlin_connector: # gremlin query endpoint 
      disabled: false # disable gremlin endpoint or not.
      port: 8182
http_service:
  default_listen_address: localhost
  admin_port: 7777
  query_port: 10000
```


## Available Configurations
For configurations associated with the root directory, we do not accept relative paths to ensure consistency.

### Service configurations

In this following table, we use the `.` notation to represent the hierarchy within the `YAML` structure.


| PropertyName       | Default   | Meaning |  Since Version |
| --------           | --------  | -------- |-----------  |
| log_level     |  INFO   | The level of database log, INFO/DEBUG/ERROR | 0.0.1 |
|default_graph  | modern | The name of default graph on which to start the graph service. | 0.0.1 |
| compute_engine.thread_num_per_worker | 1 | The number of threads will be used to process the queries. Increase the number can benefit the query throughput | 0.0.1 |
| compiler.planner.is_on | true | Determines if query optimization is enabled for compiling Cypher queries  | 0.0.1 |
| compiler.planner.opt | RBO | Specifies the optimizer to be used for query optimization. Currently, only the Rule-Based Optimizer (RBO) is supported | 0.0.1 |
| compiler.planner.rules.FilterMatchRule | N/A | An optimization rule that pushes filter (`Where`) conditions into the `Match` clause | 0.0.1 |
| compiler.planner.rules.FilterIntoJoinRule | N/A | A native Calcite optimization rule that pushes filter conditions to the Join participants before performing the join | 0.0.1 |
| compiler.planner.rules.NotMatchToAntiJoinRule | N/A | An optimization rule that transforms a "not exist" pattern into an anti-join operation  | 0.0.1 |
| compiler.endpoint.default_listen_address | localhost | The address for compiler endpoint to bind | 0.0.3 |
| compiler.endpoint.bolt_connector.disabled | false | Whether to disable the cypher endpoint| 0.0.3 |
| compiler.endpoint.bolt_connector.port | 7687 | The port for compiler's cypher endpoint.| 0.0.3 |
| compiler.endpoint.gremlin_connector.disabled | true | Whether to disable the gremlin endpoint| 0.0.3 |
| compiler.endpoint.gremlin_connector.port | 8182 | The port for compiler's cypher endpoint.| 0.0.3 |
| http_service.default_listen_address | localhost | The address for http service to bind | 0.0.2 |
| http_service.admin_port | 7777 | The port for admin service to listen on | 0.0.2 |
| http_service.query_port | 10000 | The port for query service to listen on, for stored procedure queries, user can directory submit queries to query_port without compiler involved | 0.0.2 | -->


