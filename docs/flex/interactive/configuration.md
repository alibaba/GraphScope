# Engine Configuration

Starting your GraphScope Interactive service can be straightforward, as demonstrated in our [getting_started](./getting_started.md) guide. By default, executing the command:

```bash
bin/gs_interactive service start
```

will initialize the service with its default settings. However, GraphScope is designed to be flexible and adaptable to your specific needs. This means you can tailor the service's behavior using custom configurations.

## Customizing Your Service Configuration
To customize the service's settings, you can provide a YAML configuration file. This file allows you to specify various parameters, from directory paths to log levels, ensuring the service aligns with your requirements. To use a custom configuration, simply pass the YAML file to the command as follows:

```bash
bin/gs_interactive service start -c ./conf/engine_config.yaml
```

Note: Please be aware that you're not required to configure every option. Simply adjust the settings that are relevant to your needs. Any options left unconfigured will automatically adopt their default values, as detailed in the sections that follow.


If you already have an Interactive service running and wish to apply a new set of configurations, a simple restart with the custom configuration is required. This ensures that the service updates its settings and operates according to your newly specified preferences.

To restart the service with your custom configuration, use the following command:
```bash
bin/gs_interactive service restart -c ./conf/engine_config.yaml
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
| http_service.default_listen_address | localhost | The address for http service to bind | 0.0.2 |
| http_service.admin_port | 7777 | The port for admin service to listen on | 0.0.2 |
| http_service.query_port | 10000 | The port for query service to listen on, for stored procedure queries, user can directory submit queries to query_port without compiler involved | 0.0.2 |


