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
| config | None    | The customized configuration file for engine interactive service | v0.4   |
| image-tag       | latest    | The version of the interactive you want to install | v0.5 |
| set             | None      | Specify additional properties for Interactive via the command line to override any corresponding settings in the configuration file if they are present | v0.5 |
<!-- | gremlin-port       | None    | The port of the gremlin service       | v0.3          | -->


<!-- *Note: The default value for `gremlin-port` is `None`, meaning the Gremlin service will not be initiated by default. -->

### Ports

By default, Interactive will launch the following services on these ports:

- Coordinator Service: 8080
- Interactive Meta Service: 7777
- Interactive Cypher Service: 7687
- Stored Procedure Service: 10000

You can customize these ports as needed. For example:

```bash
gsctl instance deploy --type interactive --coordinator-port 8081 --admin-port 7778 --cypher-port 7688 --storedproc-port 10001
```

<!-- ### Enabling Gremlin Service

The Gremlin service is disabled by default. To enable it, add the `--gremlin-port` option:

```bash
gsctl instance deploy --type interactive --coordinator-port 8081 --admin-port 7778 --cypher-port 7688 --storedproc-port 10001 --gremlin-port 8183
``` -->

### Service Configuration

By default, `Interactive` will initialize the service with its default settings.
However, GraphScope Interactive is designed to be flexible and adaptable to your specific needs. This means you can tailor the service's behavior using custom configurations.


#### Customizing Interactive Engine Service Configuration
To customize the service's settings, you can provide a YAML configuration file `interactive_config.yaml`. This file allows you to specify various parameters, from directory paths to log levels, ensuring the service aligns with your requirements. To use a custom configuration, simply pass the YAML file to the command as follows:

```bash
gsctl instance deploy --type interactive --config ./interactive_config.yaml
```

```{note}
Please be aware that you're not required to configure every option. Simply adjust the settings that are relevant to your needs. Any options left unconfigured will automatically adopt their default values, as detailed in the following sections.
```


##### Sample Configuration
Here's a glimpse of what a typical YAML configuration file might look like:

```yaml
log_level: INFO # default INFO, available(INFO,WARNING,ERROR,FATAL)
verbose_level: 0 # default 0, should be a int in range [0,10]. 10 will verbose all logs
compute_engine:
  thread_num_per_worker: 1  # the number of threads for each worker, default 1
compiler:
  planner:
    is_on: true
    opt: RBO
    rules:
      - FilterMatchRule
      - FilterIntoJoinRule
      - NotExistToAntiJoinRule
  query_timeout: 20000  # query timeout in milliseconds, default 20000
```

#### Sharded Service

The core query engine of Interactive is developed using [hiactor](https://github.com/alibaba/hiactor) which is based on [Seastar](https://github.com/scylladb/seastar). Seastar operates on a Share-nothing SMP architecture, where each core functions autonomously, without sharing memory, data structures, or CPU resources. Each Seastar core is commonly referred to as a shard.

Leveraging the future-promise API and a Cooperative micro-task scheduler, the sharded service significantly boosts performance and throughput. However, this setup can also lead to potential issues: an incoming request might experience delays even if some shards are idle, due to the shard scheduling algorithm potentially routing it to a busy shard. This can be problematic in Interactive, which typically hosts two services—`QueryService` and `AdminService`. Crucially, `AdminService` must remain responsive even when `QueryService` is under heavy load.

As discussed in [discussion-4409](https://github.com/alibaba/GraphScope/discussions/4409), one potential solution is to allocate different shards for handling distinct requests. This approach presents three scenarios:

- **Routine Scenario**: Here, users may execute both complex and simple queries, thus dedicating a shard exclusively for admin requests. However, since this shard won’t process queries, overall system performance may decline.
  
- **Performance-Critical Scenario**: In this scenario, users aim for peak performance from Interactive. All shards are used to process query requests, with admin requests being handled concurrently by them. Consequently, there may be instances of request delays.

By default, Interactive is configured for routine with the following:

```yaml
http_service:
  sharding_mode: exclusive # In exclusive mode, a shard is exclusively reserved for admin requests. In cooperative mode, both query and admin requests can be processed by any shard.
```

By changing to `sharding_mode: cooperative`, you can fully utilize all the computational power for the QueryService.


##### Available Configurations

In this following table, we use the `.` notation to represent the hierarchy within the `YAML` structure.


| PropertyName       | Default   | Meaning |  Since Version |
| --------           | --------  | -------- |-----------  |
| log_level     |  INFO   | The level of database log, INFO/WARNING/ERROR/FATAL | 0.0.1 |
| verbose_level     |  0   | The verbose level of database log, should be a int | 0.0.3 |
| compute_engine.thread_num_per_worker | 4 | The number of threads will be used to process the queries. Increase the number can benefit the query throughput | 0.0.1 |
| compute_engine.wal_uri    | file://{GRAPH_DATA_DIR}/wal | The location where Interactive will store and access WALs. `GRAPH_DATA_DIR` is a placeholder that will be populated by Interactive. | 0.5 |
| compiler.planner.is_on | true | Determines if query optimization is enabled for compiling Cypher queries  | 0.0.1 |
| compiler.planner.opt | RBO | Specifies the optimizer to be used for query optimization. Currently, only the Rule-Based Optimizer (RBO) is supported | 0.0.1 |
| compiler.planner.rules.FilterMatchRule | N/A | An optimization rule that pushes filter (`Where`) conditions into the `Match` clause | 0.0.1 |
| compiler.planner.rules.FilterIntoJoinRule | N/A | A native Calcite optimization rule that pushes filter conditions to the Join participants before performing the join | 0.0.1 |
| compiler.planner.rules.NotMatchToAntiJoinRule | N/A | An optimization rule that transforms a "not exist" pattern into an anti-join operation  | 0.0.1 |
| compiler.query_timeout  | 3000000   ｜ The maximum time for compiler to wait engine's reply, in `ms`  | 0.0.3 | 
| http_service.sharding_mode | exclusive | The sharding mode for http service, In exclusive mode, one shard is reserved exclusively for service admin request. In cooperative, both query request and admin request could be served by any shard. | 0.5 |
| http_service.max_content_length | 1GB | The maximum length of a http request that admin http service could handle | 0.5 |
| storage.string_default_max_length | 256 | The default maximum size for a string field | 0.5 |


#### TODOs

We currently only allow service configuration during instance deployment. In the near future, we will support:

- Graph-level configurations
- Modifying service configurations