# GraphScope Interactive

GraphScope Interactive is a specialized construction of [GraphScope Flex](https://github.com/alibaba/GraphScope/tree/main/flex), designed to handle concurrent graph queries at an impressive speed. Its primary goal is to process as many queries as possible within a given timeframe, emphasizing a high query throughput rate.
For the full documentation of GraphScope Interactive, please refer to [GraphScope-Interactive](https://graphscope.io/docs/interactive_engine/graphscope_interactive).

## Minimal tutorial

In this minimal tutorial, we will show you how to run graph service on builtin modern graph.

### Preparation

Set `location` to `/home/graphscope/default_graph`.

### init database

```bash
./bin/gs_interactive init -c ./conf/interactive.yaml
```

### Start service

```bash
./bin/gs_interactive service start
```

### Stop service
```bash
./bin/gs_interactive service stop
```

### Restart service 
```bash
./bin/gs_interactive service restart
```

### Get service status
```bash
./bin/gs_interactive service status
```

### Compile stored procedure
```bash
./bin/gs_interactive procedure compile -g modern -i ./examples/modern_graph/count_vertex_num.cypher
```

### Disable stored procedure
```bash
./bin/gs_interactive procedure disable -g modern -n count_vertex_num
```

### Enable stored procedure
```bash
./bin/gs_interactive procedure enable -g modern -n count_vertex_num
```

### Use user defined graph
```bash
./bin/gs_interactive service stop
./bin/gs_interactive database remove -g modern
./bin/gs_interactive database create -g test -c ./examples/modern_graph/modern_graph.yaml
./bin/gs_interactive database import -g test -c ./examples/modern_graph/bulk_load.yaml
./bin/gs_interactive service start -g test
```