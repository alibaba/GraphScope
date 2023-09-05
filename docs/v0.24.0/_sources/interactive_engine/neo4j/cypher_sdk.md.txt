# GIE for Cypher
We have implemented Neo4j's [Bolt](https://neo4j.com/docs/bolt/current/bolt/) protocol for you to connect your Neo4j applications to the GIE's Frontend service.

Your first step is to obtain the Cypher endpoint for the [Bolt](https://neo4j.com/docs/bolt/current/bolt/) connector
- Follow the [instruction](../deployment.md) while deploying GIE in a K8s cluster,
- Follow the [instruction](../dev_and_test.md) while starting GIE on a local machine.

## Connecting via Python Driver

GIE makes it easy to connect to a loaded graph with Neo4j's [Python Driver]](https://pypi.org/project/neo4j/).

You first install the dependency:
```bash
pip3 install neo4j
```

Then connect to the service and run queries:

```Python
from neo4j import GraphDatabase, RoutingControl

URI = "neo4j://localhost:7687"  # neo4j:// + Cypher endpoint you've obtained
AUTH = ("", "")  # We have not implemented authentication yet

def print_top_10(driver):
    records, _, _ = driver.execute_query(
        "MATCH (n) RETURN n Limit 10",
        routing_=RoutingControl.READ,
    )
    for record in records:
        print(record["n"])


with GraphDatabase.driver(URI, auth=AUTH) as driver:
    print_top_10(driver)
```

````{hint}
A simpler option is to use the `interactive` object for submitting Cypher queries through
[GraphScope's python SDK](../getting_started.md), which is a wrapper that encompasses Neo4j's
Python Driver and will automatically acquire the endpoint.
````


## Connecting via Cypher-Shell
1. Download and extract `cypher-shell`
    ```bash
    wget https://dist.neo4j.org/cypher-shell/cypher-shell-4.4.19.zip
    unzip cypher-shell-4.4.19.zip && cd cypher-shell
    ```
2. Connect to the Bolt connector with the Cypher endpoint you've obtained
    ```bash
    ./cypher-shell -a neo4j://localhost:7687
    ```
3. Run Queries
    ```bash
    @neo4j> Match (n) Return n Limit 10;
    ```
