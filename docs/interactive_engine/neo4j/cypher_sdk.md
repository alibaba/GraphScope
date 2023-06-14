# GIE for Cypher
This document will provide you with step-by-step guidance on how to connect your Cypher applications to the GIE's
FrontEnd service, which offers functionalities similar to the official Tinkerpop service.

Your first step is to obtain the Bolt Connector of GIE Frontend service:
- Follow the [instruction](./dev_and_test.md#manually-start-the-gie-services) while starting GIE on a local machine.

## Connecting via Python Driver

GIE makes it easy to connect to a loaded graph with Neo4j's [Python Driver]](https://pypi.org/project/neo4j/).

You first install the dependency:
```bash
pip3 install neo4j
```

Then connect to the service and run queries:

    ```python
    from neo4j import GraphDatabase, RoutingControl

    URI = "neo4j://localhost:7687"  # the bolt connector you've obtained
    AUTH = ("", "")  # We have not implemented authentication yet

    def print_top_10(driver):
        records, _, _ = driver.execute_query(
            "MATCH (n) RETURN n Limit 10",
            # For now, GIE only supports one graph, and its name is not important.
            database_="", routing_=RoutingControl.READ,
        )
        for record in records:
            print(record["n"])


    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        print_top_10(driver)
    ```

## Connecting via Cypher-Shell
1. Download and extract `cypher-shell`
    ```bash
    wget https://dist.neo4j.org/cypher-shell/cypher-shell-4.4.19.zip
    unzip cypher-shell-4.4.19.zip && cd cypher-shell
    ```
2. Connect to the Bolt Connector
    ```bash
    ./cypher-shell -a neo4j://localhost:7687
    ```
3. Run Queries
    ```bash
    @neo4j> Match (n) Return n Limit 10;
    ```
