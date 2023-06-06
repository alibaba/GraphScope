# Neo4j Ecosystem

Neo4j is a graph database management system that utilizes a graph-based approach to store and process data. Unlike traditional relational databases that rely on tables and rows, Neo4j leverages the power of interconnected nodes and relationships, forming a highly flexible and expressive data model. GIE implements Neo4j's HTTP and TCP protocol so that the system can seamlessly interact with the Neo4j ecosystem, including development tools such as [cypher-shell] (https://dist.neo4j.org/cypher-shell/cypher-shell-4.4.19.zip) and [drivers] (https://neo4j.com/developer/language-guides/).

Next, the following content will guide you on how to start the GIE Compiler service and how to connect and send queries through the official neo4j console or drivers.

## Start Cypher Service:
1. Configure the port in `GraphScope/interactive_engine/compiler/conf/neo4j.conf`
    ```
    # Bolt connector
    dbms.connector.bolt.enabled=true
    dbms.connector.bolt.listen_address=:7687
    dbms.connector.bolt.advertised_address=:7687
    ```
2. Start the Compiler service
    ```
    cd GraphScope/interactive_engine/compiler && make run
    ```

## Connecting Cypher within Python:
1. Install the `neo4j` dependency:
    ```
    pip install neo4j
    ```
2. An example
    ```python
    from neo4j import GraphDatabase, RoutingControl

    URI = "neo4j://localhost:7687"
    AUTH = ("neo4j", "password")

    def print_top_10(driver):
        records, _, _ = driver.execute_query(
            "MATCH (n) RETURN n Limit 10",
            database_="neo4j", routing_=RoutingControl.READ,
        )
        for record in records:
            print(record["n"])


    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        print_top_10(driver)
    ```

## Connecting Cypher within Shell:
1. Download and extract `cypher-shell`
    ```bash
    wget https://dist.neo4j.org/cypher-shell/cypher-shell-4.4.19.zip
    unzip cypher-shell-4.4.19.zip && cd cypher-shell
    ```
2. Connect to the Compiler service
    ```bash
    ./cypher-shell -a neo4j://localhost:7687
    ```

    ```bash
    @neo4j> Match (n) Return n Limit 10;
    ```