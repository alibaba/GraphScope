# Cypher Shell

GraphScope interactive is compatible with the existing neo4j cypher ecosystem. Users can use Cypher Shell as a client to connect to the database service and submit queries for execution.

## Downloading
We currently only support Cypher Shell version 4.4.22. You can download from the [official release](https://dist.neo4j.org/cypher-shell/cypher-shell-4.4.22.zip).

## Usage

You can connect to the started GraphScope Interactive Database though the bolt connector port. The default port is 7687. 

```bash
cypher-shell -a neo4j://{ip}:7687.
```

{ip} refers to the IP address of the machine where the database service is running. If you are running cypher shell and database service on the same host, then use `localhost` is ok.

## Limitation

Currently write operations are not supported in GraphScope Interactive, so you cannot use `create` clause in cypher shell.