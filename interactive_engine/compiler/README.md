# GOpt: A Modular Graph-Native Query Optimization Framework

## Introducing GOpt

GOpt is a modular, graph-native query optimization framework designed to accelerate graph query execution on industrial-scale graph systems. It excels in handling complex graph patterns that combine graph pattern matching with relational operations on large graphs. GOpt is not aware of the underlying storage data and focuses solely on computation on top of the data, which makes it easy and fast to be integrated into other graph or relational databases.

### Core Features

1. **Multiple Query Languages Support**: GOpt supports standard [Gremlin](https://tinkerpop.apache.org/gremlin.html) and [Cypher](https://neo4j.com/docs/cypher-manual/current/introduction/) languages, with upcoming [GQL](https://www.gqlstandards.org/) support.
2. **Lightweight, Serverless Integration**: GOpt provides modular interfaces to integrate with various platforms. It has already been integrated into GraphScope and Neo4j.
3. **Advanced Graph-native Optimization**: GOpt introduces a comprehensive set of heuristic rules, an automatic type inference algorithm, and advanced cost-based optimization techniques.

:::{figure-md}

<img src="../../docs/images/gopt/system_overview.png"
alt="system_overview"
width="80%">

GOpt System Overview
:::

### Why GOpt

1. **High Performance**

   GOpt is designed and implemented based on years of academic research, with key techniques published in prestigious systems conferences. Our experiments, as documented in our [papers](https://arxiv.org/abs/2401.17786), demonstrate that GOpt outperforms most graph and relational databases in both standard ([LDBC](https://ldbcouncil.org/)) and real-world (Alibaba) graph workloads.
2. **User-Friendly Interface**

   GOpt offers different layers of SDK tailored to various user requirements. It provides Cypher and Gremlin language support to lower the barrier of entry. User-provided Cypher or Gremlin queries can be more flexible and ambiguous, with GOpt automatically validating and completing the query information based on property graph modeling. Additionally, it provides lower-level APIs for developers who require deeper integration.
3. **Seamless Integration**

   GOpt is lightweight and serverless, facilitating seamless integration into other databases through a small-sized JAR file deployment. Built on the Calcite framework, GOpt leverages Calcite's extensive range of adapters, simplifying the integration with various data formats. This advantage allows GOpt to seamlessly integrate with mainstream relational databases that has been powered by Calcite. Additionally, GOpt is equipped with graph-native algorithms, enhancing its compatibility with graph-native database APIs.

For more details, please refer to the [GOpt Documentation](https://graphscope.io/docs/latest/interactive_engine/gopt).
