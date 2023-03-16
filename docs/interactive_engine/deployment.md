# Standalone Deployment for GIE

We have demonstrated [how to execute interactive queries](./getting_started.md) easily by installing Graphscope via `pip` on a local machine. However, in real-life applications, graphs are often too large to fit on a single machine. In such cases, Graphscope can be deployed on a cluster, such as a [self-managed k8s cluster](../deploy_graphscope_on_self_managed_k8s.md), for processing large-scale graphs. But you may wonder, "what if I only need the GIE engine and not the whole package that includes GAE and GLE?" This tutorial will walk you through the process of standalone deployment of GIE on a self-managed k8s cluster.

## Deploy GIE for Business Intelligence
[The architecture of GIE](./design_of_gie.md) reveals the potential to use diverse computing engines for various application scenarios. Presently, we will only focus on the deployment of GIE for business intelligence using the [Pegasus](../overview/glossary.md) engine, as the high-QPS scenario is still in development. [Vineyard](https://v6d.io), a distributed, in-memory, and immutable store, is primarily utilized to maintain the graph data. However, if needed, [Groot](../storage_engine/groot.md), which supports mutable and persistent storage, can also be used with alternative [deployment options](./deployment_on_groot.md) available.

### TODO
