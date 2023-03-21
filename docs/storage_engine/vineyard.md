In-memory immutable graphs on Vineyard
======================================

[Vineyard][1] is a distributed immutable in-memory data manager that is used as
the storage backend for immutable graphs in GraphScope. Vineyard provides zero-copy
data sharing using memory mapping, and different compute engines in GraphScope
can run on the same vineyard cluster to efficiently share the graph data.

Graphs in Vineyard
------------------

Vineyard supports immutable property graphs and abstracts it as the [`vineyard::ArrowFragment`][2]
class, which consists of a CSR for edges and uses tables to store edge and vertex properties.
Upon the `ArrowFragment`, vineyard abstracts distributed graph as `vineyard::ArrowFragmentGroup`
which consists of a set of fragments that spread across the cluster.

Loading Graphs to Vineyard
--------------------------

Vineyard can be deployed as a standalone service or launched along with GraphScope.
A command-line tool `vineyard-graph-loader` is provided to load fragments into
vineyard. It first accepts an optional argument `--socket <vineyard-ipc-socket>`,
which points the IPC docket that the loader will connect to. If omitted, the value
will be resolved from the environment variable `VINEYARD_IPC_SOCKET`. It takes
either a set of command-line arguments or a JSON file as configuration.

```bash
$ vineyard-graph-loader --help
Usage: loading vertices and edges as vineyard graph.

    -     ./vineyard-graph-loader [--socket <vineyard-ipc-socket>] \
                                   <e_label_num> <efiles...> <v_label_num> <vfiles...> \
                                   [directed] [generate_eid] [retain_oid] [string_oid]

    - or: ./vineyard-graph-loader [--socket <vineyard-ipc-socket>] --config <config.json>

          The config is a json file and should look like

          {
              "vertices": [
                  {
                      "data_path": "....",
                      "label": "...",
                      "options": "...."
                  },
                  ...
              ],
              "edges": [
                  {
                      "data_path": "",
                      "label": "",
                      "src_label": "",
                      "dst_label": "",
                      "options": ""
                  },
                  ...
              ],
              "directed": 1, # 0 or 1
              "generate_eid": 1, # 0 or 1
              "retain_oid": 1, # 0 or 1
              "string_oid": 0, # 0 or 1
              "local_vertex_map": 0 # 0 or 1
          }%
```

Some of the options that specify how the graph will be constructed are:

- `directed`: whether the graph is a directed graph or undirected graph.
- `generate_eid`: whether to generate a globally unique edge id for each edge.
- `retain_oid`: whether to retain the original vertex id into the final vertex's
  property table.
- `string_oid`: whether the vertex id is a string.
- `local_vertex_map`: whether to use local vertex map during the graph construction,
  which is usually used for optimizing the memory usage.

Using the `vineyard-graph-loader` to load the modern graph can be done in the following ways:

- using command line arguments

  The `vineyard-graph-loader` accepts a sequence of command line arguments to
  specify the edge files and vertex files, e.g.,

  ```bash
  $ ./vineyard-graph-loader 2 "modern_graph/knows.csv#header_row=true&src_label=person&dst_label=person&label=knows&delimiter=|" \
                              "modern_graph/created.csv#header_row=true&src_label=person&dst_label=software&label=created&delimiter=|" \
                            2 "modern_graph/person.csv#header_row=true&label=person&delimiter=|" \
                              "modern_graph/software.csv#header_row=true&label=software&delimiter=|"
  ```

- using a JSON configuration file

  ```bash
  $ ./vineyard-graph-loader --config config.json
  ```

  The JSON configuration file could be (using the "modern graph" as an example):

  ```json
     {
         "vertices": [
             {
                 "data_path": "modern_graph/person.csv",
                 "label": "person",
                 "options": "header_row=true&delimiter=|"
             },
             {
                 "data_path": "modern_graph/software.csv",
                 "label": "software",
                 "options": "header_row=true&delimiter=|"
             }
         ],
         "edges": [
             {
                 "data_path": "modern_graph/knows.csv",
                 "label": "knows",
                 "src_label": "person",
                 "dst_label": "person",
                 "options": "header_row=true&delimiter=|"
             },
             {
                 "data_path": "modern_graph/created.csv",
                 "label": "created",
                 "src_label": "person",
                 "dst_label": "software",
                 "options": "header_row=true&delimiter=|"
             }
         ],
         "directed": 1,
         "generate_eid": 1,
         "string_oid": 0,
         "local_vertex_map": 0
     }
  ```

Using Loaded Graphs
-------------------

After being loaded into vineyard, the loaded fragment can be accessed using
vineyard's IPCClient:

```cpp
void WriteOut(vineyard::Client& client, const grape::CommSpec& comm_spec,
              vineyard::ObjectID fragment_group_id) {
  LOG(INFO) << "Loaded graph to vineyard: " << fragment_group_id;
  std::shared_ptr<vineyard::ArrowFragmentGroup> fg =
      std::dynamic_pointer_cast<vineyard::ArrowFragmentGroup>(
          client.GetObject(fragment_group_id));

  for (const auto& pair : fg->Fragments()) {
    LOG(INFO) << "[frag-" << pair.first << "]: " << pair.second;
  }

  // NB: only retrieve local fragments.
  auto locations = fg->FragmentLocations();
  for (const auto& pair : fg->Fragments()) {
    if (locations.at(pair.first) != client.instance_id()) {
      continue;
    }
    auto frag_id = pair.second;
    Traverse(client, frag_id);
  }
}
```

The local fragment can be traversed using the `vineyard::ArrowFragment`'s API:

```cpp
void Traverse(vineyard::Client& client, vineyard::ObjectID frag_id) {
  auto frag = std::dynamic_pointer_cast<GraphType>(client.GetObject(frag_id));
  LOG(INFO) << "graph total node number: " << frag->GetTotalNodesNum();
  LOG(INFO) << "fragment edge number: " << frag->GetEdgeNum();
  LOG(INFO) << "fragment in edge number: " << frag->GetInEdgeNum();
  LOG(INFO) << "fragment out edge number: " << frag->GetOutEdgeNum();

  for (LabelType vlabel = 0; vlabel < frag->vertex_label_num(); ++vlabel) {
    LOG(INFO) << "vertex table: " << vlabel << " -> "
              << frag->vertex_data_table(vlabel)->schema()->ToString();
  }
  for (LabelType elabel = 0; elabel < frag->edge_label_num(); ++elabel) {
    LOG(INFO) << "edge table: " << elabel << " -> "
              << frag->edge_data_table(elabel)->schema()->ToString();
  }

  LOG(INFO) << "--------------- consolidate vertex/edge table columns ...";

  if (frag->vertex_data_table(0)->columns().size() >= 4) {
    for (LabelType vlabel = 0; vlabel < frag->vertex_label_num(); ++vlabel) {
      LOG(INFO) << "vertex table: " << vlabel << " -> "
                << frag->vertex_data_table(vlabel)->schema()->ToString();
    }
  }

  if (frag->edge_data_table(0)->columns().size() >= 4) {
    for (LabelType elabel = 0; elabel < frag->edge_label_num(); ++elabel) {
      LOG(INFO) << "edge table: " << elabel << " -> "
                << frag->edge_data_table(elabel)->schema()->ToString();
    }
  }
}
```

[1]: https://v6d.io/
[2]: https://github.com/v6d-io/v6d/blob/main/modules/graph/fragment/arrow_fragment.vineyard-mod
