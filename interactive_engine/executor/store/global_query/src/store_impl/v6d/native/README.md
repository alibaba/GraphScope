v6d ffi
=======

Usage of `vineyard-htap-loader`
-------------------------------

```bash
$ ./vineyard_htap_loader
usage: ./vineyard_htap_loader <e_label_num> <efiles...> <v_label_num> <vfiles...> [directed] [generate_eid]

   or: ./vineyard_htap_loader --config <config.json>
```

1. Loading from command line arguments:

```bash
$ ./vineyard_htap_loader 2 "$GSTEST/modern_graph/knows.csv#header_row=true&src_label=person&dst_label=person&label=knows&delimiter=|" \
                           "$GSTEST/modern_graph/created.csv#header_row=true&src_label=person&dst_label=software&label=created&delimiter=|" \
                         2 "$GSTEST/modern_graph/person.csv#header_row=true&label=person&delimiter=|" \
                           "$GSTEST/modern_graph/software.csv#header_row=true&label=software&delimiter=|"
```

2. Loading using a config file:

```bash
$ ./vineyard_htap_loader --config ../modern_graph.json
```

The config file is a JSON file with the following structure:

```
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
    "generate_eid": 1 # 0 or 1
}
```
