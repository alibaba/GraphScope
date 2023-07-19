# Edge MultiSet

- SingleSrcAdjEdgeSet: Stores edges based on adj_list(fetched via storage interface) references. Src vertices and dst vertices are both from one label.
- MultiSrcAdEdgeSet: Stores edges based on adj_list(fetched via storage interface) references. Src vertices are from multiple labels, and dst vertices are both from one label.
- FlatEdgeSet: Stores the Edges in triplet.
- MultiLabelDstEdgeSet: Stores Edges with multiple destination labels, stored in a separate manner.