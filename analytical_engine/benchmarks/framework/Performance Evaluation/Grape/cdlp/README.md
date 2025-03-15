# Community Detection using Label Propagation

Community Detection using Label Propagation(CDLP) finds “communities” in the graph, i.e.,
non-overlapping densely connected clusters that are weakly
connected. [LDBC](http://www.vldb.org/pvldb/vol9/p1317-iosup.pdf) selects for community detection
the label propagation algorithm, modified slightly to be
both parallel and deterministic. 

CDLP receives a parameter limit the max rounds. It can be set by the flag: `--cdlp_mr`.

This directory includes two variants of CDLP.

- **cdlp_auto**: The auto-parallel version of CDLP that follows the PIE model proposed in the [GRAPE](https://dl.acm.org/doi/10.1145/3035918.3035942) paper. Messages are handled automatically by the execution engine.

- **cdlp**: This variant handles messages explicitly. The generated messages are sent as soon as possible, overlapping communication with evaluation during parallel computation.