def add_edges(G, lines):
    edges = []
    for line in lines:
        if not len(line):
            continue
        # split line, should have 2 or more
        s = line.strip().split("\t")
        if len(s) < 2:
            continue
        u = s.pop(0)
        v = s.pop(0)
        d = s.pop(0)
        #try:
        u = int(u)
        v = int(v)
        data = int(d)
        #except Exception as e:
        #    raise TypeError(
        #        "Failed to convert nodes %s,%s to type %s." % (u, v, int)
        #    ) from e

        edgedata = {"weight": data}
        edges.append((u, v, edgedata))
    G.add_edges_from(edges)