GraphScope Interactive Engine
============================

GraphScope Interactive Engine (GIE) is a distributed system designed specifically to make it easy for a variety of users to analyze large and complex graph structures in an *exploratory* manner.  It exploits `Gremlin <http://tinkerpop.apache.org/>`_ to provide a high-level language for interactive graph queries, and provides automatic parallel execution.


Apache TinkerPop
----------------

`Apache TinkerPop <http://tinkerpop.apache.org/>`_ is an open framework for developing interactive graph applications using the Gremlin query language.  GIE implements TinkerPop's `Gremlin Server <https://tinkerpop.apache.org/docs/current/reference/#gremlin-server>`_ interface so that the system can seamlessly interact with the TinkerPop ecosystem, including development tools such as `Gremlin Console <https://tinkerpop.apache.org/docs/current/reference/#gremlin-console>`_ and language wrappers such as Java and Python.


Connecting Gremlin within Python
--------------------------------

GraphScope makes it easy to connect to a loaded graph within Python as shown below:

.. code:: python

    import graphscope
    from graphscope.dataset import load_ldbc

    # create a new session, load LDBC graph (as an example),
    # and get the Gremlin entry point
    sess = graphscope.session(num_workers=2)
    graph = load_ldbc(sess, prefix='/path/to/ldbc_sample')
    interactive = sess.gremlin(graph)

    # check the total node_num and edge_num using Gremlin
    node_num = interactive.execute('g.V().count()').one()
    edge_num = interactive.execute("g.E().count()").one()

In fact, the ``interactive`` object is an instance of the ``InteractiveQuery`` Python class, which is a simple wrapper around `Gremlin-Python <https://pypi.org/project/gremlinpython/>`_, which implements Gremlin within the Python language.

Each graph loaded in GraphScope is associated with a Gremlin endpoint (or URL) for a client to connect remotely to, which can be obtained as follows:

.. code:: python

    print(interactive.graph_url)

You should see the following output:

.. code:: python

    ws://your-endpoint:your-ip/gremlin

With that information, we can directly query a graph using Gremlin-Python as described at `here <https://tinkerpop.apache.org/docs/current/reference/#gremlin-python>`_.


Connecting Gremlin within Java
------------------------------

See `Gremlin-Java <https://tinkerpop.apache.org/docs/current/reference/#gremlin-java>`_ for connecting Gremlin within the Java language.


Gremlin Console
---------------

The `Gremlin Console <https://tinkerpop.apache.org/docs/current/tutorials/the-gremlin-console/>`_ allows you to experiment with GraphScope graphs and queries in an interactive environment.  With the same Gremlin endpoint (or URL), we can set up the Gremlin console to connect to a (remote) graph as the following:

1.Install Java [8, 12) required by Gremlin Console.

2.Download the appropriate version of the Gremlin console from `Apache TinkerPop <https://tinkerpop.apache.org/downloads.html>`_.

.. code:: bash

    wget https://archive.apache.org/dist/tinkerpop/3.4.8/apache-tinkerpop-gremlin-console-3.4.8-bin.zip

3.Unzip the downloaded file.

.. code:: bash

    unzip apache-tinkerpop-gremlin-console-3.4.8-bin.zip

4.Change directories into the unzipped directory.

.. code:: bash

    cd apache-tinkerpop-gremlin-console-3.4.8

5.In the `conf` subdirectory, create a file named `graphscope-remote.yaml` with the following text. Replace *your-endpoint* and *your-port* with the hostname (or IP address) and port of your GraphScope session, respectively.

.. code::

    hosts: [your-endpoint]
    port: your-port
    serializer: { className: org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0, config: { serializeResultToString: true }}

6.Enter the following command to start the Gremlin Console.

.. code:: bash

    bin/gremlin.sh

7.At the `gremlin>` prompt, enter the following to connect to the GraphScope session and switch to remote mode so that all subsequent Gremlin queries will be sent to the remote connection automatically.

.. code:: bash

    :remote connect tinkerpop.server conf/graphscope-remote.yaml
    :remote console

8.Now you can start sending Gremlin queries (such as ``g.V().limit(1)``).  When you are finished, enter the following to exit the Gremlin Console.

.. code:: bash

    :exit


Programming with Gremlin--101
-----------------------------

GIE is designed to faithfully preserve the programming model of Gremlin, and as a result it can be used to scale existing Gremlin applications to large compute clusters with minimum modification.  In this section, we provide a high-level view of the programming model, highlighting the key concepts including the data model and query language.  For a complete reference on Gremlin, see `TinkerPop reference <https://tinkerpop.apache.org/docs/current/reference/>`_.

Data model
~~~~~~~~~~

Gremlin enables users to define ad-hoc traversals on property graphs. A property graph is a directed graph in which vertices and edges can have a set of properties. Every entity (vertex or edge) is identified by a unique identifier (``ID``), and has a (``label``) indicating its type or role. Each property is a key-value pair with combination of entity ``ID`` and property name as the key.

.. image:: images/property_graph.png
    :width: 400
    :align: center
    :alt: An example e-commerce property graph.

The above figure shows an example property graph. It contains ``user``, ``product``, and ``address`` vertices connected by ``order``, ``deliver``, ``belongs_to``, and ``home_of`` edges. A path following vertices 1-->2-->3, shown as the dotted line, indicates that a buyer "Tom" ordered a product "gift" offered by a seller "Jack", with a price of "$99".

Query language
~~~~~~~~~~~~~~

In a Gremlin traversal, a set of *traversers* walk a graph according to particular user-provided instructions, and the result of the traversal is the collection of all halted traversers. A traverser is the basic unit of data processed by a Gremlin engine. Each traverser maintains a location that is a reference to the current vertex, edge or property being visited, and (optionally) the path history with application state.

The flexibility of Gremlin mainly stems from *nested traversal*, which allows a traversal to be embedded within another operator, and used as a function to be invoked by the enclosing operator for processing input. The role and signature of the function are determined by the type of the enclosing operator.

For example, a nested traversal within the ``where`` operator acts as a predicate function for conditional filters, while that within the ``select`` or ``order`` operator maps each traverser to the output or ordering key for sorting the output, respectively.

Nested traversal is also critical to the support for loops, which are expressed using a pair of the ``repeat`` and ``until/times`` operators. A nested traversal within the ``repeat`` operator will be looped over until the given break predicate is satisfied. The predicate (or termination condition) is defined within the ``until`` operator, applied to each output traverser separately from each iteration. The ``times`` operator can also terminate a loop after a fix number of ``k`` iterations.

An example
~~~~~~~~~~

Below shows a Gremlin query for cycle detection, which tries to find cyclic paths of length ``k`` starting from a given account.

.. code:: java

    g.V('account').has('id','2').as('s')
     .repeat(out('transfer').simplePath())
     .times(k-1)
     .where(out('transfer').eq('s'))
     .path().limit(1)

First, the source operator ``V`` (with the ``has`` filter) returns all the ``account`` vertices with an identifier of ``2``. The ``as`` operator is a *modulator* that does not change the input collection of traversers but introduces a name (``s`` in this case) for later references. Second, it traverses the outgoing ``transfer`` edges for exact ``k-1`` times, skipping any repeated vertices (by the ``simplePath`` operator). Third, the ``where`` operator checks if the starting vertex ``s`` can be reached by one more step, that is, whether a cycle of length ``k`` is formed. Finally, for qualifying traversers, the ``path`` operator returns the full path information. The ``limit`` operator at the end indicates only one such result is needed.


Compatibility with TinkerPop
----------------------------

GIE supports the property graph model and Gremlin traversal language defined by Apache TinkerPop, and provides a Gremlin *Websockets* server that supports TinkerPop version 3.3 and 3.4.  In this section, we provide an overview of the key differences between our implementation of Gremlin and the Apache TinkerPop specification.

Property graph constraints
~~~~~~~~~~~~~~~~~~~~~~~~~~

The current release (MaxGraph) leverages `Vineyard <https://github.com/alibaba/libvineyard>`_ to supply an in-memory store for *immutable* graph data that can be partitioned across multiple servers.  By design, it introduces the following constraints:

- Each graph has a schema comprised of the edge labels, property keys, and vertex labels used therein.

- Each vertex type or label has a primary key (property) defined by user.  The system will automatically generate a ``String``-typed unique identifier for each vertex and edge, encoding both the label information as well as user-defined primary keys (for vertex).

- Each vertex or edge property can be of the following data types: ``int``, ``long``, ``float``, ``double``, ``String``, ``List<int>``, ``List<long>``, and ``List<String>``.

Unsupported features
~~~~~~~~~~~~~~~~~~~~

Because of the distributed nature of the system, the following features are not supported in this release:

- Graph mutations.

- Lambda and Groovy expressions and functions, such as the ``.map{<expression>}``, the ``.by{<expression>}``, and the ``.filter{<expression>}`` functions, ``1+1``, and ``System.currentTimeMillis()``, etc.

- Gremlin traversal strategies.

- Transactions.

- Secondary index isn't currently available.  Primary keys will be automatically indexed.

Gremlin steps
~~~~~~~~~~~~~

Currently GIE supports the following Gremlin steps:

- Source steps, e.g.,

.. code:: java

    //V
    g.V()
    g.V(id1, id2)
    
    //E
    g.E()

- Filter steps, e.g.,

.. code:: java

    //has
    g.V().has("attrName")
    g.V().has("attrName", attrValue)
    g.V().has("attrName", gt(1))
    
    //is
    g.V().values("age").is(gt(70))
    
    //filter
    g.V().filter(values("age").is(gt(20)))
    
    //where
    g.V().where(out().count().is(gt(4)))
    
    //dedup
    g.V().out().dedup()
    g.V().out().dedup().by("name")
    
    //range
    g.V().out().limit(100)
    g.V().out().range(10, 20)
    
    //simplePath
    g.V().repeat(out().simplePath()).times(3).valeus("name")
    
    //and/or
    
    //Text.*
    g.V().has("name", Text.match(".*j.*"))
    g.V().values("name").filter(Text.match(".*j.*"))
    g.V().has("name", Text.startsWith("To"))
    g.V().values("name").filter(Text.startsWith("To"))
    
    //P.not
    g.V().has("name", P.not(Text.startsWith("To")))
    
    //Lists.contains*
    g.V().has("a", Lists.contains(30))
    g.V().values("a").filter(Lists.containsAny(Lists.of(10, 20, 30))
    g.V().has("a", P.not(Lists.contains(30)))

- Map steps, e.g.,

.. code:: java

    //constant
    g.V().out().contant(1)
    g.V().out().constant("aaa")
    
    //local count
    g.V().out().values("age").fold().count(local)
    
    //local dedup
    g.V().out().fold().dedup(local).by("name")
    
    //otherV
    g.V().bothE().otherV()
    
    //id
    g.V().id()
    
    //label
    g.V().label()
    
    //local order
    g.V().out().fold().order().by("name")
    
    //property key
    g.V().properties("name").key()
    
    //property value
    g.V().properties("name").value()
    
    //local range
    g.V().out().fold().order(local).by("name").range(local, 2, 4)
    
    //as...select
    g.V().as("a").out().out().select("a")
    g.V().as("a").as("b").out("c").out().select("a", "b", "c")
    
    //path
    g.V().out().in().path()
    g.V().outE().inV().path().bay("name").by("weight").by("name")

- FlatMap steps, e.g.,

.. code:: java

    //out/in/both
    g.V().out()
    g.V().in('person_knows_person')
    
    //outE/inE/inV/outV
    g.V().outE('person_knows_person').inV()
    g.V().inE().bothV()
    
    //properties
    g.V().values()
    g.V().values("name", "age")
    g.V().valueMap()
    
    //branch with option
    g.V().branch(values("name")).option("tom", out()).option("lop", in()).option(none, valueMap())
    g.V().branch(out.count()).option(0L, valueMap()).option(1L, out()).option(any, in())
    
    //unfold
    g.V().group().by().by(values("name")).select(values).unfold()
    
- Aggregate steps, e.g.,

.. code:: java

    //global count
    g.V().out().count()
    g.V().where(out().in().count().is(0))
    
    //fold
    g.V().fold()
    g.V().values("name").fold()
    
    //groupCount
    g.V().out().groupCount()
    g.V().values("name").groupCount()
    
    //groupBy
    g.V().out().group()
    g.V().out().group().by("name")
    g.V().out().group().by().by("name")
    
    //global max/min
    g.V().values("age").max()
    g.V().values("age").min()
    
    //global sum
    g.V().values("age").sum()

- Loop steps, e.g.,

.. code:: java

    //repeat...times
    g.V().repeat(out()).times(4).valueMap()
    
    //repeat...until
    g.V().repeat(out()).until(out().count().is(eq(0))).valueMap()
    g.V().repeat(out()).until(out().count().is(eq(0)).or().loops().is(gt(3))).where(out().count().is(eq(0)))
    
    //emit
    g.V().emit().repeat(out()).times(4).valueMap()
    
- Limit step.

Known limitations
~~~~~~~~~~~~~~~~~

The following steps are not currently available.

- Match
- Explain
- Profile
- Sack
- Subgraph (a simplified version to extract subgraphs into Vineyard is supported) 
- Cap
- ``GraphComputer`` API (such as PageRank and ShortestPath) -- please use the GraphScope Analytics Engine for the same purpose instead.

In addition, the Repeat step is supported, unless it is nested within another Repeat step.
