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
     .out('k-1..k', 'transfer')
     .with('PATH_OPT', 'SIMPLE')
     .endV()
     .where(out('transfer').eq('s'))
     .path().limit(1)

First, the source operator ``V`` (with the ``has`` filter) returns all the ``account`` vertices with an identifier of ``2``. The ``as`` operator is a *modulator* that does not change the input collection of traversers but introduces a name (``s`` in this case) for later references. Second, it traverses the outgoing ``transfer`` edges for exact ``k-1`` times ( ``out()`` with a range of lower bound ``k-1`` (included) and upper bound ``k`` (excluded)), skipping any repeated vertices (``with()`` the ``SIMPLE`` path option). Third, the ``where`` operator checks if the starting vertex ``s`` can be reached by one more step, that is, whether a cycle of length ``k`` is formed. Finally, for qualifying traversers, the ``path`` operator returns the full path information. The ``limit`` operator at the end indicates only one such result is needed.


Compatibility with TinkerPop
----------------------------

GIE supports the property graph model and Gremlin traversal language defined by Apache TinkerPop, and provides a Gremlin *Websockets* server that supports TinkerPop version 3.3 and 3.4. In addition to the original Gremlin queries, we further introduce some syntactic sugars to allow more succinct expression. In this section, we provide an overview of the key differences between our implementation of Gremlin and the Apache TinkerPop specification.

Property graph constraints
~~~~~~~~~~~~~~~~~~~~~~~~~~

The current release (MaxGraph) leverages `Vineyard <https://github.com/v6d-io/v6d>`_ to supply an in-memory store for *immutable* graph data that can be partitioned across multiple servers.  By design, it introduces the following constraints:

- Each graph has a schema comprised of the edge labels, property keys, and vertex labels used therein.

- Each vertex type or label has a primary key (property) defined by user.  The system will automatically generate a ``String``-typed unique identifier for each vertex and edge, encoding both the label information as well as user-defined primary keys (for vertex).

- Each vertex or edge property can be of the following data types: ``int``, ``long``, ``float``, ``double``, ``String``, ``List<int>``, ``List<long>``, and ``List<String>``.

Unsupported features
~~~~~~~~~~~~~~~~~~~~

Because of the distributed nature of the system, the following features are not supported in the current release:

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

    //hasLabel
    g.V().hasLabel("labelName")
    g.V().hasLabel("labelName1", "labelName2")

    //has
    g.V().has("attrName")
    g.V().has("attrName", attrValue)
    g.V().has("labelName", "attrName", attrValue)
    g.V().has("attrName", eq(1))
    g.V().has("attrName", neq(1))
    g.V().has("attrName", lt(1))
    g.V().has("attrName", lte(1))
    g.V().has("attrName", gt(1))
    g.V().has("attrName", gte(1))
    g.V().has("attrName", within([1,2,3]))
    g.V().has("attrName", without([1,2,3]))
    g.V().has("attrName", inside(10, 20))
    g.V().has("attrName", outside(10, 20))

    // P.not
    g.V().has("attrName", P.not(eq(10)))

    //is
    g.V().values("age").is(gt(70))

    //filter with expression (`expr()` syntactic sugar)
    g.V().where(expr('@.age > 20')) //@.age refers to the "age" property of the head entry
    g.V().as('a').out().as('b').where(expr('@a.age <= @b.age'))  //@a.age refers to the "age" property of the "a" entry
    g.V().where(expr('30 within @.a'))  //the "a" property of the head entry has integer array type

    //project with expression (`expr()` syntactic sugar)
    g.V().select(expr("@.age")) //@.age refers to the "age" property of the head entry

    //bits manipulation in expression
    g.V().select(expr("@.number & 2")) //the "number" property of the head entry is integer type
    g.V().select(expr("@.number | 2"))
    g.V().select(expr("@.number ^ 2"))
    g.V().select(expr("@.number << 2"))
    g.V().select(expr("@.number >> 2"))
    g.V().where(expr("@.number & 64 != 0"))

    //arithmetic operations in expression
    g.V().select(expr("@.number + 2"))
    g.V().select(expr("@.number - 2"))
    g.V().select(expr("@.number * 2"))
    g.V().select(expr("@.number / 2"))
    g.V().select(expr("(@.number + 2) / 4 + (@.age * 10)")) //the "number" and "age" properties of the head entry are integer type

    //exponentiation in expression
    g.V().select(expr("@.number ^^ 3"))
    g.V().select(expr("@.number ^^ -3"))

    //filter with a sub-query
    g.V().where(out().count().is(gt(4)))

    //dedup
    g.V().out().dedup()
    g.V().out().dedup().by("name")
    g.V().as("a").out().dedup("a")
    g.V().as("a").out().dedup("a").by("name")

    //limit
    g.V().out().limit(100)
    // to supported
    g.V().out().range(10, 20)

    //and/or

    //TextP.*
    g.V().has("attrName", TextP.containing("substr"))
    g.V().has("attrName", TextP.notContaining("substr"))
    g.V().has("attrName", TextP.startingWith("substr"))
    g.V().has("attrName", TextP.notStartingWith("substr"))
    g.V().has("attrName", TextP.endingWith("substr"))
    g.V().has("attrName", TextP.notEndingWith("substr"))

- Map steps, e.g.,

.. code:: java

    //constant
    g.V().out().contant(1)
    g.V().out().constant("aaa")

    //id
    g.V().id()

    //label
    g.V().label()

    //otherV
    g.V().bothE().otherV()

    //as...select
    g.V().as("a").out().out().select("a")
    g.V().as("a").out().as("b").out().as('c').select("a", "b", "c")

- FlatMap steps, e.g.,

.. code:: java

    //out/in/both
    g.V().out()
    g.V().in('knows')

    //outE/inE/inV/outV
    g.V().outE('knows').inV()
    g.V().inE().bothV()

    //path expansion (syntactic sugar)
    //all simple path (vertex cannot duplicate) from `V()` via `knows` edge
    //with at least 2 hops (included) and at most 4 hops (excluded)
    //keep only the end vertex of the path
    g.V().out('2..4', 'knows').with('PATH_OPT', 'SIMPLE').with('RESULT_OPT', 'END_V').endV()
    //all arbitrary path (vertex can duplicate) from `V()` via `knows` edge
    //with at least 2 hops (included) and at most 4 hops (excluded)
    //keep all vertices of the path
    g.V().out('2..4', 'knows').with('PATH_OPT', 'ARBITRARY').with('RESULT_OPT', 'ALL_V')

    //properties
    g.V().values("name")
    g.V().valueMap() // print all properties
    g.V().valueMap("name")
    g.V().valueMap("name", "age")

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

    //groupBy multiple keys and set aliases
    g.V().group().by(values("name").as("name"), values("age").as("age"))

    //groupBy multiple aggFuncs and set aliases
    g.V().group().by().by(min().as("min"), max().as("max"))

    //global max/min
    g.V().values("age").max()
    g.V().values("age").min()

    //global sum
    g.V().values("age").sum()

- Match step, e.g.,
.. code:: java
    g.V().match(
        __.as('a').out().as('b'),
        __.as('b').out().as('c')
    ).select('a', 'c')

Known limitations

The following steps/functionalities are not currently available.

- Repeat (as most repeat cases can be fulfilled via path expansion syntactic sugar)
- path()/simplePath() (as most cases can be fulfilled via path expansion syntactic sugar )
- Local (some local operations such as count(local), dedup(local), etc.)
- Branch
- Explain
- Profile
- Sack
- Subgraph (a simplified version to extract subgraphs into Vineyard is supported)
- Cap
- ``GraphComputer`` API (such as PageRank and ShortestPath) -- please use the GraphScope Analytics Engine for the same purpose instead.

