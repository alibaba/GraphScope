图交互式分析引擎 
==============

GraphScope的交互查询引擎（简称GIE）是一个分布式系统，它为不同经验的用户提供了一个易用的交互式环境，支持海量复杂图数据上的 *实时分析与交互探索* 。该引擎支持 `Gremlin <http://tinkerpop.apache.org/>`_ 语言表达的交互图查询，并提供了自动化和用户透明的分布式并行执行。


Apache TinkerPop
----------------

`Apache TinkerPop <http://tinkerpop.apache.org/>`_ 是基于Gremlin语言开发交互式图应用的一个开源框架和事实标准。GIE通过TinkerPop提供的 `Gremlin Server <https://tinkerpop.apache.org/docs/current/reference/#gremlin-server>`_ 接口，实现了与TinkerPop生态无缝集成，从而用户可以直接采用诸如 `Gremlin Console <https://tinkerpop.apache.org/docs/current/reference/#gremlin-console>`_ 的开发工具或通过Java和Python等多种语言接口编写应用逻辑。


利用Python（Gremlin）连接GraphScope
-----------------------------------

如下所示，用户可以很简单的通过Python连上一个载入GraphScope系统的图并发起Gremlin查询。

.. code:: python

    import graphscope
    from graphscope.dataset import load_ldbc

    # 创建一个新的交互会话，载入LDBC示例图数据
    # 随后返回一个Gremlin查询提交入口
    sess = graphscope.session(num_workers=2)
    graph = load_ldbc(sess, prefix='/path/to/ldbc_sample')
    interactive = sess.gremlin(graph)

    # 下面两句Gremlin示例查询分别计算图中顶点和边的总数
    node_num = interactive.execute('g.V().count()').one()
    edge_num = interactive.execute("g.E().count()").one()

上面代码中的 ``interactive`` 对象事实上是Python类 ``InteractiveQuery`` 的一个实例，而这一类封装了用Python实现的完整Gremlin客户端类库 `Gremlin-Python <https://pypi.org/project/gremlinpython/>`_ 。

每一个载入GraphScope的图都包含一个Gremlin查询提交入口，可以像下面这样获得具体的访问地址（URL）:

.. code:: python

    print(interactive.graph_url)

上面的语句会产生如下（格式）的输出：

.. code:: python

    ws://your-endpoint:your-ip/gremlin

有了这一URL信息，用户也可以直接采用Gremlin-Python访问图数据，具体可以参考 `官方文档 <https://tinkerpop.apache.org/docs/current/reference/#gremlin-python>`_ 。


利用Java（Gremlin）连接GraphScope
---------------------------------

TinkerPop同时支持Java语言按类似方式访问，详见Gremlin-Java的 `官方文档 <https://tinkerpop.apache.org/docs/current/reference/#gremlin-java>`_ 。


Gremlin Console（开发控制台）
---------------------------

`Gremlin Console <https://tinkerpop.apache.org/docs/current/tutorials/the-gremlin-console/>`_ 为开发者提供了一个与GraphScope存储的图数据进行交互的控制台，也叫做REPL环境（read-evaluate-print loop）。下面描述如何利用上文获得的URL，安装和配置Gremlin Console以连接GraphScope的步骤：

1.安装Gremlin Console依赖的Java运行时环境，版本需要满足[8, 12)。

2.从 `Apache TinkerPop <https://tinkerpop.apache.org/downloads.html>`_ 下载适当版本的Gremlin Console。

.. code:: bash

    wget https://archive.apache.org/dist/tinkerpop/3.4.8/apache-tinkerpop-gremlin-console-3.4.8-bin.zip

3.解压缩下载的文件。

.. code:: bash

    unzip apache-tinkerpop-gremlin-console-3.4.8-bin.zip

4.进入解压缩的目录。

.. code:: bash

    cd apache-tinkerpop-gremlin-console-3.4.8

5.在 `conf` 子目录创建一个名为 `graphscope-remote.yaml` 的文本文件以配置URL。具体内容如下所示，其中的 *your-endpoint* 和 *your-port* 需要分别替换为从GraphScope会话得到的URL中对应的主机名（或IP）和端口。

.. code::

    hosts: [your-endpoint]
    port: your-port
    serializer: { className: org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0, config: { serializeResultToString: true }}

6.输入下列命令启动Gremlin Console。

.. code:: bash

    bin/gremlin.sh

7.在 `gremlin>` 提示符下，输入下列命令连接到对应的GraphScope会话；第二条命令切换到远程模式，从而接下来输入的所有Gremlin查询都被自动传输到（远程）GraphScope执行。

.. code:: bash

    :remote connect tinkerpop.server conf/graphscope-remote.yaml
    :remote console

8.现在你可以尝试一些简单的Gremlin查询了！例如 ``g.V().limit(1)`` 。当你完成交互，输入下列命令可以退出Gremlin Console。

.. code:: bash

    :exit


Gremlin编程入门--101
--------------------

GIE以忠实保留Gremlin编程模型为设计目标，从而让已有的应用只需最小化的修改就可以扩展到大规模计算集群。在此我们提供一个Gremlin的总体介绍，特别是其中包含的图数据模型和查询语言等关键概念。更详细和完整的介绍，请参考 `TinkerPop reference <https://tinkerpop.apache.org/docs/current/reference/>`_ 。

图数据模型
~~~~~~~~~~

Gremlin允许用户在属性图模型上定义特设（ad-hoc）遍历查询。一个属性图是一个有向图，其中的顶点和边可以拥有一组属性。图中的每个对象（点或边）都有一个唯一标识（``ID``）和一个类别名称（``label``）指定其类型或角色。每个属性是一个包含属性名和属性值的（键-值）对，其所属对象的 ``ID`` 加上属性名可以唯一确定属性值。

.. image:: ../images/property_graph.png
    :width: 400
    :align: center
    :alt: 电商属性图模型示例。

上图展示了一个属性图模型示例。它包含 ``user`` （用户）、 ``product`` （商品）和 ``address`` （地址）三类点，它们通过 ``order`` （购买）、 ``deliver`` （递送）、 ``belongs_to`` （属于）和 ``home_of`` （家庭地址）四类边相互关联。图中虚线展示的一条（从起点到终点的）路径1-->2-->3，代表了一个用户（买家）"Tom"购买了一个卖家"Jack"提供的标价"$99"的商品"gift"。

查询语言
~~~~~~~~

一个Gremlin查询或图遍历的执行，可以用一组 *遍历器* （traversers）标识。它们依据Gremlin查询提供的用户指令在输入图中游走，最终所有停止的遍历器集合（包含它们的位置）代表了查询的结果。一个遍历器是Gremlin引擎处理的最小数据单元。每个遍历器都维护它对应的图中的当前位置，可以是被访问的点、边或属性。同时，可选的它也可以包含走过的完整路径历史甚至应用状态。

Gremlin语言丰富灵活的表达能力主要来自于它对 *嵌套遍历* 的支持，它允许一个（子）查询或遍历被包含在另一个操作中，作为一个可调用的函数被包裹操作用于处理其每一个输入。函数的声明和作用都由包裹操作的语义决定。

例如， ``where`` （过滤）操作可以包含一个嵌套查询，作为过滤条件谓词。而 ``select`` （映射）或 ``order`` （排序）操作各自可以通过嵌套查询讲每一个输入单独映射到从它开始的子遍历得到的结果，或依据结果值作为排序依据。

嵌套遍历的另一个重要应用是表达循环，在Gremlin中通过 ``repeat`` （循环）操作和随后的 ``until/times`` （终止条件）表达。 ``repeat`` 操作包含一个嵌套遍历作为循环体，每一个输入都会重复送入这一子查询，直到终止条件满足。 ``until`` （条件终止）操作类似 ``where`` ，可以表达一个条件谓词，它被独立应用于循环体的每一个输出遍历器，满足条件的遍历器就会离开循环。另一个常用的 ``times`` （迭代轮次终止）操作可以利用一个整型常量 ``k`` 表达固定迭代轮次后终止循环。

一个例子
~~~~~~~~

下面展示了一个完整的Gremlin示例，它尝试从一个给定账户（account）点开始找到长度为 ``k`` 的有向环路。

.. code:: java

    g.V('account').has('id','2').as('s')
     .repeat(out('transfer').simplePath())
     .times(k-1)
     .where(out('transfer').eq('s'))
     .path().limit(1)

首先，输入图操作 ``V`` （包含一个 ``has`` 表达的简单过滤）返回图中满足条件的 ``account`` 点（即唯一标识为 ``2`` 的点）。紧随其后的 ``as`` 操作是一个 *修饰符* ，它不改变输入遍历器集合，但对其中每一个遍历器的当前位置，打上一个有名标签（这个例子中的 ``s`` ），从而今后可以引用。接下来，查询沿着 ``transfer`` 类型的出边循环游走 ``k-1`` 次，且每一次都过滤或跳过路径中的重复点（利用 ``simplePath`` 操作实现）。最后， ``where`` 操作检查此时遍历路径的下一跳是否可以回到起点（用 ``s`` 指代），从而形成一个长度为 ``k`` 的环。对于检测到的环，查询还通过 ``path`` 操作展示每个遍历器的完成路径信息。 ``limit`` 操作类似SQL中的top K，它表达了查询结果仅需要包含一个这样的路径（如果有的话）。


Gremlin兼容性（对比TinkerPop）
----------------------------

GIE支持Apache TinkerPop定义的属性图模型和Gremlin遍历查询，且实现了一个与TinkerPop 3.3和3.4版本兼容的 *Websockets* 服务接口。下面我们列出当前实现和Apache TinkerPop规范的主要差一点（其中一些差异会有机会消除、另一些是目前GraphScope定位的场景差异造成的不同设计选择）。

属性图模型约束
~~~~~~~~~~~~~

目前的MaxGraph技术预览版利用了 `Vineyard <https://github.com/alibaba/libvineyard>`_ 项目提供的分布式内存存储作为输入图，它支持一次载入 *不可修改* 的图模型数据，和图分片存储在分布式集群。当前设计有下面的一些限制：

- Schema（模式）约束：每个图的数据需要满足事先定义的Schema，包括点、边的类型名称（label）和属性名及值类型。

- 主键约束：每个顶点类型需要包含一个用户可自定义的主键（属性），同时系统会为每个点和边对象，自动分配产生一个字符串类型的唯一标识（ID）。对于点来说，ID编码了类型（label）和用户自定义主键信息。

- 每个点或边的属性，可以包含下列类型的属性值：``int``、``long``、``float``、``double``、``String``、``List<int>``、``List<long>`` 和 ``List<String>`` 。

尚不支持的功能特性
~~~~~~~~~~~~~~~~~

因为系统的全分布式可扩展架构，当前定位的场景和实现不支持下列功能：

- 图修改操作。

- Lambda和Groovy表达式或自定义函数，例如：``.map{<expression>}``、``.by{<expression>}`` 和 ``.filter{<expression>}`` 函数，``1+1`` 和 ``System.currentTimeMillis()`` 等表达式或Java调用等等。

- 定制Gremlin图遍历策略（traversal strategies），即查询优化由GraphScope系统自动完成。

- 事务。

- 二级索引目前尚未支持（用户定义的主键会被自动索引）。

支持的Gremlin操作
~~~~~~~~~~~~~~~~~

当前GraphScope支持下列Gremlin操作（和示例用法）：

- Source（输入图），如：

.. code:: java

    //V
    g.V()
    g.V(id1, id2)
    
    //E
    g.E()

- Filter（过滤），如：

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

- Map（映射），如：

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

- FlatMap（多重映射），如：

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
    
- Aggregate（聚合），如：

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

- Loop（循环），如：

.. code:: java

    //repeat...times
    g.V().repeat(out()).times(4).valueMap()
    
    //repeat...until
    g.V().repeat(out()).until(out().count().is(eq(0))).valueMap()
    g.V().repeat(out()).until(out().count().is(eq(0)).or().loops().is(gt(3))).where(out().count().is(eq(0)))
    
    //emit
    g.V().emit().repeat(out()).times(4).valueMap()
    
- Limit（top K，即取前k个结果）。

已知限制
~~~~~~~~

GraphScope暂时不支持下列Gremlin操作（会逐步支持）：

- Match（子图模式匹配）
- Explain（查询计划解释）
- Profile（查询执行性能分析）
- Sack（自定义状态计算）
- Subgraph（计算子图，目前实现了一个简化版本，支持抽取子图写入Vineyard存储）
- Cap（访问自定义状态）
- ``GraphComputer`` 接口（例如PageRank和ShortestPath）；这部分功能GraphScope通过图分析引擎和NetworkX兼容接口提供。

此外，目前支持的Repeat（循环）操作不支持嵌套，也就是在循环体内不可以出现另一个Repeat操作。

