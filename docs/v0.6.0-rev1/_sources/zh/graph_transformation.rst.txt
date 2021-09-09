.. _graph_transformation:


图的变换操作
=========================

我们将介绍一系列可以在图上进行新增/投影的方法，以及如何将一个复杂的图转换为可以适配普通算法应用的方法。
最后，我们展示如何将算法得到的结果加回到图中去。

具体而言，图 :class:`Graph` 提供了两个增加标签的函数, 和一个投影的函数。

.. code:: python

    def add_vertices(self, vertices, label="_", properties=[], vid_field=0):
        pass

    def add_edges(self, edges, label="_", properties=[], src_label=None, dst_label=None, src_field=0, dst_field=1):
        pass

    def project(self, vertices, edges):
        pass


其中，我们已经在 :ref:`载图<loading_graphs>` 一节见到过 `add_vertices` 和 `add_edges` 这两个函数，当时我们用它来构建一张图。
进一步的，当图构建好并载入了 Vineyard 中之后，我们仍然可以用其增加更多的标签。当然这一步并不会在原图上修改，而是会返回基于原图之上，
增加了新的标签的新图。


添加新的标签
----------------

以 LDBC-SNB 属性图为例，我们现在载入其中一部分标签，作为接下来一系列转换操作的起始图。

.. code:: python

    import graphscope
    from pathlib import Path
    from graphscope.framework.loader import Loader

    sess = graphscope.session()

    graph = sess.g(directed=directed)
    graph = graph.add_vertices(Loader("person_0_0.csv", delimiter="|"), "person")
    graph = graph.add_edges(Loader("person_knows_person_0_0.csv", delimiter="|"),
                "knows", src_label="person", dst_label="person"
        )

    # graph has 1 vertex label "person"
    print(graph.schema)

到这里， 我们已经载入了一张图。接下来我们在这张图上再添加几个标签。

.. code:: python

    graph1 = graph.add_vertices(Loader("comment_0_0.csv", delimiter="|"), "comment")

    # Now graph1 has 2 vertex labels "person" and "comment"
    print(graph1.schema)

    graph2 = graph1.add_edges(Loader("comment_replyOf_comment_0_0.csv", delimiter="|"),
                "replyOf", src_label="comment", dst_label="comment"
        )

    # graph2 has 2 edge labels "knows" and "replyOf"
    print(graph2.schema)


可以看到每次 `add` 都会产生一张新的图，在底层，他们共有的部分会指向同一块内存，所以并不会将原图的数据复制一份。


投影
-------

在某些场景下，我们需要将从一张复杂的图提取出一个子图。这个操作可以借助 `project` 来完成。

.. code:: python

    def project(
            self,
            vertices: Mapping[str, Union[List[str], None]],
            edges: Union[Mapping[str, Union[List[str], None]], None]
        ):
        pass

`project` 包含两个参数 `vertices` 和 `edges`，其值为一个字典，字典的键是标签名，值是要取的属性的列表。值可以为 None，
代表选择所有的属性。

`project` 的返回值也是一个属性图，并且可以被进一步 `project`。
以下是几个例子。

.. code:: python

    sub_graph = graph2.project(vertices={"person": ["firstName", "lastName"]}, edges={"knows": None})

    # 包含一个点标签 "person" 和一个边标签 "knows"， 以及所选择的属性。
    print(sub_graph.schema)

    sub_graph2 = sub_graph.project(vertices={"person": []}, edges={"knows": ["creationDate"]})

    # 现在点上没有属性，边上有一个属性
    print(sub_graph2.schema)



自动转换为简单图
--------------------------

当执行一个仅可以跑在简单图上的算法时，其会默认将其参数中的属性图转换为简单图，如果不能进行这种转换（即多于一个点标签和一个边标签，或多于一个属性），
那么就会报错。

.. code:: python

    from graphscope import wcc

    ret = wcc(sub_graph2)

    # wcc(graph2)  # 错误！ 转换不合法，多于一个点/边标签
    # wcc(sub_graph)  # 错误！转换不合法，多于一个属性


将计算结果作为新的属性加入图中
------------------------------------------------

上一步算法的运行结果可以被加入一张图中, 作为点的一个属性。

不仅可以加入运算结果到直接被查询的图上，还可以将这个查询结果加到被 `project` 而得到被查询的图上，只要被加入属性的点标签相同。

.. code:: python

    new_graph = sub_graph2.add_column(ret, selector={'cc': 'r'})

    new_graph = sub_graph.add_column(ret, selector={'cc': 'r'})

    new_graph = graph.add_column(ret, selector={'cc': 'r'})
