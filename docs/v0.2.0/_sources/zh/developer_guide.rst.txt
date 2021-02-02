开发者指南
==========

GraphScope 的背后有一群活跃的工程人员和研究人员团队来推进日常的开发和维护。
我们热忱欢迎来自开源社区的、为改善该项目所做的任何贡献！

GraphScope 遵循 Apache License 2.0 的开源协议。

构建和测试
--------------------

构建 GraphScope 需要一些第三方的工具和依赖。为了让开发者更容易上手，我们提供了两个安装了所需依赖的 docker 镜像。

    - `graphscope-vineyard` 作为编译环境的镜像，
    - `graphscope-runtime` 作为运行时环境的镜像。

开发者需要通过 ``git clone`` 的命令从我们的开源代码库 `repo <https://github.com/alibaba/GraphScope>`_ 中获得最新版的代码,
在此基础上做开发或代码的更改，然后在代码的根目录执行：

.. code:: bash

    make graphscope

该命令会开始 GraphScope 的构建过程。
该过程将在 `graphscope-vineyard` 的容器中构建当前源代码，
并将生成的可执行文件复制到运行时基础镜像 `graphscope-runtime` 中，
生成的镜像将被标记(tag)为 ``graphscope/graphscope:SHORTSHA``。

GraphScope 的 Python 客户端不包含在该镜像中，构建也与引擎有所不同，。
如果开发者正在开发 Python 客户端并且未修改引擎相关的文件，
那么 GraphScope 映像不需要重建。
开发者只需要在本地重新安装 GraphScope Python 客户端。

.. code:: bash

    cd python
    python3 setup.py install

测试新构建的镜像，用户可以手动打开一个会话，指定用新编译的镜像。

.. code:: python

    import graphscope
    
    sess = graphscope.session(k8s_gs_image='graphscope/graphscope:SHORTSHA')
    
    # ...
    

或者使用测试脚本来通过所有的测试用例。

.. code:: bash

    ./scripts/test.sh --all --image graphscope/graphscope:SHORTSHA


构建 Python Wheels
-------------------

GraphScope 的 Python 客户端可以在 Linux 和 macOS 上运行，Python Wheel 包通过
在 `pypi <https://pypi.org/project/graphscope>`_ 分发。 对于开发人员而言，Wheel 包也可以
通过以下过程构建：

Linux
^^^^^

Linux 下的 Wheel 分发包在 manylinux2010 的环境下构建，该编译环境的镜像地址可以这样获得：

.. code:: bash

    docker pull registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-manylinux2010:latest


或者，您可以从 GraphScope 的根目录中，从头构建该镜像。（请注意，您需要在重建 docker 镜像时
更新 `manylinux2010.Dockerfile` 中的依赖项）


.. code:: bash

    cd k8s
    make graphscope-manylinux2010

如果您为 Python{36,37,38,39} 版本构建，可以使用以下命令：

.. code:: bash

    cd k8s
    make graphscope-manylinux2010-py{36,37,38,39}

macOS
^^^^^
为 macOS 准备的 Wheel 分发包可以直接在 macOS 下构建。在代码根目录运行如下命令：

.. code:: bash

    python3 setup.py bdist_wheel

如果你需要 Wheel 包具有最大兼容性：

.. code:: bash

    python3 setup.py bdist_wheel --plat-name macosx-10.9-x86_64

请注意，如果你需要该分发包能支持不同的 Python 版本，你可能需要通过 `conda` 或者 `pyenv` 安装多个 Python 的版本

代码风格
-----------

GraphScope 遵循 `Google C++ 代码风格 <https://google.github.io/styleguide/cppguide.html>`_ 
和 `black Python 风格 <https://github.com/psf/black#the-black-code-style>`_ 。

如果你的代码没有通过CI的风格检查，你可以使用 ``clang-format`` 或 ``black`` 格式化你的代码。