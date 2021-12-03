开发者指南
==========

`GraphScope <https://github.com/alibaba/GraphScope>`_ 的背后有一群活跃的工程人员和研究人员团队来推进日常的开发和维护。我们热忱欢迎来自开源社区的、为改善该项目所做的任何贡献！

GraphScope 遵循 Apache License 2.0 的开源协议。


基于 Docker 环境构建并测试 GraphScope
----------------------------------

构建 GraphScope 需要一些第三方的工具和依赖。为了让开发者更容易上手，我们提供了安装了所需依赖的 docker 镜像。

.. code:: bash

    sudo docker pull registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:latest

开发者需要通过 ``git clone`` 的命令从我们的开源代码库 `repo <https://github.com/alibaba/GraphScope>`_ 中获得最新版的代码,
在此基础上做开发或代码的更改，然后在代码的根目录执行：

.. code:: bash

    # set docker container shared memory: 10G
    sudo docker run --shm-size 10240m -it registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:latest /bin/bash

    git clone https://github.com/alibaba/GraphScope.git

    # download dataset for test
    git clone https://github.com/GraphScope/gstest.git

    # building
    cd GraphScope && make install

    # testing
    cd GraphScope && make minitest(unittest)

你也可以通过如下构建命令开发并测试其中某一个模块，比如 `python 客户端` 或 `图分析引擎 GAE 模块`。

.. code:: bash

    cd GraphScope
    # make client/gae/gie/gle/coordinator
    make client


基于 Kubernetes 环境构建并测试 GraphScope
--------------------------------------

开发者需要通过 ``git clone`` 的命令从我们的开源代码库 `repo <https://github.com/alibaba/GraphScope>`_ 中获得最新版的代码,
在此基础上做开发或代码的更改，然后通过如下命令构建 GraphScope 镜像。

.. code:: bash

    cd GraphScope
    make graphscope-dev-image

该命令会开始 GraphScope 的构建过程，该过程将在 `graphscope-vineyard` 的容器中构建当前源代码， 并将生成的可执行文件复制到
运行时基础镜像 `graphscope-runtime` 中， 生成的镜像将被标记(tag)为 ``graphscope/graphscope:SHORTSHA``。

GraphScope 的 Python 客户端不包含在该镜像中，构建也与引擎有所不同，如果开发者正在开发 Python 客户端并且未修改引擎相关的文件，
那么 GraphScope 镜像是不需要重建的。因此，开发者只需要在本地重新安装 GraphScope Python 客户端即可。

.. code:: bash

    cd python
    python3 setup.py install

测试新构建的镜像，用户可以手动打开一个会话，指定用新编译的镜像。

.. code:: python

    import graphscope

    sess = graphscope.session(k8s_gs_image='graphscope/graphscope:SHORTSHA')

    # ...


构建 Python Wheels
------------------

Linux
^^^^^

Linux 下的 `Wheel <https://pypi.org/project/graphscope>`_ 分发包是基于 manylinux2014 环境下构建的。

- 构建 GraphScope Server Wheels

.. code:: bash

    cd GraphScope
    make graphscope-py3-package

- 在 Python{36,37,38,39} 下分别构建 GraphScope client wheels

.. code:: bash

    cd GraphScope
    make graphscope-client-py3-package

macOS
^^^^^

由于 macOS 下的构建过程是在本地(非docker container)中进行，因此需要本地事先安装 GraphScope 的依赖。

.. code:: bash

    cd GraphScope
    ./scripts/install_deps.sh --dev --vineyard_prefix /opt/vineyard
    source ~/.graphscope_env

- 构建 GraphScope Server Wheels

.. code:: bash

    cd GraphScope
    make graphscope-py3-package

- 基于当前 Mac 环境下的 Python 版本构建 GraphScope client wheels

.. code:: bash

    cd GraphScope
    make graphscope-client-py3-package


需要注意的是，如果你需要该分发包能支持不同的 Python 版本，你可能需要通过 `conda` 或者 `pyenv` 安装多个 Python 的版本


代码风格
-----------

GraphScope 遵循 `Google C++ 代码风格 <https://google.github.io/styleguide/cppguide.html>`_
和 `black Python 风格 <https://github.com/psf/black#the-black-code-style>`_ 。

如果你的代码没有通过CI的风格检查，你可以使用 ``clang-format`` 或 ``black`` 格式化你的代码。
