Developer Guide
===============

`GraphScope <https://github.com/alibaba/GraphScope>`_ has been developed by an active team of
software engineers and researchers. Any contributions from the open-source community to improve
this project are greatly appreciated!

GraphScope is licensed under Apache License 2.0.


Building and Testing GraphScope locally with Docker
---------------------------------------------------

GraphScope has many dependencies. To make life easier, We provide a docker image based on centos7
with all required dependencies installed.

.. code:: bash

    sudo docker pull registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:latest

For developers, they just need to ``git clone`` the latest version of code from our `repo <https://github.com/alibaba/GraphScope>`_,
make their changes to the code and build GraphScope with command:

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

If you just change or want to build one of the specific components of graphscope, such as `python client` or `analytical engine`,
you can run command with:

.. code:: bash

    cd GraphScope
    # make client/gae/gie/gle/coordinator
    make client


Building and Testing GraphScope on Kubernetes
---------------------------------------------

For developers, they just need to ``git clone`` the latest version of code from our `repo <https://github.com/alibaba/GraphScope>`_,
make their changes to the code and build with command to build graphscope image:

.. code:: bash

    cd GraphScope
    make graphscope-dev-image

This command triggers the building process. It will build the current source code in a container with
image `graphscope-vineyard`, and copy built binaries into a new image based from `graphscope-runtime`.
The generated releasing image is tagged as ``graphscope/graphscope:SHORTSHA``

GraphScope python client is separate with the engines image. If you are developing python client and
not modifying the proto files, the engines image doesn't need to rebuild. You may want to reinstall
the python client on local.

.. code:: bash

    cd python
    python3 setup.py install


To test the newly built binaries, manually open a session and assigned your image:

.. code:: python

    import graphscope

    sess = graphscope.session(k8s_gs_image='graphscope/graphscope:SHORTSHA')

    # ...


Building and Testing GraphScope on local
---------------------------------------------------

To build graphscope Python package and the engine binaries, some dependencies and build tools need to be installed.

To make life easier, we provide a script to install the dependencies and build tools.
the script is supported on the following 64-bit systems:

- Ubuntu 18.04 or later
- CentOS 8 or later
- macOS 11.2.1 (Big Sur) or later, with both Intel chip and Apple M1 chip

The script would install following dependencies or tools which needed by GraphScope building:
- C++ compiler (gcc or llvm)
- cmake (>=3.1)
- java sdk (>=8)
- maven
- boost (>=1.66)
- apache-arrow
- rust (> 1.52.0)
- etcd
- openmpi
- protobuf
- grpc
- libgrape-lite (the core of analytical engine)
- vineyard
- fastFFI (for grape-jdk)

First, you need to ``git clone`` the latest version of code from our `repo <https://github.com/alibaba/GraphScope>`_
and run the command:

.. code:: bash

    cd GraphScope
    ./scripts/install_deps.sh --dev

    # With argument --cn to speed up the download if you are in China.
    ./scripts/install_deps.sh --dev --cn

The `install_deps.sh` does not install the grape-jdk dependency `fastFFI` by default.
If you don't care about `grape jdk <https://github.com/alibaba/GraphScope/blob/main/analytical_engine/java/README.md>`,
you can ignore this phase. But if you want to use the grape jdk of GraphScope,
you need to run the command after install the default dependencies.

.. code::bash
    # source the environment created by command above
    source ~/.graphscope_env
    # install the grape-jdk dependency
    ./script/install_deps.sh --grape-jdk


Then you can build GraphScope with pre-configured `make` commands.

```bash
# to make graphscope whole package, including python package + engine binaries.
make graphscope

# or make the engine components
# make gie
# make gae
# make gle
```

To test the newly built binaries, manually open a session:

.. code:: python

    import graphscope

    sess = graphscope.session(cluster_type="hosts")

    # ...


Build Python Wheels
-------------------

Linux
^^^^^

The wheel packages for Linux is built inside the manylinux2014 environment.

- Build GraphScope Server Wheels

.. code:: bash

    cd GraphScope
    make graphscope-py3-package

- Build GraphScope Client Wheels for python{36,37,38,39}

.. code:: bash

    cd GraphScope
    make graphscope-client-py3-package


MacOS
^^^^^

The wheel packages for MacOS could be built directly on Mac, thus you need to install the dependent locally first.

.. code:: bash

    cd GraphScope
    ./scripts/install_deps.sh --dev --vineyard_prefix /opt/vineyard
    source ~/.graphscope_env

Assuming you are in the root directory of GraphScope repository.

- Build GraphScope Server Wheels

.. code:: bash

    cd GraphScope
    make graphscope-py3-package

Build GraphScope Client Wheels for specified python version.

.. code:: bash

    cd GraphScope
    make graphscope-client-py3-package

Note that if you want to build wheel packages for different Python versions, you may need to install multiple
version of Python using `conda <https://docs.conda.io/en/latest/>`_ or `pyenv <https://github.com/pyenv/pyenv>`_.




Code Format
-----------

GraphScope follows the `Google Style Guide <https://google.github.io/styleguide/cppguide.html>`_
for C++ and `black <https://github.com/psf/black#the-black-code-style>`_ for python.

Please reformat your code with ``clang-format`` and ``black`` if your Pull Request violates the CI.
