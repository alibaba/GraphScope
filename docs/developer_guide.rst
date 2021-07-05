Developer Guide
===============

GraphScope has been developed by an active team of software engineers and researchers.
Any contributions from the open-source community to improve this project are  greatly appreciated!

GraphScope is licensed under Apache License 2.0.

Building and Testing
--------------------

GraphScope has many dependencies.

To make life easier, we provide two docker images with all required dependencies
installed.

    - `graphscope-vineyard` as the builder, and
    - `graphscope-runtime` as the base image for runtime.

For developers, they just need to ``git clone`` the latest version of code from
our `repo <https://github.com/alibaba/GraphScope>`_,
make their changes to the code and build with command in the root:

.. code:: bash

    make graphscope

This command triggers the building process.
It will build the current source code in a container with image `graphscope-vineyard`,
and copy built binaries into a new image based from `graphscope-runtime`.
The generated releasing image is tagged as ``graphscope/graphscope:SHORTSHA``

GraphScope python client is separate with the engines image.
If you are developing python client and not modifying the proto files,
the engines image doesn't need to rebuild.
You may want to re-install the python client on local.

.. code:: bash

    cd python
    python3 setup.py install


To test the newly built binaries, manually open a session and assigned your image:

.. code:: python

    import graphscope

    sess = graphscope.session(k8s_gs_image='graphscope/graphscope:SHORTSHA')

    # ...


Or use the test script to pass all test cases.

.. code:: bash

    ./scripts/test.sh --all --image graphscope/graphscope:SHORTSHA

Build Python Wheels
-------------------

GraphScope's python client can run on Linux and MacOS, which can be installed from wheel packages we
distributed on `pypi <https://pypi.org/project/graphscope>`_. For developers, the wheel packages could
be built via the following procedure:

Linux
^^^^^

The wheel packages for Linux is built inside the manylinux2010 environment. The pre-built docker image
is available via

.. code:: bash

    docker pull registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-manylinux2010:latest

Or, you can build the image from scratch. Assuming you are in the root directory of GraphScope repository,
You could build the docker image (note that you only need to rebuild the docker image when you
update dependencies in :code:`manylinux2010.Dockerfile`) by

.. code:: bash

    cd k8s
    make graphscope-manylinux2010

The wheel packages for python{36,37,38,39} could be build by the following command:

.. code:: bash

    cd k8s
    make graphscope-manylinux2010-py{36,37,38,39}

MacOS
^^^^^

The wheel packages for MacOS could be built directly on Mac. Assuming you are in the root directory of
GraphScope repository:

.. code:: bash

    python3 setup.py bdist_wheel

To make sure the maximum compatibility you may need:

.. code:: bash

    python3 setup.py bdist_wheel --plat-name macosx-10.9-x86_64

Note that if you want to build wheel packages for different Python versions, you may need to install multiple
version of Python using `conda` or `pyenv`.

The GraphScope analytical engine and interactive engine could be built locally on mac with script.

If GraphScope's dependencies are not satisfied，you could use the install_denpendencies
script to install dependencies.

.. code::shell

    ./script/install_denpendencies.sh

build the analytical engine and interactive engine with script.

.. code::shell

    ./script/build.sh


Code Format
-----------

GraphScope follows the `Google Style Guide <https://google.github.io/styleguide/cppguide.html>`_
for C++ and `black <https://github.com/psf/black#the-black-code-style>`_ for python.

Please reformat your code with ``clang-format`` and ``black`` if your Pull Request violates the CI.