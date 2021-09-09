Installation
============

The client of GraphScope is distributed as a Python package. It manages a set of
backend engines and the coordinator via containers.

In practice, GraphScope runs on clusters managed by Kubernetes.
For quickly getting started, we set up a local Kubernetes cluster and take advantage of pre-built Docker images as follows.

To run GraphScope on your local computer, the following dependencies or tools are required.

- Docker
- Python 3.6+ (with pip)
- Local Kubernetes cluster set-up tool (e.g. `Kind <https://kind.sigs.k8s.io>`_)

On Windows and macOS, you can follow the official guides to install them and enable Kubernetes in Docker.
For Ubuntu/CentOS Linux distributions, we provide a script to install the above
dependencies and prepare the environment.
Alternatively, you may want to install `WSL2 <https://docs.microsoft.com/zh-cn/windows/wsl/install-win10>`_ on Windows to use the script.

.. code:: bash

    # run the environment preparing script.
    ./scripts/prepare_env.sh

Then the package can be easily installed using `pip <https://pip.pypa.io/en/stable/>`_:

.. code:: shell

    pip install graphscope

Or you can install the package from source

.. code:: shell

    pip install 'git+https://github.com/alibaba/GraphScope.git'

If you have the :code:`.wheel` package, you can install the package using

.. code:: shell

    pip install graphscope-0.1.macosx-10.14-x86_64.tar.gz


To build the package from source code, please download the latest version
from our `repo <https://github.com/alibaba/GraphScope.git>`_ with git:

.. code:: shell

    git clone git@github.com:alibaba/GraphScope.git
    git submodule update --init
    cd python

Then install the package from source with the following command:

.. code:: shell

    pip install -U -r requirements.txt
    python setup.py install
