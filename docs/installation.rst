Installation
============

GraphScope is tested and supported on the following 64-bit systems:

- Python 3.6 - 3.9
- Ubuntu 18.04 or later
- CentOS 7 or later
- macOS 11.2.1 (Big Sur) or later (Apple M1 is not support yet!)


Instal GraphScope locally without Kubernetes
--------------------------------------------

GraphScope is distributed as a `python package <https://pypi.org/project/graphscope>`_ and can be easily installed with `pip <https://pip.pypa.io/en/stable/>`_.

**GraphScope packages require a pip version > 19.0 (or > 20.3 for macOS)**

.. code:: bash

    # Requires the latest pip
    pip3 install --upgrade pip

    # Current stable release
    pip3 install --upgrade graphscope


Install GraphScope Client for Kubernetes
----------------------------------------

For quickly getting started, we set up a local kubernetes cluster and take advantage of pre-built Docker images as follows.
The following dependencies or tools are required.

- Python 3.6 - 3.9
- Local Kubernetes cluster set-up tool (e.g. `Kind <https://kind.sigs.k8s.io>`_)

On Windows and macOS, you can follow the official guides to install them and enable Kubernetes in Docker.
For Ubuntu/CentOS Linux distributions, we provide a script to install the above
dependencies and prepare the environment.
Alternatively, you may want to install `WSL2 <https://docs.microsoft.com/en-us/windows/wsl/install-win10>`_ on Windows to use the script.

.. code:: bash

    # run the k8s environment preparing script.
    ./scripts/install_deps.sh --k8s


Install a subset of the whole package with `client <https://pypi.org/project/graphscope-client>`_ functions **only** for running GraphScope on `Kubernetes <https://kubernetes.io>`_.

.. code:: bash

    # Requires the latest pip
    pip3 install --upgrade pip

    # Current stable release
    pip3 install --upgrade graphscope-client