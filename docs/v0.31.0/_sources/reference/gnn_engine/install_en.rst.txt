Install from pip
================

We build the wheel package with\ ``g++ 5.4.0`` and ``python 2.7`` on
``Ubuntu 16.04``. If you have the same environment, you can download and
install directly. Otherwise, please refer to the section ‘build from
source’. Currently, the example models provided by **GL** are developed
based on **TensorFlow 1.12**. To run the example models, please install
**TensorFlow 1.12**. Users who only rely on system interfaces for model
development can modify the source code slightly and remove the relevant
part of ``import *tf*`` in the ``__init__.py`` file.

Get wheel package
-----------------

.. code:: bash

   wget http://graph-learn-whl.oss-cn-zhangjiakou.aliyuncs.com/graphlearn-0.1-cp27-cp27mu-linux_x86_64.whl

Install using pip
-----------------

.. code:: bash

   sudo pip install graphlearn-0.1-cp27-cp27mu-linux_x86_64.whl

Install TensorFlow
------------------

.. code:: bash

   sudo pip install tensorflow==1.12.0

Build from source
=================

Install git
-----------

.. code:: bash

   sudo apt-get install git-all

Install dependent libraries
---------------------------

.. code:: bash

   sudo apt-get install autoconf automake libtool cmake python-numpy

Compile
-------

.. code:: bash

   git clone https://github.com/alibaba/graph-learn.git
   cd graph-learn
   git submodule update --init
   make test
   make python

Install
-------

.. code:: bash

   sudo pip install dist/your_wheel_name.whl

Run test
--------

.. code:: bash

   source env.sh
   ./test_cpp_ut.sh
   ./test_python_ut.sh

Docker image
============

You can download our docker image to run **GL** projects.

CPU version:

::

   docker pull registry.cn-zhangjiakou.aliyuncs.com/pai-image/graph-learn:v0.1-cpu

GPU version:

::

   docker pull registry.cn-zhangjiakou.aliyuncs.com/pai-image/graph-learn:v0.1-gpu

You can also refer to our Dockerfile to build your own image. Please
checkout this `document <../docker_image/README.md>`__.

`Home <../README.md>`__
