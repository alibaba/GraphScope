Developer Guide
=======================

Install dependencies
--------------------

In the `pygrape` directory, run

.. code:: shell

    pip install -U -r requirements.txt

Generate protobuf and gar
-------------------------

In the root directory of pygrape, run

.. code:: shell

    python3 setup.py build_proto build_gar

If you want to generate protobuf files only, run

.. code:: shell

    python3 setup.py build_proto

If you want to generate the builtin gar file ony, run

.. code:: shell

    python3 setup.py build_gar

Test your code
--------------

You can run all tests using the `pytest` command:

.. code:: shell

    python3 setup.py test

or

.. code:: shell

    pytest .

If you only want to run tests in a specific file:

.. code:: shell

    pytest tests/test_builtin.py

If you want to run a specific test case only:

.. code:: shell

    pytest -k test_modify_edges

or

.. code:: shell

    pytest tests/test_nx.py -k test_modify_edges

Code format
-----------

Before commit, you need to run the linters first

.. code:: shell

    pylint grape

    flake8 grape --count --show-source --statistics

You can install `pylint` and `flake8` using pip with the following command:

.. code:: shell

    pip install flake8 pylint

Release the package
-------------------

You can build the doc of this package using sphinx, just run the following command:

.. code:: shell

    python3 setup.py build_sphinx

or inside the `docs` directory, run

.. code:: shell

    make html

The package can be easily packaged with

.. code:: shell

    python3 setup.py sdist

or (for `wheel` package)

.. code:: shell

    python3 setup.py bdist_wheel

Bug report
----------

If you find anything that doesn't work with pygrape, free to issue a bug and you can reach us via `7br@alibaba-inc.com <mailto:7br@alibaba-inc.com>`_.
