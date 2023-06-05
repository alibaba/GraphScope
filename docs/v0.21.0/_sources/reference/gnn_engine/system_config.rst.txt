System configuration
====================

If needed, set these system configs ``BEFORE`` using GL’s other modules.

.. code:: python

   import graphlearn as gl
   gl.set_xxx
   gl.set_yyy

   gl.other_models...

-  Set default neighbor id. Default is ``0``.

When the neighbor count is less than sampling count, fill with a default
id.

.. code:: python

   gl.set_default_neighbor_id(nbr_id)

-  Set default int attribute value. Default is ``0``.

Fill with the default attribute value for not found nodes or edges when
looking up.

.. code:: python

   gl.set_default_int_attribute(value)

-  Set default float attribute value. Default is ``0.0``.

Fill with the default attribute value for not found nodes or edges when
looking up.

.. code:: python

   gl.set_default_float_attribute(value)

-  Set default string attribute value. Default is ``''``.

Fill with the default attribute value for not found nodes or edges when
looking up.

.. code:: python

   gl.set_default_string_attribute(value)

-  Set inter thread number. Default is ``32``.

Inter threads are used to handle operators and response system request.

.. code:: python

   gl.set_inter_threadnum(num)

-  Set intra thread number. Default is ``32``.

Intra threads are used inside operators to speed up.

.. code:: python

   gl.set_intra_threadnum(num)

-  Set RPC max message size. Default is ``16MB``.

Usually no need to reset. In some large-scale cases, enlarge it as
requirement.

.. code:: python

   gl.set_rpc_message_max_size(size)

`Home <../README.md>`__
