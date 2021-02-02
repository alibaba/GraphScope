Integrating Other Data Sources
==============================

Data source plays an important role in **GL**, through which **GL**
loads the raw data to build graph and sync states among distributed
servers. The data source is implemented by a kind of **file system**.
Currently, GL has implemented the file system interface to operate local
files. When running distributedly, a distributed file system is needed.
**GL** supports to be launched by `KubeFlow <dist_config.md>`__ and all
servers access the file system by mounting **NFS** to local.

If it is difficult to configure your environment with KubeFlow and NFS,
you ``NEED`` to implement a kind of file system to satisfy the
requirements of GL.

File System Interface
=====================

The `interface <../graphlearn/platform/file_system.h>`__ of file system
defines the basic operation for file and directory, such as **Exist**,
**Create** and **Delete**. Besides, we abstract **three** kinds of files
that a file system should implement.

ByteStreamAccessFile
--------------------

A **ByteStreamAccessFile** enables to read a file in byte stream. That
is, you can create such a file reader, seek to an ``offset``, and then
read the next ``n bytes`` until reach the end.

.. code:: cpp

   virtual Status Read(size_t n, LiteString* result, char* buffer) = 0;

The **GetFileSize()** interface in file system should return the size in
bytes for a **ByteStreamAccessFile**.

StructuredAccessFile
--------------------

Different from **ByteStreamAccessFile**, **StructuredAccessFile** will
consume the file in records instead of in bytes. That is, you can create
such a file reader, seek to an ``offset``, and then read the
``next Record`` until reach the end.

.. code:: cpp

   virtual Status Read(io::Record* result) = 0;

A ``Record`` contains several columns described by **Schema**. Each
column value may be in type of ``Int32``, ``Int64``, ``Float``,
``Double`` or ``String``.

The raw graph data will be placed in **StructuredAccessFile**\ s, **GL**
will load it and build graph index. Refer `HERE <source_data.md>`__ for
more supported raw data formats.

The **GetRecordCount()** interface in file system should return the size
in records for a **StructuredAccessFile**.

WritableFile
------------

A **WritableFile** enables to append pieces of data into a file, and
does not care about what the format is.

.. code:: cpp

   virtual Status Append(const LiteString& data) = 0;

Register with Scheme
====================

At last, register the new file system to the **Environment**. Each file
system should has a unique scheme, such as ``hdfs://``.

::

   REGISTER_FILE_SYSTEM("scheme", MyFileSystem);

After registration, you can use such a file path when building
`Graph <graph.md>`__.

`Home <../README.md>`__
