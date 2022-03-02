.. _gae_java_sdk_about:

About Grape JDK 
========================

Grape JDK is a subproject under GraphScope, presenting an efficient java SDK for GraphScope analytical engine: `GRAPE`.
Grape JDK wrap the core c++ implemented APIs in Java interfaces, which guides Java programmers to implement a PIE algorithm
from scratch. It also provides efficient implementation for cross-language communication by leveraging 
`Alibaba fastFFI <https://github.com/alibaba/fastFFI>`_. 
See `grape-jdk README <https://github.com/alibaba/GraphScope/blob/main/analytical_engine/java/README.md>`_ for more information.


Project Structure
-------------------------

Grape JDK has three subprojects

- **grape-jdk**: Defines the interfaces you need to write you own java app.

- **grape-runtime**: Contains the implementation for the interfaces defines in ``grape-jdk``. You never need it
if you are just want to write a java app.

- **grape-demo**: Having no idea how to implement your algorithm to fit Grape `PIE` programming model? Take a look
at the provided sample apps, hopefully you will be inspired!


Building from source
-------------------------

You need fastFFI installed in your local repository before building ``grape-jdk``.

.. code:: shell

    git clone https://github.com/alibaba/fastFFI.git 
    cd fastFFI 
    mvn clean install -DskipTests -Dmaven.antrun.skip=true


Although Grape JDk only support clone & and building from source at the moment, we plans to distribute it through maven
remote repository in near feature. 

To only build ``grape-jdk``

.. code:: shell

    cd GraphScope/analytical_engine/java/grape-jdk
    mvn clean install

If you want to build from parent project without building :code:`libgrape-jni.so`

.. code:: shell

    cd GraphScope/analytical_engine/java/
    mvn clean install -Dmaven.antrun.skip=true

You can turn on the jni library building by remove the mvn option `-Dmaven.antrun.skip=true`. But you can not expect a successful building if 
your develop environment doesn't satisfy the following requirements.

- GraphScope-GAE installed
- libgrape-lite installed
- mac/linux platform