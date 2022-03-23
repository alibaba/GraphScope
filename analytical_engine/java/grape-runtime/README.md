# GraphScope runtime

This folder contains the implementation of **```GraphScope Java runtime```**.

If you just want to implement your java app and run it on ```GraphScope```
Analytical Engine, you have nothing to know about this directory. You Just need to download
GRAPE-jdk, write your own app with the programming interfaces, and invoke
```Graphscope.JavaApp``` to run your java app.

If you are interested in this submodule, than you may find some runtime-needed java utilities to run
your Java App on ```GraphScope``` Analytical Engine, and the actual implementation code for
interfaces defined in `GRAPE-jdk`(via codegen).
