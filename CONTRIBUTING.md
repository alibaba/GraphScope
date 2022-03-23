Contributing to GraphScope
==========================

GraphScope has been developed by an active team of software engineers and
researchers. Any contributions from the open-source community to improve this
project are welcome!

GraphScope is licensed under [Apache License 2.0][1].

Newcomers to GraphScope
-----------------------

For newcomers to GraphScope, you could find instructions about how to build
and run applications using GraphScope in [README][2].

GraphScope is hosted on Github, and use Github issues as the bug tracker.
you can [file an issue][3] when you meets trouble when working with GraphScope.

Before creating a new bug entry, we recommend you first [search][4] among existing
GraphScope bugs to see if it has already been resolved.

When creating a new bug entry, please provide necessary information of your
problem in the description , such as operating system version, GraphScope
version, and other system configurations to help us diagnose the problem.

We also welcome any help on GraphScope from the community, including but not
limited to fixing bugs and adding new features. Note that you will be required to
sign the [CLA][5] before submitting patches to us.

Documentation
-------------

GraphScope documentation is generated using Doxygen and sphinx. Users can build
the documentation in the build directory using:

    make graphscope-docs

The HTML documentation will be available under `docs/_build/html`:

    open docs/index.html

The latest version of online documentation can be found at [https://graphscope.io/docs][6].

GraphScope provides comprehensive documents to explain the underlying
design and implementation details. The documentation follows the syntax
of Doxygen and sphinx markup. If you find anything you can help, submit 
pull request to us. Thanks for your enthusiasm!

Build Python Wheels
-------------------

The GraphScope python package is built using the [manylinux2010][7] environments.
Please refer to [./docs/developer_guide.rst][8] for more detail instructions. 

Working Convention
------------------

### Code format

GraphScope follows the [Google C++ Style Guide][9] for C++ code, and the [black][10] code
style for Python code. When submitting patches to GraphScope, please format your code
with clang-format by the Makefile command `make graphscope_clformat`, and make sure
your code doesn't break the cpplint convention using the Makefile command `make graphscope_cpplint`.

### Open a pull request

When opening issues or submitting pull requests, we'll ask you to prefix the
pull request title with the issue number and the kind of patch (`BUGFIX` or `FEATURE`)
in brackets, for example, `[BUGFIX-1234] Fix bug in SSSP on property graph`
or `[FEATURE-2345] Support loading empty graphs`.

### Git workflow for newcomers

You generally do NOT need to rebase your pull requests unless there are merge
conflicts with the main. When Github complaining that "Can’t automatically merge"
on your pull request, you'll be asked to rebase your pull request on top of
the latest main branch, using the following commands:

+ First rebasing to the most recent main:

        git remote add upstream https://github.com/alibaba/GraphScope.git
        git fetch upstream
        git rebase upstream/main

+ Then git may show you some conflicts when it cannot merge, say `conflict.cpp`,
  you need
  - Manually modify the file to resolve the conflicts
  - After resolved, mark it as resolved by

        git add conflict.cpp

+ Then you can continue rebasing by

        git rebase --continue

+ Finally push to your fork, then the pull request will be got updated:

        git push --force

[1]: https://github.com/alibaba/GraphScope/blob/main/LICENSE
[2]: https://github.com/alibaba/GraphScope/blob/main/README.md
[3]: https://github.com/v6d-io/v6d/issues/new/new
[4]: https://github.com/v6d-io/v6d/pulls
[5]: https://cla-assistant.io/alibaba/GraphScope
[6]: https://graphscope.io/docs
[7]: https://github.com/pypa/manylinux
[8]: ./docs/developer_guide.rst
[9]: https://google.github.io/styleguide/cppguide.html
[10]: https://github.com/psf/black
