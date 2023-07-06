# Command-line Utility `gsctl`

`gsctl` is a command-line utility for GraphScope. It is shipped with `graphscope-client` and provides a set of functionalities to make it easy to use GraphScope. These functionalities include building and testing binaries, managing sessions and resources, and more.

## Install/Update `gsctl`

Since it is shipped with python package `graphscope-client`, the `gsctl` command will be available in your terminal after installing GraphScope:
```bash
pip install graphscope-client
```

In some cases, such as development on `gsctl`, you may want to build it from source. 
To do this, navigate to the directory where the source code is located and run the following command:

```bash
cd REPO_HOME/python
# If you want to develop gsctl, 
# please note the entry point is located on: 
# /python/graphscope/gsctl/gsctl.py
pip install --editable .
```
This will install `gsctl` in an editable mode, which means that any changes you make to the source code will be reflected in the installed version of `gsctl`.

## Commands

With `gsctl`, you can do the following things. Always remember to
 use `--help` on a command to get more information.

- `gsctl install-deps`, install dependencies for building GraphScope.
- `gsctl make`, build GraphScope executable binaries and artifacts.
- `gsctl make-image`, build GraphScope docker images.
- `gsctl test`, trigger test suites.

