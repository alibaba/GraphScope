# graphscope-jupyter

The project structure refers to **(ipycytoscape)[https://github.com/QuantStack/ipycytoscape/tree/1.1.0]**.

A widget enabling interactive graph visualization with [Graphin](https://github.com/antvis/Graphin) in JupyterLab and the Jupyter notebook.

![graphin screencast](https://gw.alipayobjects.com/mdn/rms_f8c6a0/afts/img/A*EJvtT7KcywAAAAAAAAAAAAAAARQnAQ)

## Installation

Note that graphscope-jupyter requires jupyterlab 2.x (e.g., `2.3.0a0`) and is known unusable with jupyterlab 3.x.

With `mamba`:

```
mamba install -c conda-forge graphscope-jupyter
```

With `conda`:

```
conda install -c conda-forge graphscope-jupyter
```

With `pip`:

```bash
pip install graphscope-jupyter
```

#### For jupyterlab users:

If you are using JupyterLab 1.x or 2.x then you will also need to install `nodejs` and the `jupyterlab-manager` extension. You can do this like so:

```bash
# installing nodejs
conda install -c conda-forge nodejs


# install jupyterlab-manager extension
jupyter labextension install @jupyter-widgets/jupyterlab-manager@2

# if you have previously installed the manager you still to run jupyter lab build
jupyter lab build
```

### For Jupyter Notebook 5.2 and earlier

You may also need to manually enable the nbextension:

```bash
jupyter nbextension enable --py [--sys-prefix|--user|--system] graphscope-jupyter
```

## For a development installation:

**(requires npm)**

While not required, we recommend creating a conda environment to work in:

```bash
conda create -n graphscope -c conda-forge jupyterlab nodejs networkx
conda activate graphscope

# clone repo
git clone https://github.com/alibaba/GraphScope.git
cd GraphScope/python/jupyter/graphscope

# Install python package for development, runs npm install and npm run build
pip install -e .
```

When developing graphscope-jupyter, you need to manually enable the extension with the
notebook / lab frontend. For lab, this is done by the command:

```
# install this extension
jupyter labextension install .
```

For classic notebook, you can run:

```
jupyter nbextension install --sys-prefix --symlink --overwrite --py graphscope-jupyter
jupyter nbextension enable --sys-prefix --py graphscope-jupyter
```

Note that the `--symlink` flag doesn't work on Windows, so you will here have to run
the `install` command every time that you rebuild your extension. For certain installations
you might also need another flag instead of `--sys-prefix`, but we won't cover the meaning
of those flags here.

You need to install and build `npm` packages:

```
npm install && npm run build
```

Every time you change your typescript code it's necessary to build it again:

```
npm run build
```

### How to see your changes

#### TypeScript:

To continuously monitor the project for changes and automatically trigger a rebuild, start Jupyter in watch mode:

```bash
jupyter lab --watch
```

And in a separate session, begin watching the source directory for changes:

```bash
npm run watch
```

#### Python:

If you make a change to the python code then you need to restart the notebook kernel to have it take effect.

### How to run tests locally

Install necessary dependencies with pip:

```
cd GraphScope/python/jupyter/graphscope
pip install -e .
```

## License

We use a shared copyright model that enables all contributors to maintain the
copyright on their contributions.

This software is licensed under the Apache License 2.0. See the
[LICENSE](LICENSE) file for details.
