## Description
Scripts for building GraphScope docker images, for ease of deployment and
dependencies management.

It could be breaks to 4 parts:

- The images that acts like a builder, contains dependencies of all components and image distribution.
- The images of each components in GraphScope
- The image of GraphScope-Store
- The utility images, such as datasets and jupyter.

Here's an simple illustration of what these images are used for and how are they are built.

These are four base images acts for building or as base images.
- graphscope-dev-base: Contains all dependencies for graphscope, except vineyard
- graphscope-dev: It's just graphscope-dev-base plus vineyard. We make it separated from the base is because vineyard maybe upgraded more often than other dependencies.
- vineyard-dev: Contains all dependencies for vineyard, with vineyard itself. Acts as a base image for GAE.
- vineyard-runtime: Base centOS image plus vineyard and its dependent libraries. Acts as a base image for GIE and GLE.

```
*graphscope-dev                  ---------> vineyard-runtime
     ∆                          | copy libs       ∆
     |                          |                 |
     | + vineyard               |                 |
     |                          |                 |
graphscope-dev-base         vineyard-dev       centos:7
     ∆                          ∆
     |                          |
     | + gs dependencies        |
     |                          |
manylinux2014                 centos:7

```

These are the images for each components. They are built from a dev image and the built artifacts are copied to a runtime image.

- coordinator
- analytical
- analytical-java
- interactive-frontend
- interactive-executor
- learning

This is the image contains all components in one image. It's built by `pip install graphscope`,
we keep this to illustrate how to bootstrap a basic runtime environment for the standalone graphscope (without k8s clustter).

- graphscope

These are utility images for ease of use.

- dataset: Contains a dataset hosted on Aliyun OSS. 
- jupyter: Contains a jupyter-lab which connects to the graphscope cluster.

This is the image for GraphScope-Store

- graphscope-store
