## Description
Scripts for building GraphScope docker images, for ease of deployment and
dependencies management.

It could be breaks to 4 parts:

- The images that acts like a builder, contains dependencies of all components and image distribution.
- The images of each components in GraphScope
- The image of GraphScope-Store
- The utility images, such as datasets and jupyter.

