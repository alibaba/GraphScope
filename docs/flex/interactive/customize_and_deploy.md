# Customize and Deploy

This document outlines how to pack Interactive from the source, deploy it in your local environment. This maybe useful if you want deploy a customized version of Interactive.

## Packing Interactive from Source

Interactive is packed and delivered via Docker image. To build the Image of Interactive, you need a machine with [docker](https://www.docker.com/) installed. 
Then clone the code and build with the Interactive Runtime Image.

```bash
git clone https://github.com/alibaba/GraphScope.git # Or clone your forked repo
cd GraphScope/k8s
python3 ./gsctl.py flexbuild interactive --app docker
```

This will take some time. Then you should be able to find the built image like `graphscope/interactive:latest`. You could tag the image with your desired registry and image, tag names.

```{note}
Note that docker image are bound to platforms, which means you image you build on x86_64(amd64) may not be able to run on a arm64 machine. 
Even if the image could be run on other platforms with the support of QEMU, it will be extremely slow. 
If you want to build a multiple-platform image, you may make use of [Buildx](https://docs.docker.com/reference/cli/docker/buildx/) or [Docker-Manifest](https://www.docker.com/blog/multi-arch-build-and-images-the-simple-way/).
```

## Deploying Interactive

`gsctl` natively support deploying interactive instance with customized image. You just need to specify the registry, image name and tag.

```bash
python3 gsctl.py instance deploy --type interactive --image-registry {YOUR_IMAGE_REGISTRY} --image-tag {IMAGE_TAG}
```

## Connecting and Using

Refer to [Getting Started](./getting_started.md) for instructions on using Interactive. For detailed usage, consult the [Java SDK Documentation](./development/java/java_sdk.md) and [Python SDK Documentation](./development/python/python_sdk.md).
