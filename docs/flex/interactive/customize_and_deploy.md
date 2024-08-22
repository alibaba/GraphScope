# Customize and Deploy

This document explains how to package Interactive from the source and deploy it locally. This is useful for deploying a customized version of Interactive.

## Packing Interactive from Source

Interactive is packaged and delivered via a Docker image. To build the image, ensure you have [Docker](https://www.docker.com/) installed on your machine. Next, clone the repository and build the Interactive Runtime Image.

```bash
git clone https://github.com/alibaba/GraphScope.git # Or clone your forked repo
cd GraphScope/k8s
python3 ./gsctl.py flexbuild interactive --app docker
```

The build process may take some time. Once completed, you will find the image tagged as `graphscope/interactive:latest`.

```{note}
Docker images are platform-specific, meaning an image built on x86_64 (amd64) may not run on arm64 machines. While it can execute on other platforms with QEMU support, performance will be significantly slower. To create a multi-platform image, use [Buildx](https://docs.docker.com/reference/cli/docker/buildx/) or [Docker-Manifest](https://www.docker.com/blog/multi-arch-build-and-images-the-simple-way/).
```

## Deploying Interactive

`gsctl` natively supports deploying an Interactive instance with a customized image.

```bash
python3 gsctl.py instance deploy --type interactive --image-registry graphscope --image-tag latest
```

## Pushing to Your Own Registry

You can push the image to your own registry for access from other machines.

```bash
docker tag graphscope/interactive:latest {YOUR_IMAGE_REGISTRY}/interactive:{TAG}
docker push {YOUR_IMAGE_REGISTRY}/interactive:latest
python3 gsctl.py instance deploy --type interactive --image-registry {YOUR_IMAGE_REGISTRY} --image-tag {TAG}
```

## Connecting and Using

Refer to [Getting Started](./getting_started.md) for instructions on using Interactive. For detailed usage, consult the [Java SDK Documentation](./development/java/java_sdk.md) and [Python SDK Documentation](./development/python/python_sdk.md).
