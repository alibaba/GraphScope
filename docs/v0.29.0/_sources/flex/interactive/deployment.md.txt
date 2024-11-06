# Deployment

## Deploy with Docker

Interactive is packaged as a Docker image and can be easily deployed with `docker`, `python3 >=3.8`, and `pip >=19.3` installed on your system. You will use a command-line tool called `gsctl` to install, start, and manage Interactive services.

```bash
pip3 install gsctl
# Deploy the interactive service in local mode
gsctl instance deploy --type interactive 
```

### Customizing Ports

By default, Interactive launches various services on these ports: Coordinator Service on `8080`, Interactive Meta Service on `7777`, Interactive Cypher service on `7687`, and Stored Procedure Service on `10000`. These ports can be customized.

```bash
gsctl instance deploy --type interactive --coordinator-port 8081 --admin-port 7778 \ 
                --cypher-port 7688 --storedproc-port 10001
```

For more details on customizing ports and connecting to the Interactive Service, refer to [Installation](./installation.md) and [Getting Started](./getting_started.md).

By default, `gsctl` uses the same version for both the tool and Interactive. For customized deployment, follow the instructions in [Deploy From Source Code](#deploy-from-source-code).

## Deploy with Helm

TODO

## Deploy from Source Code

This section describes how to package and deploy Interactive from the source for a customized version.

### Packing Interactive from Source

Interactive is packaged as a Docker image. Ensure you have [Docker](https://www.docker.com/) installed, then clone the repository and build the Interactive Runtime Image.

```bash
git clone https://github.com/alibaba/GraphScope.git # Or clone your fork
cd GraphScope/k8s
python3 ./gsctl.py flexbuild interactive --app docker
```

The build process will take some time. Upon completion, the image will be tagged as `graphscope/interactive:latest`.

```{note}
Docker images are platform-specific, meaning an image built on x86_64 (amd64) may not run on arm64 machines. While it can run on other platforms with QEMU support, it will be slower. To create multi-platform images, use [Buildx](https://docs.docker.com/reference/cli/docker/buildx/) or [Docker-Manifest](https://www.docker.com/blog/multi-arch-build-and-images-the-simple-way/).
```

### Deploying Customized Built Interactive

`gsctl` supports deploying an Interactive instance with a customized image.

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
