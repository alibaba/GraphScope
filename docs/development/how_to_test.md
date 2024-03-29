# How to Test

GraphScope is comprised of three engines targeting for different business scenario, and a coordinator that could bring them together, and a client for users to connect and perform various tasks.

We already covered how to test for engines in

- [Build and test analytical engine](../analytical_engine/dev_and_test.md#how-to-test)
- [Build and test interactive engine](../interactive_engine/dev_and_test.md#how-to-test)
- [Build and test learning engine](../interactive_engine/dev_and_test.md#how-to-test)

Here in this guide we will focus on how to do the e2e test, where multiple engines will working together.

## Test GraphScope on local

### Dev Environment

Here we would use a prebuilt docker image with necessary dependencies installed.

```bash
docker run --name dev -it --shm-size=4096m registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:latest
```

Please refer to [Dev Environment](../development/dev_guide.md#dev-environment) to find more options to get a dev environment.

### Build All Targets for GraphScope

With `gs` command-line utility, you can build all targets for GraphScope with a single command.

```bash
python3 gsctl.py make install
```

### Run tests

Run a bunch of test cases that involves 3 engines
```bash
python3 gsctl.py test e2e --local
```

## Test GraphScope on Kubernetes cluster

Make sure you have [docker](https://www.docker.com) installed.

### Build images

```bash
python3 gsctl.py make-image all
```

Executing this command will generate the corresponding images for each component.

It would produce a couple of images like this

:::{figure-md}

<img src="../images/gs-images.png"
     alt="GraphScope Images"
     width="80%">

Images of GraphScope
:::    

Or you could choose to generate image for a specific component. For example

```bash
python3 gsctl.py make-image analytical
```

Try `python3 gsctl.py make-image -h` for more available options.

### Prepare a Kubernetes cluster

Please refer to [Prepare a kubernetes cluster](../deployment/deploy_graphscope_on_self_managed_k8s.md#prepare-a-kubernetes-cluster) to get a cluster if you doesn't have one.

### Run tests

The tests would launch a couple of pods on your cluster, so you need to set appropriate environment variables to let it use your newly built image.

Take our previous built image for example, which has an empty registry (or you could say it's `docker.io` by default), and tagged `latest`

```bash
python3 gsctl.py test e2e --k8s --registry="docker.io" --tag="latest"
```
