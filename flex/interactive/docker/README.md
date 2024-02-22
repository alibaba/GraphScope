# Build Interactive Docker Image

## Build Base Image

```bash
make interactive-base 
# will be interactive-base to a multi-platform images, amd64 and arm64, and push to the remote
```

## Build Runtime Image with latest GraphScope

```bash
# get the current commit id
commit_id=$(git rev-parse --short HEAD)
# on a arm machine
make interactive-runtime VERSION=${commit_id} # build and push to the remote 
# on a x86 machine
make interactive-runtime VERSION=${commit_id} # build and push to the remote 
```

## Create and Push Docker Manifest 

```bash
VERSION=${real_tag}
sudo docker manifest create registry.cn-hongkong.aliyuncs.com/graphscope/interactive:${VERSION} --amend registry.cn-hongkong.aliyuncs.com/graphscope/interactive:${commit_id}-x86_64 --amend registry.cn-hongkong.aliyuncs.com/graphscope/interactive:${commit_id}-aarch64

sudo docker manifest push registry.cn-hongkong.aliyuncs.com/graphscope/interactive:${VERSION}
```