# Build Interactive Docker Image

## Build Base Image

```bash
#docker build -f interactive-base.Dockerfile -t interactive-base .
docker buildx build --platform linux/amd64,linux/arm64 -f interactive-base.Dockerfile -t registry.cn-hongkong.aliyuncs.com/graphscope/interactive-base --push .
```

## Build Runtime Image with latest GraphScope

```bash
#docker build -f interactive-runtime.Dockerfile --target final_image -t interactive .
docker buildx build --platform linux/amd64,linux/arm64 -f interactive-runtime.Dockerfile --target final_image -t interactive --no-cache .
```

## Tag the image

```bash
docker tag interactive:latest registry.cn-hongkong.aliyuncs.com/graphscope/interactive:{version}
```
