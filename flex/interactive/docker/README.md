# Build Interactive Docker Image

## Build Base Image

```bash
docker build -f interactive-base.Dockerfile -t interactive-base .
```

## Build Runtime Image with latest GraphScope

```bash
docker build -f interactive-base.Dockerfile --target final_image -t interactive .
```

## Tag the image

```bash
docker tag interactive:latest registry.cn-hongkong.aliyuncs.com/graphscope/interactive:{version}
```