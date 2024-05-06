# GraphScope Interactive SDK

GraphScope Interactive use [OpenAPI](https://openapis.org) to define the http interface, and use [OpenAPI-generator](https://openapi-generator.tech) to generate python/java SDKs.

## Usage

To use Interactive Python SDK
```bash
pip3 install interactive-sdk==0.0.3
```

To use Interactive Java SDK
```xml
<dependency>
    <groupId>com.alibaba.graphscope</groupId>
    <artifactId>interactive-java-sdk</artifactId>
    <version>0.0.3</version>
</dependency>
```

## Docs

[GraphScope Interactive Java SDK](https://graphscope.io/docs/flex/interactive/java_sdk)
[GraphScope Interactive Python SDK](https://graphscope.io/docs/flex/interactive/python_sdk)


## Generate SDK by yourself

The generated SDKs are not contained in the github codebase, you maybe interested in generated code.

### Install OpenAPI-generator-cli

```bash
mkdir -p ~/bin/openapitools
curl https://raw.githubusercontent.com/OpenAPITools/openapi-generator/master/bin/utils/openapi-generator-cli.sh > ~/bin/openapitools/openapi-generator-cli
chmod u+x ~/bin/openapitools/openapi-generator-cli
export PATH=$PATH:~/bin/openapitools/
export OPENAPI_GENERATOR_VERSION=7.2.0
```

### Generate SDKs

```bash
# generate java sdk to java/interactive_sdk/
bash generate_sdk.sh -g java
# generate python sdk to python/interactive_sdk/
bash generate_sdk.sh -g python
```

## SDK examples.