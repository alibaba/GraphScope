# GraphScope Interactive SDK

GraphScope Interactive use [OpenAPI](https://openapis.org) to define the http interface, and use [OpenAPI-generator](https://openapi-generator.tech) to generate python/java SDKs.

## Usage

## Generate SDK by yourself

### Install OpenAPI-generator-cli

```bash
mkdir -p ~/bin/openapitools
curl https://raw.githubusercontent.com/OpenAPITools/openapi-generator/master/bin/utils/openapi-generator-cli.sh > ~/bin/openapitools/openapi-generator-cli
chmod u+x ~/bin/openapitools/openapi-generator-cli
export PATH=$PATH:~/bin/openapitools/
export OPENAPI_GENERATOR_VERSION=7.2.0
```

### Generate SDKs

[Generate Python SDK](./python/README.md)
[Generate Java SDK](./java//README.md)