# Assembly
This will build graphscope or groot into an assembly archive.

# Usage
`mvn package -P graphscope,graphscope-assembly` will generate a graphscope.tar.gz under `target/`.
`mvn package -P groot,groot-assembly` will generate a groot.tar.gz under `target/`.