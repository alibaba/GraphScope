FROM gaia-ir

# build compiler
RUN cd /opt/GraphScope/interactive_engine/compiler && \
    make build && \
    chmod 777 set_properties.sh

CMD cd /opt/GraphScope/interactive_engine/compiler && \
    make run graph.schema:=../executor/ir/core/resource/modern_schema.json