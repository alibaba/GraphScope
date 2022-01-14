FROM rust:1.55.0
WORKDIR /usr/src/pegasus/
COPY .. .
RUN rm -f Cargo.lock && \
    cd ./benchmark && rm -f Cargo.lock && \
    cd .. && cp ./crates-io.config /usr/local/cargo/config && \
    cd ./benchmark && cargo install --path . && rm -rf target
CMD ["service"]