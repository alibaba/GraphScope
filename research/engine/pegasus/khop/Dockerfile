FROM rust:1.55.0
WORKDIR /usr/src/pegasus/
COPY .. .
RUN rm -f Cargo.lock && \
    cd ./khop && rm -f Cargo.lock && \
    cd .. && cp ./crates-io.config /usr/local/cargo/config && \
    cd ./khop && cargo install --path . && rm -rf target
CMD ["pegasus-khop"]