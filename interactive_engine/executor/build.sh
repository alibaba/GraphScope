set -e
set -x

MODE=$1
SKIP=$2

if [ "$SKIP" = "true" ]; then
    exit 0
fi

if [ "$MODE" = "debug" ]; then
    ./exec.sh cargo build --workspace
elif [ "$MODE" = "release" ]; then
    ./exec.sh cargo build --workspace --release
else
    exit 1
fi


if [ "$(uname -s)" = "Darwin" ]; then
    SUFFIX="dylib"
else
    SUFFIX="so"
fi

rm -rf $(pwd)/target/${MODE}/build
rm -rf $(pwd)/target/${MODE}/deps
ln -sf $(pwd)/target/${MODE}/libmaxgraph_ffi.${SUFFIX} ./target/libmaxgraph_ffi.${SUFFIX}
