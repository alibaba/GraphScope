set -e
set -x

MODE=$1
SKIP=$2

if [ "$SKIP" = "true" ]; then
    exit 0
fi

if [ "$MODE" = "debug" ]; then
    cargo build --all
elif [ "$MODE" = "release" ]; then
    cargo build --all --release
else
    exit 1
fi


if [ "$(uname -s)" = "Darwin" ]; then
    SUFFIX="dylib"
else
    SUFFIX="so"
fi

ln -sf `pwd`/target/${MODE}/libmaxgraph_runtime.${SUFFIX} ./target/libmaxgraph_runtime.${SUFFIX}
#ln -sf `pwd`/target/${MODE}/libmaxgraph_query.${SUFFIX} ./target/libmaxgraph_query.${SUFFIX}
