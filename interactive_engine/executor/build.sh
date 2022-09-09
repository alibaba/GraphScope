set -e
set -x

MODE=$1
SKIP=$2
FEATURE=$3

if [ "$SKIP" = "true" ]; then
    exit 0
fi

if [ "$(uname -s)" = "Darwin" ]; then
    SUFFIX="dylib"
    STRIP_OPTION="-u"
else
    SUFFIX="so"
    STRIP_OPTION=""
fi

cd assembly;
if [ "$MODE" = "debug" ]; then
  ../exec.sh cargo build --workspace --features="$FEATURE"
elif [ "$MODE" = "release" ]; then
  ../exec.sh cargo build --workspace --release --features="$FEATURE"
else
  exit 1
fi

rm -rf $(pwd)/target/${MODE}/build
rm -rf $(pwd)/target/${MODE}/deps

strip ${STRIP_OPTION} $(pwd)/target/${MODE}/libmaxgraph_ffi.${SUFFIX}
ln -sf $(pwd)/target/${MODE}/libmaxgraph_ffi.${SUFFIX} ./target/libmaxgraph_ffi.${SUFFIX}
