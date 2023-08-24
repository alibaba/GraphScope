set -e
set -x

MODE=$1
SKIP=$2
TARGET=$3
FEATURE=$4

if [ "$SKIP" = "true" ]; then
    exit 0
fi

if [ "$TARGET" = "v6d" ] || [ "$TARGET" = "groot" ]; then
    echo "Build target $TARGET"
else
    echo "Invalid target, choose from v6d or groot."
    exit 1
fi

if [ "$(uname -s)" = "Darwin" ]; then
    SUFFIX="dylib"
    STRIP_OPTION="-u"
else
    SUFFIX="so"
    STRIP_OPTION=""
fi

if [ -z "$FEATURE" ]; then
  append="--features=$FEATURE"
else
  append=""
fi


cd assembly/$TARGET;
if [ "$MODE" = "debug" ]; then
  cargo build $append
elif [ "$MODE" = "release" ]; then
  cargo build --release $append
else
  echo "Invalid mode, choose from debug or release."
  exit 1
fi

if [ "$TARGET" = "groot" ]; then
  strip ${STRIP_OPTION} $(pwd)/target/${MODE}/libgroot_ffi.${SUFFIX}
  ln -sf $(pwd)/target/${MODE}/libgroot_ffi.${SUFFIX} $(pwd)/target/libgroot_ffi.${SUFFIX}
fi
