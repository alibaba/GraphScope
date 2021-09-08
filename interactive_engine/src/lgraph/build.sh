set -e

MODE=$1

mkdir -p build

if [ "$MODE" = "debug" ]; then
    mkdir -p build/debug
    cd build/debug
    cmake -DCMAKE_BUILD_TYPE=Debug ../.. && make
    cd ../..
elif [ "$MODE" = "release" ]; then
    mkdir -p build/release
    cd build/release
    cmake -DCMAKE_BUILD_TYPE=Release ../.. && make
    cd ../..
else
    exit 1
fi

if [ "$(uname -s)" = "Darwin" ]; then
    SUFFIX="dylib"
else
    SUFFIX="so"
fi

ln -sf `pwd`/build/${MODE}/liblgraph.${SUFFIX} ./build/liblgraph.${SUFFIX}
