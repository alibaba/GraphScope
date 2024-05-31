install_libgrape_lite() {
    pushd /tmp
    git clone https://github.com/alibaba/libgrape-lite.git
    cd libgrape-lite
    git checkout v0.3.2
    mkdir build && cd build && cmake .. -DBUILD_LIBGRAPELITE_TESTS=OFF
    make -j ${parallelism} && sudo make install
    sudo cp /usr/local/lib/libgrape-lite.so /usr/lib/libgrape-lite.so
    popd && rm -rf /tmp/libgrape-lite
}