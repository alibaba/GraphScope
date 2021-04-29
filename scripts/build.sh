#!/usr/bin/env bash
#
# A script to build GraphScope standalone.

set -e
set -x
set -o pipefail

graphscope_src="$( cd "$(dirname "$0")/.." >/dev/null 2>&1 ; pwd -P )"

function install_libgrape-lite() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') build and install libgrape-lite"
  export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib
  git clone -b master --single-branch --depth=1 https://github.com/alibaba/libgrape-lite.git /tmp/libgrape-lite
  pushd /tmp/libgrape-lite
  mkdir build && cd build
  cmake ..
  make -j`nproc`
  sudo make install
  popd
  rm -fr /tmp/libgrape-lite
}

function install_vineyard() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') build and install vineyard"
  git clone -b v0.2.1 --single-branch --depth=1 https://github.com/alibaba/libvineyard.git /tmp/libvineyard
  pushd /tmp/libvineyard
  git submodule update --init
  mkdir build && pushd build
  cmake .. -DBUILD_VINEYARD_PYPI_PACKAGES=ON -DBUILD_SHARED_LIBS=ON -DBUILD_VINEYARD_IO_OSS=ON
  make -j`nproc`
  make vineyard_client_python -j`nproc`
  sudo make install
  popd
  python3 setup.py bdist_wheel
  pip3 install -U ./dist/*.whl
  popd
  rm -fr /tmp/libvineyard
}

function build_graphscope_gae() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') build and install graphscope analytical engine"
  # build GraphScope GAE
  cd ${graphscope_src}
  mkdir analytical_engine/build && pushd analytical_engine/build
  cmake ..
  make -j`nproc`
  sudo make install
  popd
}

function build_graphscope_gie() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') build and install graphscope interactive engine"
  # build GraphScope GIE
  source ~/.cargo/env
  cd ${graphscope_src}
  # build frontend coordinator graph-manager
  pushd interactive_engine
  mvn clean package -DskipTests -Pjava-release --quiet
  popd
  # build executor
  pushd interactive_engine/src/executor
  cargo build --all --release
  popd
  # copy dependencies into GRAPHSCOPE_PREFIX
  mkdir -p ${GRAPHSCOPE_PREFIX}/pid ${GRAPHSCOPE_PREFIX}/logs
  # copy mvn package
  cp ./interactive_engine/src/instance-manager/target/0.0.1-SNAPSHOT.tar.gz ${GRAPHSCOPE_PREFIX}/0.0.1-instance-manager-SNAPSHOT.tar.gz
  cp ./interactive_engine/src/assembly/target/0.0.1-SNAPSHOT.tar.gz ${GRAPHSCOPE_PREFIX}/0.0.1-SNAPSHOT.tar.gz
  tar -xf ${GRAPHSCOPE_PREFIX}/0.0.1-SNAPSHOT.tar.gz -C ${GRAPHSCOPE_PREFIX}/
  tar -xf ${GRAPHSCOPE_PREFIX}/0.0.1-instance-manager-SNAPSHOT.tar.gz -C ${GRAPHSCOPE_PREFIX}/
  # coordinator
  mkdir -p ${GRAPHSCOPE_PREFIX}/coordinator
  cp -r ./interactive_engine/src/coordinator/target ${GRAPHSCOPE_PREFIX}/coordinator/
  # frontend
  mkdir -p ${GRAPHSCOPE_PREFIX}/frontend/frontendservice
  cp -r ./interactive_engine/src/frontend/frontendservice/target ${GRAPHSCOPE_PREFIX}/frontend/frontendservice/
  # executor
  mkdir -p ${GRAPHSCOPE_PREFIX}/conf
  cp ./interactive_engine/src/executor/target/release/executor ${GRAPHSCOPE_PREFIX}/bin/executor
  cp ./interactive_engine/src/executor/store/log4rs.yml ${GRAPHSCOPE_PREFIX}/conf/log4rs.yml
}

function build_graphscope_gle() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') build and install graphscope learning engine"
  cd ${graphscope_src}
  git submodule update --init
  pushd learning_engine/graph-learn
  git submodule update --init third_party/pybind11
  mkdir cmake-build && pushd cmake-build
  cmake .. -DWITH_VINEYARD=ON -DTESTING=OFF
  make -j`nproc`
  sudo make install
  popd
}

function install_client_and_coordinator() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') install graphscope coordinator and client"
  # install GraphScope client
  export WITH_LEARNING_ENGINE=ON
  cd ${graphscope_src}
  pushd python
  pip3 install -U setuptools
  pip3 install -r requirements.txt -r requirements-dev.txt
  python3 setup.py bdist_wheel
  pip3 install -U ./dist/*.whl
  popd

  # install GraphScope coordinator
  pushd coordinator
  pip3 install -r requirements.txt -r requirements-dev.txt
  python3 setup.py bdist_wheel
  pip3 install -U ./dist/*.whl
  popd
}

install_libgrape-lite

install_vineyard

build_graphscope_gae

build_graphscope_gie

build_graphscope_gle

install_client_and_coordinator

echo "The script has successfully builded GraphScope."
echo "Now you are ready to have fun with GraphScope."

set +x
set +e
set +o pipefail
