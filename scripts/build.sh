#!/usr/bin/env bash
#
# A script to build GraphScope standalone.

set -e
set -x
set -o pipefail

graphscope_home="$( cd "$(dirname "$0")/.." >/dev/null 2>&1 ; pwd -P )"

function install_libgrape-lite() {
  export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib
  git clone -b master --single-branch --depth=1 https://github.com/alibaba/libgrape-lite.git /tmp/libgrape-lite
  pushd /tmp/libgrape-lite
  mkdir build && cd build
  cmake ..
  make -j`nproc`
  sudo make install
  popd
}

function install_vineyard() {
  export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib
  git clone -b v0.1.14 --single-branch --depth=1 https://github.com/alibaba/libvineyard.git /tmp/libvineyard
  pushd /tmp/libvineyard
  git submodule update --init
  mkdir build && cd build
  cmake .. -DBUILD_VINEYARD_PYPI_PACKAGES=ON -DBUILD_SHARED_LIBS=ON -DBUILD_VINEYARD_IO_OSS=ON
  make -j`nproc`
  make vineyard_client_python -j`nproc`
  sudo make install
  python3 setup.py bdist_wheel
  pip3 install ./dist/*.whl
  popd
}

function build_graphscope_gae() {
  # build GraphScope GAE
  export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib
  cd ${graphscope_home}
  mkdir analytical_engine/build && pushd analytical_engine/build
  cmake ..
  make -j`nproc`
  sudo make install
  popd
}

function build_graphscope_gie() {
  # build GraphScope GIE
  export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib
  export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
  export PATH=${JAVA_HOME}/bin:${PATH}:/usr/local/go/bin:/usr/local/zookeeper/bin:/usr/share/maven/bin
  export GRAPHSCOPE_PREFIX=/tmp/graphscope_prefix
  # build frontend coordinator graph-manager
  cd ${graphscope_home}
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

function install_client_and_coordinator() {
  # install GraphScope client
  export WITH_LEARNING_ENGINE=ON
  export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib
  cd ${graphscope_home}
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

install_dependencies

install_libgrape-lite

install_vienyard

build_graphscope_gae

build_graphscope_gie

install_client_and_coordinator

echo "The script has successfully builded GraphScope."
echo "Now you are ready to have fun with GraphScope."

set +x
set +e
set +o pipefail
