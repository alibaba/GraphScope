#!/bin/bash
set -e

# start a local etcd server
#   $ /tmp/etcd-download-test/etcd
# write,read to etcd
#   $ /tmp/etcd-download-test/etcdctl --endpoints=localhost:2379 put foo bar
#   $ /tmp/etcd-download-test/etcdctl --endpoints=localhost:2379 get foo

ETCD_VER=${1:-v3.4.13}

UNAME="$(uname | awk '{print tolower($0)}')"

GOOGLE_URL=https://storage.googleapis.com/etcd
GITHUB_URL=https://github.com/etcd-io/etcd/releases/download
DOWNLOAD_URL=${GITHUB_URL}

FILE_EXT=".zip"
if [[ $UNAME == "linux" ]]; then FILE_EXT=".tar.gz"; fi

rm -f /tmp/etcd-${ETCD_VER}-${UNAME}-amd64${FILE_EXT}
rm -rf /tmp/etcd-download-test && mkdir -p /tmp/etcd-download-test

curl -L ${DOWNLOAD_URL}/${ETCD_VER}/etcd-${ETCD_VER}-${UNAME}-amd64${FILE_EXT} -o /tmp/etcd-${ETCD_VER}-${UNAME}-amd64${FILE_EXT}
if [[ "$UNAME" == "linux" ]]; then
  tar xzvf /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz -C /tmp/etcd-download-test --strip-components=1
  rm -f /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz
elif [[ "$UNAME" == "darwin" ]]; then
  unzip /tmp/etcd-${ETCD_VER}-darwin-amd64.zip -d /tmp && rm -f /tmp/etcd-${ETCD_VER}-darwin-amd64.zip
  mv /tmp/etcd-${ETCD_VER}-darwin-amd64/* /tmp/etcd-download-test && rm -rf mv /tmp/etcd-${ETCD_VER}-darwin-amd64
fi
