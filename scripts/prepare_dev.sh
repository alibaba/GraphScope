#!/usr/bin/env bash
#
# A script to install dependencies for GraphScope dev user

set -x
set -e
set -o pipefail

platform=$(awk -F= '/^NAME/{print $2}' /etc/os-release)

##########################
# Install dependencies for make image, doc, and test.
# Globals:
#   platform
# Arguments:
#   None
##########################
function install_dependencies() {
  if [[ "${platform}" == *"Ubuntu"* ]]; then
    sudo apt-get update -y
    sudo apt-get install -y make doxygen || true # For make image and doc
    sudo apt-get install -y maven openjdk-8-jre  # For interactive engine test
    sudo apt-get clean
  elif [[ "${platform}" == *"CentOS"* ]]; then
    sudo yum update -y
    sudo yum install -y make doxygen || true # For make image and doc
    sudo yum install -y maven java-1.8.0-openjdk-devel  # For interactive engine test
    if [[ x"`rpm --eval '%{centos_ver}'`" == x"8" ]]; then
      # install doxygen
      dnf config-manager --set-enabled powertools || true
      dnf -y install doxygen || true
    fi
    sudo yum clean all
  else
    echo "Only support Ubuntu and CentOS"
    exit 1
  fi
  pip3 install -U pip --user
  pip3 install pytest --user
}

install_dependencies

set +x
set +e
set +o pipefail
