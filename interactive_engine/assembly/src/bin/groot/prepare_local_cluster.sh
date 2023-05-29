#!/usr/bin/env bash

SOURCE_DIR="$(dirname -- "$(readlink -f "${BASH_SOURCE}")")"

cp ~/.m2/repository/org/apache/curator/curator-test/5.4.0/curator-test-5.4.0.jar ${SOURCE_DIR}/lib/
cp ~/.m2/repository/io/dropwizard/metrics/metrics-core/4.2.18/metrics-core-4.2.18.jar ${SOURCE_DIR}/lib/
cp ~/.m2/repository/org/scala-lang/scala-library/2.13.9/scala-library-2.13.9.jar ${SOURCE_DIR}/lib

pip3 install graphscope_client --user


sed 's@LOG4RS_CONFIG@./conf/log4rs.yml@' ${SOURCE_DIR}/config.template ${SOURCE_DIR}/groot.config

export NODE_IP="127.0.0.1"
export GREMLIN_PORT="12312"
export GRPC_PORT="55556"

${SOURCE_DIR}/bin/store_ctl.sh start