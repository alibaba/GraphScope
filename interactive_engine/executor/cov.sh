#!/bin/sh
# Copyright 2020 Alibaba Group Holding Limited.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

BASE_DIR="`dirname "$0"`"
BASE_DIR=`cd ${BASE_DIR}; pwd`
TMP_LOG=${BASE_DIR}/tmp.log
ALL_LOG=${BASE_DIR}/all.log
REPORT=${BASE_DIR}/report.html

function module_prefix {
    module=$1
    head -5  ${module}/Cargo.toml  | grep name | cut -d '"' -f 2 | sed "s#-#_#g"
}

function run_module {
    module=$1
    ut_bin_prefix=`module_prefix ${module}`
    ut_bin=`find target/debug/ -regextype egrep -maxdepth 1 -regex ".*/${ut_bin_prefix}-[^.]+" | xargs ls -t | head -1`
    echo "module: ${module}, ut binary: ${ut_bin}"
    rm -f ${TMP_LOG}
    cd ${BASE_DIR}/${module}
    kcov --verify \
         --exclude-pattern=/.cargo,/usr/lib,/usr/include,/proto,/target/,/apsara/alicpp  \
         ${BASE_DIR}/target/cov ${BASE_DIR}/${ut_bin} \
         --logfile ${TMP_LOG}
    # It's treated as error only when module's unit test exits abnormally,
    # otherwise it runs successfully no matter tests passed or failed.
    # For the 126 maigc, see: https://www.gnu.org/software/bash/manual/bash.html#Exit-Status
    if [ "$?" -ge 126 ]; then
        echo "Module's unit test exits abnormally, maybe killed with some signal."
        exit 1
    fi
}

function print_result {
    cd ${BASE_DIR}
    passed_cnt=`grep '^ok ' ${ALL_LOG} -c`
    failed_cnt=`grep '^failed ' ${ALL_LOG} -c`
    ignored_cnt=`grep '^ignored ' ${ALL_LOG} -c`

    echo "TEST_CASE_AMOUNT: {\"blocked\":0,\"passed\":${passed_cnt},\"failed\":${failed_cnt},\"skipped\":${ignored_cnt}}"

    cov_report_file="target/cov/kcov-merged/coverage.json"
    if [[ ! -f "${cov_report_file}" ]]; then
        cov_report_file=`find target/cov/ -name coverage.json | head -n 1`
    fi
    covered_lines=`grep covered_lines ${cov_report_file}  | grep -v file | awk '{print $2}' | sed 's#,##g'`
    total_lines=`grep total_lines ${cov_report_file}  | grep -v file | awk '{print $2}' | sed 's#,##g'`

    echo "CODE_COVERAGE_LINES: ${covered_lines}/${total_lines}"
    echo "CODE_COVERAGE_EN_NAME_LINES: Line"
    echo "CODE_COVERAGE_NAME_LINES: 行"

    echo "==============================="
    echo "            FINISH             "
    echo "==============================="
}


function show_header {
    echo "<!DOCTYPE html>
<html>
<head>
    <title>Rust Test Case Report</title>
    <style type="text/css">
    table {
        border-collapse: collapse;
        width: 100%;
    }
    th, td {
        text-align: left;
        padding: 8px;
        border-bottom: 1px solid #ddd;
    }
    tr:hover {background-color:#f5f5f5;}
    th {
        background-color: #4CAF50;
        color: white;
    }
    td.title { text-align: center; padding-bottom: 10px; font-size: 20pt; font-weight: bold; }
    td.ok {}
    td.ignored { background-color:#F5C045; }
    td.failed { background-color:#CD2020; }
</style>
</head>
<body>
    <table width="100%" border="0" cellspacing="0" cellpadding="0">
        <tr>
            <td class="title">Rust Test Case Report</td>
        </tr>
        <tr>
            <th>Case Name</th>
            <th>Result</th>
        </tr>"
}

function show_footer {
    echo "</table>
</body>
</html>"
}

function show_data {
    log_file=$1
    awk '{printf("<tr><td>%s</td><td class=\"%s\">%s</td></tr>\n", $2, $1, $1);}' ${log_file}
}

function generate_report {
    show_header
    show_data $1
    show_footer
}

function upload_report {
    file=$1
    ts=`date '+%Y_%m_%d_%H_%M_%S'`
    oss_url="oss://graphcompute/reports/${ts}_report.html"
    osscmd put ${file} ${oss_url}
    url=`osscmd signurl ${oss_url} --timeout=31536000 2>1 | grep http`
    echo "TEST_REPORT: ${url}"
}

function install_kcov {
    sudo yum install cmake zlib-devel libcurl-devel binutils-devel elfutils-devel elfutils-libelf-devel -y
    wget https://github.com/SimonKagstrom/kcov/archive/v33.tar.gz
    tar zxf v33.tar.gz
    cd kcov-33
    cmake .
    make
    sudo make install
}

function run {
    rm -fr target/cov
    RUSTFLAGS='-C link-dead-code' cargo test --all --no-run
    if [[ $? -ne 0 ]]; then
        echo "Compile failed"
        exit 1
    fi
    rm -f ${ALL_LOG}
    for m in "$@"; do
        cd ${BASE_DIR}
        run_module $m
        cat ${TMP_LOG} >> ${ALL_LOG}
        rm -f ${TMP_LOG}
    done
}

function usage {
    echo "Usage: "
    echo "    ./cov --install-deps"
    echo "    ./cov module_a module_b"
}

if [[ "$1" == "" ]]; then
    usage
elif [[ "$1" == "--install-deps" ]]; then
    install_kcov
else
    run $@
    print_result
    generate_report ${ALL_LOG} > ${REPORT}
    upload_report ${REPORT}
fi

