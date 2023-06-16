########################################################
# Clone or update the latest test datasets from github.
# Arguments:
#   None
########################################################
function get_test_data() {
  if [[ -z ${test_dir} ]]; then
    test_dir="/tmp/gstest"
  fi
  echo ${test_dir}
  if [[ -d ${test_dir} ]]; then
    cd ${test_dir}
    git pull
    cd -
  else
    git clone -b master --single-branch --depth=1 https://github.com/7br/gstest.git ${test_dir}
  fi
}


#get_test_data
#unzip graph data
path_to_validator=../build/tests/gs_tests/validator
path_to_loader=../build/tests/gs_tests/graph_db_loader
path_to_input_data=./sf01/
path_to_graph_data=../build/gstest/flex/ldbc-sf01-long-date/

# load graph to tmp directory.
GLOG_v=10 ${path_to_loader} ../engines/hqps/config/sf0.1-grape.yaml /tmp/sf01-graph-dir 1

# then run validator
GLOG_v=10 ${path_to_validator} ${path_to_input_data} /tmp/sf01-graph-dir