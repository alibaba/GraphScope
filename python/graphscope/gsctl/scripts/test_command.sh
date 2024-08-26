script_dir="$(cd "$(dirname "$0")" && pwd)"
source ${script_dir}/lib/get_os_version.sh
source ${script_dir}/lib/log.sh
source ${script_dir}/lib/colors.sh
source ${script_dir}/lib/install_thirdparty_dependencies.sh
source ${script_dir}/lib/install_vineyard.sh
source ${script_dir}/lib/util.sh
source ${script_dir}/initialize.sh


while getopts ":t:d:l:s:k:n:" opt; do
  case $opt in
    t)
      type=$OPTARG
      ;;
    d)
      testdata=$OPTARG
      ;;
    l)
      on_local=$OPTARG
      ;;
    s)
      storage_type=$OPTARG
      ;;
    k)
      on_k8s=$OPTARG
      ;;
    n)
      nx=$OPTARG
      ;;
    :)
      echo "Invalid options: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "The option -$OPTARG requires parameters" >&2
      exit 1
      ;;
     esac
done

export GS_TEST_DIR=${testdata}

# analytical, analytical-java, interactive, learning, e2e, groot

GS_SOURCE_DIR="$(dirname -- "$(readlink -f "${BASH_SOURCE}")")"


function get_test_data {
	if [[ ! -d ${GS_TEST_DIR} ]]; then
		log "Downloading test data to ${testdata}"
		git clone -b master --single-branch --depth=1 https://github.com/graphscope/gstest.git "${GS_TEST_DIR}"
	fi
}

function test_analytical {
	get_test_data
	info "Testing analytical on local"
	"${GS_SOURCE_DIR}"/analytical_engine/test/app_tests.sh --test_dir "${GS_TEST_DIR}"
}

function test_analytical-java {
	get_test_data
	info "Testing analytical-java on local"

	pushd "${GS_SOURCE_DIR}"/analytical_engine/java || exit
	mvn test -Dmaven.antrun.skip=true
	popd || exit

	version=$(cat "${GS_SOURCE_DIR}"/VERSION)
	export RUN_JAVA_TESTS=ON
	export USER_JAR_PATH="${GS_SOURCE_DIR}"/analytical_engine/java/grape-demo/target/grape-demo-${version}-shaded.jar
	# for giraph test
	export GIRAPH_JAR_PATH="${GS_SOURCE_DIR}"/analytical_engine/java/grape-giraph/target/grape-giraph-${version}-shaded.jar

	"${GS_SOURCE_DIR}"/analytical_engine/test/app_tests.sh --test_dir "${GS_TEST_DIR}"
}

function test_interactive {
	get_test_data
	if [ "${on_local}" == "True" ]; then
		if [[ ${storage_type} = "experimental" ]]; then
			info "Testing interactive on local with experimental storage"
			# IR unit test
			cd "${GS_SOURCE_DIR}"/interactive_engine/compiler && make test
			# CommonType Unit Test
			cd "${GS_SOURCE_DIR}"/interactive_engine/executor/common/dyn_type && cargo test
			# Store Unit test
			cd "${GS_SOURCE_DIR}"/interactive_engine/executor/store/exp_store && cargo test
			# IR integration test
			cd "${GS_SOURCE_DIR}"/interactive_engine/compiler && ./ir_exprimental_ci.sh
			# IR integration pattern test
			cd "${GS_SOURCE_DIR}"/interactive_engine/compiler && ./ir_exprimental_pattern_ci.sh
		elif [[ ${storage_type} = "vineyard" ]]; then
			info "Testing interactive on local with vineyard storage"
			# start vineyard service
			export VINEYARD_IPC_SOCKET=/tmp/vineyard.sock
			vineyardd --socket=${VINEYARD_IPC_SOCKET} --meta=local &
			# load modern graph
			export STORE_DATA_PATH="${GS_SOURCE_DIR}"/charts/gie-standalone/data
			vineyard-graph-loader --config "${GS_SOURCE_DIR}"/charts/gie-standalone/config/v6d_modern_loader.json
			# start gie executor && frontend
			export GRAPHSCOPE_HOME="${GS_SOURCE_DIR}"/interactive_engine/assembly/target/graphscope
			schema_json=$(ls /tmp/*.json | head -1)
			object_id=${schema_json//[^0-9]/}
			GRAPHSCOPE_HOME=${GRAPHSCOPE_HOME} ${GRAPHSCOPE_HOME}/bin/giectl create_gremlin_instance_on_local /tmp/gs/${object_id} ${object_id} ${schema_json} 1 1235 1234 8182 ${VINEYARD_IPC_SOCKET}
			# IR integration test
			sleep 3s
			cd "${GS_SOURCE_DIR}"/interactive_engine/compiler
			make gremlin_test || true
			# clean
			rm -rf /tmp/*.json
			id=$(pgrep -f 'gaia_executor')
			if [[ -n ${id} ]]; then
				echo ${id} | xargs kill
			fi
			id=$(pgrep -f 'frontend')
			if [[ -n ${id} ]]; then
				echo ${id} | xargs kill
			fi
			id=$(pgrep -f 'vineyardd')
			if [[ -n ${id} ]]; then
				echo ${id} | xargs kill -9
			fi
		else
			info "Testing interactive on local with default storage"
		fi
	fi
	if [ "${on_k8s}" == "True" ]; then
		info "Testing interactive on k8s"
		export PYTHONPATH="${GS_SOURCE_DIR}"/python:${PYTHONPATH}
		cd "${GS_SOURCE_DIR}"/interactive_engine && mvn clean install --quiet -DskipTests -Drust.compile.skip=true -P graphscope,graphscope-assembly
		cd "${GS_SOURCE_DIR}"/interactive_engine/tests || exit
		./function_test.sh 8112 2
	fi
}
function test_learning {
	get_test_data
	err "Not implemented"
	exit 1
}

function test_e2e {
	get_test_data
	# Import python projects in the source directory
	cd "${GS_SOURCE_DIR}"/python || exit
	if [ "${on_local}" == "True" ]; then
		# unittest
		python3 -m pytest -s -vvv --exitfirst graphscope/tests/minitest/test_min.py
	fi
	if [ "${on_k8s}" == "True" ]; then
		# Run tests in Kubernetes environment using pytest
		python3 -m pytest -s -vvv --exitfirst ./graphscope/tests/kubernetes/test_demo_script.py
	fi
}

function test_groot {
	# Used to test groot
	get_test_data
	if [ "${on_local}" == "True" ]; then
		info "Testing groot on local"
		cd "${GS_SOURCE_DIR}"/interactive_engine/groot-server
		mvn test -Pgremlin-test
	fi
	if [ "${on_k8s}" == "True" ]; then
		info "Testing groot on k8s, note you must already setup a groot cluster and necessary environment variables"
		cd "${GS_SOURCE_DIR}"/python || exit
		python3 -m pytest --exitfirst -s -vvv ./graphscope/tests/kubernetes/test_store_service.py
	fi
}

test_"${type}"
