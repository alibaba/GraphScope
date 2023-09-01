script_dir="$(cd "$(dirname "$0")" && pwd)"
source ${script_dir}/lib/get_os_version.sh
source ${script_dir}/lib/log.sh
source ${script_dir}/lib/colors.sh
source ${script_dir}/lib/install_thirdparty_dependencies.sh
source ${script_dir}/lib/install_vineyard.sh
source ${script_dir}/lib/util.sh


echo "# this file is located in 'src/format_command.sh'"
echo "# code for 'gs format' goes here"
echo "# you can edit it freely and regenerate (it will not be overwritten)"

while getopts ":l:" opt; do
  case $opt in
    l)
      lang="$OPTARG"
      ;;
    \?)
      echo "Invalid options: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "The option -$OPTARG requires parameters" >&2
      exit 1
      ;;
  esac
done



GS_SOURCE_DIR="$(dirname -- "$(readlink -f "${BASH_SOURCE}")")"

function format_cpp {
	if ! [ -x "$(command -v clang-format)" ]; then
		echo 'Downloading clang-format.' >&2
		curl -L https://github.com/muttleyxd/clang-tools-static-binaries/releases/download/master-22538c65/clang-format-8_linux-amd64 --output ${GRAPHSCOPE_HOME}/bin/clang-format
		chmod +x ${GRAPHSCOPE_HOME}/clang-format
		export PATH="${GRAPHSCOPE_HOME}/bin:${PATH}"
	fi
	pushd "${GS_SOURCE_DIR}"/analytical_engine || exit
	files=$(find ./apps ./benchmarks ./core ./frame ./misc ./test \( -name "*.h" -o -name "*.cc" \))

	# run format
	clang-format -i --style=file $(echo $files)
	popd || exit
}

function lint_cpp {
	pushd "${GS_SOURCE_DIR}"/analytical_engine || exit
	files=$(find ./apps ./benchmarks ./core ./frame ./misc ./test \( -name "*.h" -o -name "*.cc" \))

	./misc/cpplint.py $(echo $files)
	popd || exit
}

function format_java {
	jarfile=google-java-format-1.13.0-all-deps.jar
	if [[ ! -f ${jarfile} ]]; then
		wget https://github.com/google/google-java-format/releases/download/v1.13.0/${jarfile}
	fi
	# run formatter in-place
	java -jar ${jarfile} --aosp --skip-javadoc-formatting -i $(git ls-files *.java)

}

function format_python {
	if ! [ -x "$(command -v black)" ]; then
		pip3 install -r ${GS_SOURCE_DIR}/coordinator/requirements-dev.txt --user
	fi
	pushd python || exit
	python3 -m isort --check --diff .
	python3 -m black --check --diff .
	python3 -m flake8 .
	popd || exit
	pushd coordinator || exit
	python3 -m isort --check --diff .
	python3 -m black --check --diff .
	python3 -m flake8 .
	popd || exit
}

function format_rust {
	cd "${GS_SOURCE_DIR}"/interactive_engine/executor/assembly/groot
	cargo +nightly fmt -- --check
	cd "${GS_SOURCE_DIR}"/interactive_engine/executor/assembly/v6d
	cargo +nightly fmt -- --check
	cd "${GS_SOURCE_DIR}"/interactive_engine/executor/common/dyn_type/
	cargo +nightly fmt -- --check
	cd "${GS_SOURCE_DIR}"/interactive_engine/executor/engine/pegasus/
	cargo +nightly fmt -- --check
	cd "${GS_SOURCE_DIR}"/interactive_engine/executor/ir/
	cargo +nightly fmt -- --check
	cd "${GS_SOURCE_DIR}"/interactive_engine/executor/store/
	cargo +nightly fmt -- --check
}

format_"${lang}"
