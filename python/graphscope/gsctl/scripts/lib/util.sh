# refer from https://github.com/pypa/manylinux/blob/b4884d90c984cb17f7cb4aabe3509347698d7ee7/docker/build_scripts/build_utils.sh#L26
function fetch_source {
    local file=$1
    local url=$2
    if [ -f "${file}" ]; then
        echo "${file} exists, skipping fetch"
    else
        curl -fsSL -o "${file}" "${url}/${file}"
        # Use sock5s proxy to download files in case download fails in normal cases
        # `host.docker.internal` is the localhost of host machine from a container's perspective.
        # See https://docs.docker.com/desktop/networking/#i-want-to-connect-from-a-container-to-a-service-on-the-host
        # curl -fsSL -o ${file} ${url}/${file} || curl -x socks5h://host.docker.internal:13659 -fsSL -o ${file} ${url}/${file}
    fi
}

function download_tar_and_untar_if_not_exists {
    local directory=$1
    local file=$2
    local url=$3
    if [ ! -d "${directory}" ]; then
      [ ! -f "${file}" ] &&
        fetch_source "${file}" "${url}"
      tar zxf "${file}"
    fi
}

function clone_if_not_exists {
    local directory=$1
    local file=$2
    local url=$3
    local branch=$4
    if [ ! -d "${directory}" ]; then
      if [ ! -f "${file}" ]; then
        git clone --depth=1 --branch "${branch}" "${url}" "${directory}"
        pushd "${directory}" || exit
        git submodule update --init || true
        popd || exit
      else
        tar zxf "${file}"
      fi
    fi
}

function cleanup_files {
  if [ "${GRAPHSCOPE_NO_INSTALL_CLEANUP}" != "true" ]; then
    log "Cleaning up intermediate files [$*]"
    log "Disable this behaviour by setting GRAPHSCOPE_NO_INSTALL_CLEANUP=true."
    for file in "$@"
    do
        log "Cleaning up ${file}"
        if [[ -f "${file}" || -d "${file}" ]]; then
          rm -rf "${file}"
        fi
    done
  fi
}

function maybe_set_to_cn_url {
  local url=$1
  if [ "${GRAPHSCOPE_DOWNLOAD_FROM_CN}" == "true" ]; then
    url="https://graphscope.oss-cn-beijing.aliyuncs.com/dependencies"
  fi
  echo ${url}
}
