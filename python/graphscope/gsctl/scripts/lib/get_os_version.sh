get_os_version() {
  if [ -f /etc/centos-release ]; then
    # Older Red Hat, CentOS, Alibaba Cloud Linux etc.
    PLATFORM=CentOS
    OS_VERSION=$(sed 's/.* \([0-9]\).*/\1/' < /etc/centos-release)
    if grep -q "Alibaba Cloud Linux" /etc/centos-release; then
      PLATFORM="Aliyun_based_on_CentOS"
      OS_VERSION=$(rpm -E %{rhel})
    fi
  elif [ -f /etc/os-release ]; then
    # freedesktop.org and systemd
    . /etc/os-release
    PLATFORM="${NAME}"
    OS_VERSION="${VERSION_ID}"
  elif type lsb_release >/dev/null 2>&1; then
    # linuxbase.org
    PLATFORM=$(lsb_release -si)
    OS_VERSION=$(lsb_release -sr)
  elif [ -f /etc/lsb-release ]; then
    # For some versions of Debian/Ubuntu without lsb_release command
    . /etc/lsb-release
    PLATFORM="${DISTRIB_ID}"
    OS_VERSION="${DISTRIB_RELEASE}"
  elif [ -f /etc/debian_version ]; then
    # Older Debian/Ubuntu/etc.
    PLATFORM=Debian
    OS_VERSION=$(cat /etc/debian_version)
  else
    # Fall back to uname, e.g. "Linux <version>", also works for BSD, Darwin, etc.
    PLATFORM=$(uname -s)
    OS_VERSION=$(uname -r)
  fi
  echo "$PLATFORM-$OS_VERSION"
}
