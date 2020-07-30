#!/bin/bash
set -e
set -o pipefail

# install dependencies
echo "$(date '+%Y-%m-%d %H:%M:%S') install systemd dependencies"
sudo apt update && sudo apt install -yqq daemonize dbus-user-session fontconfig

# enable systemd in WSL
echo "$(date '+%Y-%m-%d %H:%M:%S') enable systemd in WSL"
SCRIPT_PATH=`realpath ${BASH_SOURCE[0]}`
SCRIPT_DIR=`dirname ${SCRIPT_PATH}`
sudo cp ${SCRIPT_DIR}/start-systemd-namespace.sh /usr/sbin/start-systemd-namespace
sudo cp ${SCRIPT_DIR}/enter-systemd-namespace.sh /usr/sbin/enter-systemd-namespace
sudo chmod +x /usr/sbin/enter-systemd-namespce.sh

sudo sed -i 2a"Defaults        env_keep +=  WSLPATH\nDefaults        env_keep += WSLENV\n\
Defaults        env_keep += WSL_INTEROP\nDefaults        env_keep += WSL_DISTRO_NAME\n\
Defaults        env_keep += PRE_NAMESPACE_PATH\n%sudo ALL=(ALL) NOPASSWD: /usr/sbin/enter-systemd-namespace\n" /etc/sudoers

sudo sed -i 2a"# Start or enter a PID namespace in WSL2\nsource /usr/sbin/start-systemd-namespace\n" /etc/bash.bashrc

# then open a new session
echo "$(date '+%Y-%m-%d %H:%M:%S') please open a new wsl session."

set +e
set +o pipefail
