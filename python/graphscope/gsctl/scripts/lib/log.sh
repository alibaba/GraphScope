
err() {
  echo -e "$(red_bold "[ERROR] $*")" >&2
}

warning() {
  echo -e "$(yellow_bold "[WARNING] $*")" >&1
}

log() {
  echo -e "$(green "$*")" >&1
}

info() {
  echo -e "$(green "$*")" >&1
}

succ() {
  echo -e "$(green_bold "[SUCCESS]: $*")" >&1
}
