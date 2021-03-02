#!/usr/bin/env bash
#
# This file is referred and derived from project natelandau/shell-scripts
# https://github.com/natelandau/shell-scripts
#

# ##################################################
# The parser to encode the LDBC generated data to a graph database.
#
version="1.0.0"               # Sets version variable
#
# HISTORY:
#
# * DATE - v1.0.0  - First Creation
#
# ##################################################

# Provide a variable with the location of this script.
script_path="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"


# safeExit
# -----------------------------------
# Non destructive exit for when script exits naturally.
# Usage: Add this function at the end of every script.
# -----------------------------------
function safeExit() {
  # Delete temp files, if any
  # if [ -d "${tmpDir}" ]; then
  #  echo "Cleaning up temporary files @ ${tmpDir}"
  #  rm -r "${tmpDir}"
  # fi
  trap - INT TERM EXIT
  exit
}

# Set Temp Directory
# -----------------------------------
# Create temp directory with three random numbers and the process ID
# in the name.  This directory is removed automatically at exit.
# -----------------------------------
#(umask 077 && mkdir "${tmpDir}") #|| {
  #die "Could not create temporary directory! Exiting."
#}

function checkArgs() {
  if [ ! "${root_dir}" ]; then
    root_dir=$script_path
  elif [ ! "${ldbc_dir}" ]; then
    echo "Must specify < -d|--ldbc_dir >"
    usage >&2; safeExit
  elif [ ! "${graph_dir}" ]; then
    echo "Must specify < -g|--graph_dir >"
  elif [ ! "${schema}" ]; then
    echo "Must specify < -s|--schema >"
    usage >&2; safeExit
  fi

  echo "The configurations:" \
     "-d ${ldbc_dir}," \
     "-p ${ldbc_partitions}," \
     "-g ${graph_dir}," \
     "-w ${workers}," \
     "-t ${hosts},"
}

function createDirs() {
  # TODO(longbin) pssh
  mkdir -p ${root_dir}
}

function buildTools() {
    if [ ! -f "${root_dir}/download_raw_from_hdfs" ]
    then
      echo "Building download_raw_from_hdfs"
      cargo build --release --bin download_raw_from_hdfs
      # TODO(longbin) pscp
      cp "${script_path}/target/release/download_raw_from_hdfs" ${root_dir}
    fi
    if [ ! -f "${root_dir}/ldbc_parse_partition" ]
    then
      echo "Building ldbc_parse_partition"
      cargo build --release --bin ldbc_parse_partition
      # TODO(longbin) pscp
      cp "${script_path}/target/release/ldbc_parse_partition"  ${root_dir}
    fi
}

function downloadRawFromHdfs() {
  bin="${root_dir}/download_raw_from_hdfs"
  if [[ $ldbc_dir = hdfs* ]]
  then
      is_local_data=false
      local_ldbc_dir=${root_dir}/`echo ${ldbc_dir} | awk -F'/' '{print $NF}'`
      echo "Downloading LDBC data from HDFS@ ${ldbc_dir} to ${local_ldbc_dir}"
      if [ ! -d ${local_ldbc_dir} ]
      then
        # TODO(longbin) must create the local ldbc dir in all machines
        echo -n ''
      fi
  else
    is_local_data=true
    echo "LDBC data is local."
  fi
}

function createGraphDir() {
  # TODO(longbin) pssh
  mkdir -p "${graph_dir}/graph_schema"
  # TODO(longbin) pssh
  cp ${schema} "${graph_dir}/graph_schema"
}

function mainScript() {
############## Begin Script Here ###################
####################################################
  bin="${root_dir}/ldbc_parse_partition"
  if [ $is_local_data ]
  then
    # TODO(longbin) "pssh"
    RUST_LOG=Info ${bin} ${ldbc_dir} ${graph_dir} ${ldbc_partitions} -w ${workers}
  else
    if [ -d ${local_ldbc_dir} ]
    then
      # TODO(longbin)"pssh"
      ${bin} ${local_ldbc_dir} ${graph_dir} ${ldbc_partitions} -w ${workers}
    else
      echo "Must first download data to ${local_ldbc_dir}"
    fi
  fi
####################################################
############### End Script Here ####################
}


############## Begin Options and Usage ###################

# Print usage
usage() {
  echo -n "./parse.sh [OPTION]... [FILE]...
 ${bold}Options:${reset}
  -r, --root_dir  The working directory of the parser
  -d, --ldbc_dir    The directory of the LDBC raw data, HDFS folder if starting with "hdfs://"
  -p, --ldbc_partitions The partitiosn of the LDBC raw data
  -g, --graph_dir   The director of the graph store
  -w, --graph_partitions  The number of partitions of the graph data in each machine
  -t, --hosts   The hosts file to records the machines (ip_addr:port) that the graph data will be maintained
  -l, --log         Print log to file
  -h, --help        Display this help and exit
      --version     Output version information and exit
"
}

# Iterate over options breaking -ab into -a -b when needed and --foo=bar into
# --foo bar
optstring=h
unset options
while (($#)); do
  case $1 in
    # If option is of type -ab
    -[!-]?*)
      # Loop over each character starting with the second
      for ((i=1; i < ${#1}; i++)); do
        c=${1:i:1}

        # Add current char to options
        options+=("-$c")

        # If option takes a required argument, and it's not the last char make
        # the rest of the string its argument
        if [[ $optstring = *"$c:"* && ${1:i+1} ]]; then
          options+=("${1:i+1}")
          break
        fi
      done
      ;;

    # If option is of type --foo=bar
    --?*=*) options+=("${1%%=*}" "${1#*=}") ;;
    # add --endopts for --
    --) options+=(--endopts) ;;
    # Otherwise, nothing special
    *) options+=("$1") ;;
  esac
  shift
done
set -- "${options[@]}"
unset options

# Print help if no arguments were passed.
# Uncomment to force arguments when invoking the script
# [[ $# -eq 0 ]] && set -- "--help"

ldbc_partitions=1
workers=1
isWholeData=false
# Read the options and set stuff
while [[ $1 = -?* ]]; do
  case $1 in
    -h|--help) usage >&2; safeExit ;;
    --version) echo "$(basename $0) ${version}"; safeExit ;;
    -r|--root_dir) shift; root_dir=${1}; ;;
    -d|--ldbc_dir) shift; ldbc_dir=${1}; ;;
    -p|--ldbc_partitions) shift; ldbc_partitions=${1} ;;
    -g|--graph_dir) shift; graph_dir=${1};;
    -w|--graph_partitions) shift; workers=${1} ;;
    -s|--schema) shift; schema=${1} ;;
    -t|--hosts) shift; hosts=${1} ;;
    -l|--log) printLog=true ;;
    --endopts) shift; break ;;
    *) die "invalid option: '$1'." ;;
  esac
  shift
done

# Store the remaining part as arguments.
args+=("$@")

############## End Options and Usage ###################


# ############# ############# #############
# ##       TIME TO RUN THE SCRIPT        ##
# ##                                     ##
# ## You shouldn't need to edit anything ##
# ## beneath this line                   ##
# ##                                     ##
# ############# ############# #############

# Trap bad exits with your cleanup function
# trap trapCleanup EXIT INT TERM

# Set IFS to preferred implementation
IFS=$'\n\t'

# Exit on error. Append '||true' when you run the script if you expect an error.
set -o errexit

# Run in debug mode, if set
# if ${debug}; then set -x ; fi

# Exit on empty variable
# if ${strict}; then set -o nounset ; fi

# Bash will remember & return the highest exitcode in a chain of pipes.
# This way you can catch the error in case mysqldump fails in `mysqldump |gzip`, for example.
set -o pipefail

# Invoke the checkDependenices function to test for Bash packages.  Uncomment if needed.
# checkDependencies

# Check arguments
checkArgs

# Create working directory
createDirs

# Build the parsing tools
buildTools

# Download ldbc data from HDFS if necessary
downloadRawFromHdfs

# Create graph_dir on each machine
createGraphDir

# Run your script
mainScript

# Exit cleanlyd
safeExit
