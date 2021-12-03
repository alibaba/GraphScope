#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
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
#

import argparse
import tempfile
import os
import shutil
import subprocess
from utils import get_hosts_from_toml, hosts_dict_to_list, sync_process, clean_up, str2bool

def extract_file_name(line: str):
    """
    Extract the file name from a line getting by a line of string that contains a directory like `/path/to/file`,
    where `file` will be extracted.
    :param line: A line of string
    :return: The extracted tuple of absolute path and filename
    """
    start_idx = line.rfind('hdfs://')
    if start_idx == -1:
        absolute_path = line
    else:
        absolute_path = line[start_idx:]
    idx = absolute_path.rfind('/')
    filename = absolute_path[idx + 1:]
    if filename.startswith('.'):
        return None, None

    return absolute_path, filename


####################################################################
####################### Prepare the arguments ######################
####################################################################

TMP_FOLDER = "gaia_tmp"
parser = argparse.ArgumentParser()
# Note that we need to create temporary folder on each machine
tmp_dir = os.path.join(tempfile.gettempdir(), TMP_FOLDER)
parser.add_argument("-o", "--opt", choices=["clean_up", "par_loader", "start_rpc"], required=True, help="Running options")
parser.add_argument("-d", "--raw_data_dir", help="The directory of the raw data, \
        HDFS folder if starting with \"hdfs://\"")
parser.add_argument("-g", "--graph_dir", help="The directory of the graph store")
parser.add_argument("-s", "--schema_file", help="The file of the graph schema")
parser.add_argument("-H", "--host_file", help="The hosts file (in toml format as sampled) to records the machines \
        that the graph data will be maintained")
parser.add_argument("-A", "--hadoop_home", help="The $HADOOP_HOME")
parser.add_argument("-p", "--raw_partitions", default=1, type=int, help="The partitiosn of the raw data")
parser.add_argument("-w", "--workers", default=1, type=int, help="The number of workers (threads) in each machine")
parser.add_argument("-N", "--num_machines", default=0,type=int, help="The number of machines used")
parser.add_argument("-t", "--tmp_dir", default=tmp_dir, help="The tmp directory to maintain intermediate data")
parser.add_argument("-P", "--port", help="The port for RPC connection")
parser.add_argument("-D", "--download", type=str2bool, nargs='?', const=True, default=False,
                    help="Whether to download raw file")
# parser.add_argument("-C", "--clean", type=str2bool, nargs='?', const=True, default=False, help="Clean the process")

args = parser.parse_args()
if args.tmp_dir is not None:
    tmp_dir = args.tmp_dir

if args.host_file is not None:
    hosts = hosts_dict_to_list(get_hosts_from_toml(args.host_file))
    num_machines = args.num_machines
    if num_machines == 0 or len(hosts) < num_machines:
        # The default number of machines is the number of hosts in the host file
        num_machines = len(hosts)
else:
    num_machines = 1

####################################################################
################### End  of preparing arguments ####################
####################################################################


def prepare_files():
    schema_folder = os.path.join(args.graph_dir, "graph_schema")
    schema_file = os.path.join(args.graph_dir, "graph_schema", "schema.json")

    subprocess.check_call([shutil.which("mkdir"), "-p", tmp_dir])
    subprocess.check_call([shutil.which("mkdir"), "-p", schema_folder])
    subprocess.check_call([shutil.which("cp"), args.schema_file, schema_file])

    if args.host_file is not None:
        host_list_file = os.path.join(tmp_dir, "hosts")
        with open(host_list_file, 'w') as f:
            for host in hosts:
                f.write("%s:%s\n" % (host[0], host[1]))
        for i in range(num_machines):
            subprocess.check_call([shutil.which("ssh"), hosts[i][0], "mkdir", "-p", tmp_dir])
            subprocess.check_call([shutil.which("ssh"), hosts[i][0], "mkdir", "-p", schema_folder])
            subprocess.check_call([shutil.which("scp"), args.schema_file, "%s:%s" % (hosts[i][0], schema_file)])
            subprocess.check_call([shutil.which("scp"), host_list_file, "%s:%s" % (hosts[i][0], tmp_dir)])


def download_from_hdfs():
    """
    Download given files from HDFS to local hosts
    """
    if args.host_file is None:
        print("Must specify -H/--host_file")
        parser.print_help()
        exit(1)
    program = os.path.join(os.getcwd(), "bin", "downloader")
    pool = []
    env = "RUST_LOG=Info"
    if args.host_file is not None:
        host_list_file = os.path.join(tmp_dir, "hosts")
        for i in range(num_machines):
            run_params = "%s %s %s %s %s %d -w %d -n %d -p %d -h %s" % \
                     (env, program, args.hadoop_home, args.raw_data_dir, tmp_dir,
                      args.raw_partitions, args.workers, num_machines, i, host_list_file)
            cmd = [shutil.which("ssh"), hosts[i][0], run_params]
            print(cmd)
            pool.append(subprocess.Popen(cmd))
        sync_process(pool)


def par_loader():
    """
    Parallelize loading the graph store from the raw data
    """
    # Check the required arguments for building storage
    if args.raw_data_dir is None or args.graph_dir is None or args.schema_file is None:
        print("Must specify -d/--raw_data_dir, -g/--graph_dir, -s/--schema_file")
        parser.print_help()
        exit(1)
    if args.host_file is None:
        print("Must specify -H/--host_file")
        parser.print_help()
        exit(1)

    graph_name = extract_file_name(args.graph_dir)[1]
    log_dir = os.path.join(os.getcwd(), "logs", "par_loader", "%s_w%d_m%d" % (graph_name, args.workers, num_machines))
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    local_dir = args.raw_data_dir
    # Means the raw data is in HDFS
    if args.raw_data_dir.startswith("hdfs://"):
        if args.hadoop_home is None:
            print("Must specify -A or --hadoop_home")
            parser.print_help()
            exit(1)
        if args.download:
            download_from_hdfs()
        # After downloading data to local
        local_dir = tmp_dir

    program = os.path.join(os.getcwd(), "bin", "par_loader")
    pool = []
    env = "RUST_LOG=Info"
    if args.host_file is not None:
        host_list_file = os.path.join(tmp_dir, "hosts")
        for i in range(num_machines):
            log_file = open(os.path.join(log_dir, "%02d.log" % i), 'w')
            run_params = "%s %s %s %s %d -w %d -n %d -p %d -h %s" % \
                         (env, program, local_dir, args.graph_dir,
                          args.raw_partitions, args.workers, num_machines, i, host_list_file)
            cmd = [shutil.which("ssh"), hosts[i][0], run_params]
            print(cmd)
            pool.append(subprocess.Popen(cmd, stdout=log_file, stderr=log_file))
            log_file.close()
        sync_process(pool)

def start_rpc():
    """
    Run RPC service
    """
    # Check the required arguments for starting RPC service
    if args.graph_dir is None:
        print("Must specify -g/--graph_dir")
        parser.print_help()
        exit(1)
    if args.host_file is None:
        print("Must specify -H/--host_file")
        parser.print_help()
        exit(1)

    graph_name = extract_file_name(args.graph_dir)[1]
    log_dir = os.path.join(os.getcwd(), "logs", "start_rpc", "%s_m%d" % (graph_name, num_machines))
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    program = os.path.join(os.getcwd(), "bin", "start_rpc_server")
    pool = []
    env = "RUST_LOG=graph_store=Info,gremlin_core=Info,pegasus_server=Info DATA_PATH=\"%s\"" % args.graph_dir
    if args.host_file is not None:
        prev_host = ""
        prev_port = 0
        for i in range(num_machines):
            log_file = open(os.path.join(log_dir, "%02d.log" % i), 'w')
            env_i = "%s PARTITION_ID=%d" % (env, i)
            run_params = "%s %s -h %s -i %d" % (env_i, program, args.host_file, i)
            port_opt = "1234"
            if args.port is not None:
                port_opt = args.port
            if prev_port == 0:
                prev_port = int(port_opt)
                port = prev_port
            if hosts[i][0] == prev_host:
                # Running in the same host, the port can not be the same
                # For now, we simply increment the port number by 1 for the following hosts
                port = prev_port + 1
            prev_port = port
            run_params = "%s -p %d --report" % (run_params, port)
            cmd = [shutil.which("ssh"), hosts[i][0], run_params]
            print(cmd)
            pool.append(subprocess.Popen(cmd, stdout=log_file, stderr=log_file))
            prev_host = hosts[i][0]
            log_file.close()
        sync_process(pool)


if __name__ == '__main__':
    if args.opt == "clean_up":
        if args.host_file is not None:
            print("Require **Root** for cleaning up...")
            clean_up("downloader", hosts, num_machines)
            clean_up("par_loader", hosts, num_machines)
            clean_up("start_rpc_server", hosts, num_machines)
            exit(1)
    elif args.opt == "par_loader":
        prepare_files()
        par_loader()
        exit(1)
    elif args.opt == "start_rpc":
        start_rpc()
        exit(1)
    else:
        print("The option <%s> is not supported" % args.opt)
