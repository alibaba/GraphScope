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
import shutil
import subprocess
import toml
import time


def get_hosts_from_toml(toml_hosts) -> dict:
    """
    Get machine lists from a `toml` file, which has the format as:
        [[peers]] \n
        server_id = 0 \n
        ip = '127.0.0.1' \n
        port = 12334 \n
        [[peers]] \n
        server_id = 1 \n
        ip = '127.0.0.1' \n
        port = 12335
    :param toml_hosts: The directory to the toml-formatted host file
    :return: A hosts dictionary of the type {server_id: { ip_addr: <ip>, port: <port> }}
    """
    toml_parser = toml.load(toml_hosts)
    hosts_dict = {}
    for items in toml_parser.get("peers"):
        hosts_dict[int(items['server_id'])] = {'ip_addr': items['ip'], 'port': items['port']}
    return hosts_dict


def hosts_dict_to_list(hosts_dict: dict) -> list:
    """
    Change a host dictionary to a list that contains a tuple of `ip_addr` and `port`
    :param hosts_dict: The dictionary returned from `get_hosts_from_toml`
    :return: A list of tuples of `ip_addr, port`
    """
    hosts = list()
    for idx in sorted(hosts_dict.keys()):
        hosts.append((hosts_dict[idx]['ip_addr'], hosts_dict[idx]['port']))

    return hosts


def sync_process(pool):
    """
    Synchronize the process of all machines in the cluster
    """
    for p in pool:
        p.wait()
    del pool[:]


def terminate_when_one_end(pool):
    while True:
        time.sleep(5)
        for p in pool:
            if p.poll() != None:
                del pool[:]


def clean_up(prog_name, hosts, num=0):
    """
    Clean up the program in all given machines. **Root** is required.
    :param prog_name:
    :param machines: The machines
    """
    if num == 0:
        num = len(hosts)
    pool = list()
    for server_id in range( num):
        host = hosts[server_id][0]
        command = [shutil.which("ssh"), host, "pkill -f " + prog_name]
        print(command)
        pool.append(subprocess.Popen(command))

    sync_process(pool)


def str2bool(v):
    """
    Parse a string option either of yes/true/t/y/1 into `True`, and a a string of no/false/f/n/0 into `False`
    :param v: The string option
    :return: A boolean value from the string
    """
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    if v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False

    raise argparse.ArgumentTypeError('Boolean value expected.')
