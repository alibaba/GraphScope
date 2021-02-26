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

"""Utility module to launch cluster on aliyun or aws
"""

import argparse
import json
import os
import sys
import subprocess
import time
import yaml

import click
import boto3

from alibabacloud_cs20151215.client import Client as CS20151215Client
from alibabacloud_ecs20140526.client import Client as Ecs20140526Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_cs20151215 import models as cs20151215_models
from alibabacloud_ecs20140526 import models as ecs_20140526_models
from alibabacloud_vpc20160428.client import Client as Vpc20160428Client
from alibabacloud_vpc20160428 import models as vpc_20160428_models


aws_vpc_template = "https://s3.us-west-2.amazonaws.com/amazon-eks/cloudformation/2020-10-29/amazon-eks-vpc-sample.yaml"
aws_workers_template = "https://s3.us-west-2.amazonaws.com/amazon-eks/cloudformation/2020-10-29/amazon-eks-nodegroup.yaml"


def process_args(cloud_type):
    config = {}
    config["access_key_id"] = click.prompt("Your access_key_id", type=str)
    config["secret_access_key"] = click.prompt("Your secret_access_key", type=str)
    config["region"] = click.prompt("Your region", type=str)
    config["cluster_name"] = click.prompt("The cluster name you want to use or create", type=str)
    default_k8s_version = ("1.18" if cloud_type == "aws" else "1.18.8-aliyun.1")
    config["k8s_version"] = click.prompt("k8s version, default",
                                         type=str, default=default_k8s_version)
    default_instance_type = ("t2.medium" if cloud_type == "aws" else "ecs.n4.large")
    config["instance_type"] = click.prompt("Worker node instance type, defalut",
                                           type=str, default=default_instance_type)
    config["node_num"] = click.prompt("Worker node num, default", type=int, default=2)
    config_file = os.environ["HOME"] + "/.kube/config" 
    config["output_path"] = click.prompt("output kube config location, default",
                                         type=str, default=config_file)
    return config


class Launcher(object):
    pass


class AWSLauncher(Launcher):
    def __init__(self,
                 access_key_id=None,
                 secret_access_key=None,
                 region=None,
                 cluster_name=None,
                 k8s_version="1.18",
                 instance_type="t2.medium",
                 node_num=2,
                 output_path=None):
        self._sess = boto3.session.Session(aws_access_key_id=access_key_id,
                                   aws_secret_access_key=secret_access_key,
                                   region_name=region)
        self._region = region
        self._cluster_name = cluster_name
        self._k8s_version = k8s_version
        self._vpc_name = cluster_name + "-vpc"
        self._workers_name = cluster_name + "-workers"
        self._k8s_admin_role_name = cluster_name + "-role"
        self._instance_type = instance_type
        self._node_group_min = 0
        self._node_group_max = node_num
        self._config_output_path = output_path

    def get_role(self):
        iam = self._sess.client("iam")
        print("*** IAM role")
        try:
            # See if role exists.
            role = iam.get_role(RoleName=self._k8s_admin_role_name)
            print("IAM role exists.")
        except:
            print("IAM role does not exist.  Creating...")
            # This is an AWS role policy document.  Allows access for EKS.
            trust_policy = json.dumps({
              "Version": "2012-10-17",
              "Statement": [
                {
                  "Effect": "Allow",
                  "Principal": {
                    "Service": "eks.amazonaws.com"
                  },
                  "Action": "sts:AssumeRole"
                }
              ]
            })
            # Create role.
            iam.create_role(
                RoleName=self._k8s_admin_role_name,
                AssumeRolePolicyDocument=trust_policy,
                Description="Role providing access to EKS resources from EKS"
            )
            # Add policies allowing access to EKS API.
            iam.attach_role_policy(
                RoleName=self._k8s_admin_role_name,
                PolicyArn="arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
            )
            iam.attach_role_policy(
                RoleName=self._k8s_admin_role_name,
                PolicyArn="arn:aws:iam::aws:policy/AmazonEKSServicePolicy"
            )

        role = iam.get_role(RoleName=self._k8s_admin_role_name)
        return role["Role"]["Arn"]
    
    def stack_exists(self, cf, name):
        try:
            stack = cf.describe_stacks(StackName=name)
            return True
        except:
            return False

    def get_vpc_stack(self):
        # The VPC stack is a VPC and subnetworks to allow K8s communication.
        cf = self._sess.client("cloudformation")

        print("*** VPC stack")
        if self.stack_exists(cf, self._vpc_name):
            # stack exists, do nothing.
            print("VPC stack already exists.")
        else:
            print("Creating VPC stack...")
            # Create VPC stack.
            response = cf.create_stack(
                StackName=self._vpc_name,
                TemplateURL=vpc_template,
                Parameters=[],
                TimeoutInMinutes=15,
                OnFailure='DELETE'
            )
            if response == None:
                print("Could not create VPC stack.")
                sys.exit(1)
            if not "StackId" in response:
                print("Could not create VPC stack.")
                sys.exit(1)

            # Get stack ID for later.
            stack_id=response["StackId"]

            print("Created stack " + self._vpc_name)
            print("Waiting for VPC stack creation to complete...")

            try:
                # A waiter is something which polls AWS to find out if an operation
                # has completed.
                waiter = cf.get_waiter('stack_create_complete')
                # Wait for stack creation to complet
                res = waiter.wait(StackName=self._vpc_name)
            except:
                # If waiter fails, that'll be the thing taking too long to deploy.
                print("Gave up waiting for stack to create.")
                sys.exit(1)
            print("VPC stack created")

        # Get output information from the stack: VPC ID, security group and subnet IDs.
        stack = cf.describe_stacks(StackName=self._vpc_name)
        vpc_sg=None
        vpc_subnet_ids=None
        vpc_id=None
        # Loop over outputs grabbing information.
        for v in stack["Stacks"][0]["Outputs"]:
            if v["OutputKey"] == "SecurityGroups": vpc_sg=v["OutputValue"]
            if v["OutputKey"] == "VpcId": vpc_id=v["OutputValue"]
            if v["OutputKey"] == "SubnetIds": vpc_subnet_ids=v["OutputValue"]

        print("VPC ID: %s" % vpc_id)
        print("VPC security group: %s" % vpc_sg)
        print("VPC subnet IDs: %s" % vpc_subnet_ids)
        # Split subnet IDs - it's comma separated.
        vpc_subnet_ids = vpc_subnet_ids.split(",")
        vpc_meta = {
            "sg": vpc_sg,
            "subnets": vpc_subnet_ids,
            "id": vpc_id,
        }

        return vpc_meta 

    def create_cluster_completed(self, eks, name):
        try:
            waiter = eks.get_waiter('cluster_active')
            res = waiter.wait(name=name)
        except:
            print("Gave up waiting for cluster to create.")
            sys.exit(1)
        print("Cluster active.")

    def create_cluster(self):
        eks = self._sess.client("eks")
        print("*** EKS cluster")
        vpc_meta = self.get_vpc_stack()
        try:
            cluster = eks.describe_cluster(name=self._cluster_name)
            print("Cluster already exists.")
        except:
            print("Creating cluster (ETA ~10 minutes)...")
            role_arn = self.get_role()
            # Creating Kubernetes cluster.
            response = eks.create_cluster(
                name=self._cluster_name,
                version=self._k8s_version,
                roleArn=role_arn,
                resourcesVpcConfig={
                    "subnetIds": vpc_meta["subnets"],
                    "securityGroupIds": [vpc_meta["sg"]],
                    'endpointPublicAccess': True
                },
            )
            print("Cluster creation initiated.")
            print("Waiting for completion (ETA 10 minutes)...")
            self.create_cluster_completed(eks, self._cluster_name)

        # Get cluster stuff
        cluster = eks.describe_cluster(name=self._cluster_name)

        # This spots the case where the cluster isn't in an expected state.
        status = cluster["cluster"]["status"]
        if status != "ACTIVE":
            print("Cluster status %s, should be ACTIVE!" % status)
            sys.exit(1)

        # Get cluster endpoint and security info.
        cluster_cert = cluster["cluster"]["certificateAuthority"]["data"]
        cluster_ep = cluster["cluster"]["endpoint"]
        print("Cluster: %s" % cluster_ep)
        # write k8s config
        self.write_kube_config(cluster_cert, cluster_ep, self._output_path)
        self.create_worker_stack(vpc_meta["id"], vpc_meta["sg"], vpc_meta["subnets"])
    
    def create_worker_stack(self, vpc_id, vpc_sg, vpc_subnet_ids):
        # a stack of worker instances is created using CloudFormation.
        print("*** Workers stack.")
        cf = self._sess.client("cloudformation")
        if self.stack_exists(cf, self._workers_name):
            print("Workers stack already exists.")
        else:
            print("Creating workers stack...")
            # Create key pair
            ec2 = self._sess.client("ec2")
            resp = ec2.create_key_pair(KeyName=self._cluster_name + "-keypair")
            # Create stack
            response = cf.create_stack(
                StackName=self._workers_name,
                TemplateURL=workers_template,
                Capabilities=["CAPABILITY_IAM"],
                Parameters=[
                    {
                        "ParameterKey": "ClusterName",
                        "ParameterValue": self._cluster_name
                    },
                    {
                        "ParameterKey": "ClusterControlPlaneSecurityGroup",
                        "ParameterValue": vpc_sg
                    },
                    {
                        "ParameterKey": "NodeGroupName",
                        "ParameterValue": self._cluster_name + "-worker-group"
                    },
                    {
                        "ParameterKey": "NodeAutoScalingGroupMinSize",
                        "ParameterValue": str(1)
                    },
                    {
                        "ParameterKey": "NodeAutoScalingGroupMaxSize",
                        "ParameterValue": str(4)
                    },
                    {
                        "ParameterKey": "NodeInstanceType",
                        "ParameterValue": self._instance_type
                    },
                    {
                        "ParameterKey": "KeyName",
                        "ParameterValue": self._cluster_name + "-keypair"
                    },
                    {
                        "ParameterKey": "VpcId",
                        "ParameterValue": vpc_id
                    },
                    {
                        "ParameterKey": "Subnets",
                        "ParameterValue": ",".join(vpc_subnet_ids)
                    }
                ],
                TimeoutInMinutes=15,
                OnFailure='DELETE'
            )

            if response == None:
                print("Could not create worker group stack.")
                sys.exit(1)
            if not "StackId" in response:
                print("Could not create worker group stack.")
                sys.exit(1)
            print("Initiated workers (ETA 5-20 mins)...")
            print("Waiting for workers stack creation to complete...")

            try:
                # This is a water which waits for the stack deployment to complete.
                waiter = cf.get_waiter('stack_create_complete')
                res = waiter.wait(
                    StackName=self._workers_name
                )
            except:
                print("Gave up waiting for stack to create.")
                sys.exit(1)
            print("Worker stack created.")

        stack = cf.describe_stacks(StackName=self._workers_name)
        node_instance_role=None
        # We need NodeInstanceRole output.
        for v in stack["Stacks"][0]["Outputs"]:
            if v["OutputKey"] == "NodeInstanceRole": node_instance_role=v["OutputValue"]

        print("Node instance role: %s" % node_instance_role)
        print("*** Update worker auth.")

        config = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: aws-auth\n" + \
                "  namespace: kube-system\ndata:\n  mapRoles: |\n" + \
                "    - rolearn: " + node_instance_role + "\n" + \
                "      username: system:node:{{EC2PrivateDNSName}}\n" + \
                "      groups:\n        - system:bootstrappers\n        - system:nodes\n"

        print("Write config map...")
        worker_auth = os.path.dirname(self._output_path) + "/aws-auth-cm.yaml"
        with open(worker_auth, "w") as f:
            f.write(config)
        
        # add the worker resource to cluster
        resp = subprocess.call(["kubectl", "--kubeconfig=%s" % self._output_path, "apply",
                                "-f", worker_auth])
        if resp != 0:
            print("The kubectl command didn't work.")
            sys.exit(1)
        
        print("kube config generated. Try:")
        print("  kubectl --kubeconfig=%s get nodes" % self._output_path)

    def write_kube_config(self, cluster_cert, cluster_ep, path):
        print("*** EKS configuration.")
        # This section creates a Kubernetes kubectl configuration file if one does
        # not exist.
        if os.path.isfile(path):
            print("Config exists already.")
        else:
            cluster_config = {
                "apiVersion": "v1",
                "kind": "Config",
                "clusters": [
                    {
                        "cluster": {
                            "server": str(cluster_ep),
                            "certificate-authority-data": str(cluster_cert)
                        },
                        "name": "kubernetes"
                    }
                ],
                "contexts": [
                    {
                        "context": {
                            "cluster": "kubernetes",
                            "user": "aws"
                        },
                        "name": "aws"
                    }
                ],
                "current-context": "aws",
                "preferences": {},
                "users": [
                    {
                        "name": "aws",
                        "user": {
                            "exec": {
                                "apiVersion": "client.authentication.k8s.io/v1alpha1",
                                "command": "aws-iam-authenticator",
                                "args": [
                                    "token", "-i", self._cluster_name
                                ]
                            }
                        }
                    }
                ]
            }

            # Write in YAML.
            config_text=yaml.dump(cluster_config, default_flow_style=False)
            open(path, "w").write(config_text)
            print("Written to %s." % path)


class AliyunLauncher(Launcher):
    def __init__(self,
                 access_key_id=None,
                 secret_access_key=None,
                 region=None,
                 cluster_name=None,
                 k8s_version="v1.18.8-aliyun.1",
                 instance_type="ecs.n4.large",
                 node_num=2,
                 output_path=None):
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key
        self._region = region
        self._cluster_name = cluster_name
        self._k8s_version = k8s_version
        self._vpc_name = cluster_name + "-vpc"
        self._instance_type = instance_type
        self._node_num = node_num
        self._output_path = output_path
    
    def get_vpc(self):
        print("*** VPC")
        config = open_api_models.Config(
            access_key_id=self._access_key_id,
            access_key_secret=self._secret_access_key
        )
        config.endpoint = 'vpc.aliyuncs.com'
        client = Vpc20160428Client(config)
        
        # check vpc exists
        describe_vpcs_request = vpc_20160428_models.DescribeVpcsRequest(
            region_id=self._region,
            vpc_name=self._vpc_name
        )
        desc_response = client.describe_vpcs(describe_vpcs_request)
        if desc_response.body.total_count > 0:
            print("VPC already exists.")
            vpc_id = desc_response.body.vpcs.vpc[0].vpc_id
            vswitch_ids=desc_response.body.vpcs.vpc[0].v_switch_ids.v_switch_id
        else:
            # create vpc
            print("Creating VPC and Switch...")
            create_vpc_request = vpc_20160428_models.CreateVpcRequest(
                region_id=self._region,
                vpc_name=self._vpc_name,
                cidr_block='192.168.0.0/16'
            )
            vpc_id = client.create_vpc(create_vpc_request).body.vpc_id

            # check vpc create complete
            describe_vpcs_request = vpc_20160428_models.DescribeVpcsRequest(
                vpc_id=vpc_id,
                region_id=self._region
            )
            while True:
                res = client.describe_vpcs(describe_vpcs_request)
                if res.body.vpcs.vpc[0].status == "Available":
                    break

            # get zones of region
            print("Creating switch...")
            describe_zones_request = vpc_20160428_models.DescribeZonesRequest(region_id=self._region)
            desc_zone_res = client.describe_zones(describe_zones_request)
            zone_id = desc_zone_res.body.zones.zone[0].zone_id
            # create switch
            create_vswitch_request = vpc_20160428_models.CreateVSwitchRequest(
                zone_id=zone_id,
                cidr_block='192.168.0.0/19',
                vpc_id=vpc_id,
            )
            vswitch_res = client.create_vswitch(create_vswitch_request)
            vswitch_ids = [vswitch_res.body.v_switch_id]

        print("Get vpc ID: " + vpc_id)
        print("Get vswitch ids: " + str(vswitch_ids))
        return vpc_id, vswitch_ids
    
    def create_cluster(self):
        config = open_api_models.Config(
            access_key_id=self._access_key_id,
            access_key_secret=self._secret_access_key
        )
        config.endpoint = "cs.%s.aliyuncs.com" % self._region
        client = CS20151215Client(config)

        # check cluster exists
        describe_clusters_v1request = cs20151215_models.DescribeClustersV1Request(
            name=self._cluster_name
        )
        clusters = client.describe_clusters_v1(describe_clusters_v1request).body.clusters
        if len(clusters) > 0:
            print("Cluster already exists.")
            cluster_id = clusters[0].cluster_id
        else:
            print("Create key-pair.")
            config.endpoint = "ecs.%s.aliyuncs.com" % self._region
            ecs = Ecs20140526Client(config)
            create_key_pair_request = ecs_20140526_models.CreateKeyPairRequest(
                region_id=self._region,
                key_pair_name=self._cluster_name + '-KeyPair'
            )
            ecs.create_key_pair(create_key_pair_request)

            print("Creating Cluster.")
            taint_0 = cs20151215_models.Taint()
            addon_0 = cs20151215_models.Addon()
            runtime = cs20151215_models.Runtime()
            vpc_id, vswitch_ids = self.get_vpc()
            create_cluster_request = cs20151215_models.CreateClusterRequest(
                name=self._cluster_name,
                region_id=self._region,
                cluster_type='ManagedKubernetes',
                runtime=runtime,
                vpcid=vpc_id,
                container_cidr='172.20.0.0/16',
                service_cidr='172.21.0.0/20',
                num_of_nodes=self._node_num,
                key_pair=self._cluster_name + '-KeyPair',
                addons=[
                    addon_0
                ],
                taints=[
                    taint_0
                ],
                vswitch_ids=vswitch_ids,
                worker_instance_types=[
                    self._instance_type
                ],
                worker_system_disk_size=120,
                kubernetes_version=self._k8s_version,
                endpoint_public_access=True,
            )
            response = client.create_cluster(create_cluster_request)
            cluster_id = response.body.cluster_id
            # Wait for two minutes before doing the checking.
            time.sleep(120)
            self.wait_cluster_ready(client, cluster_id)

        # get kube config of cluster
        describe_cluster_user_kubeconfig_request = cs20151215_models.DescribeClusterUserKubeconfigRequest()
        config = client.describe_cluster_user_kubeconfig(cluster_id, describe_cluster_user_kubeconfig_request).body.config
        with open(self._output_path, "w") as f:
            f.write(config)

        print("kube config generated. Try:")
        print("  kubectl --kubeconfig=%s get nodes" % self._output_path)
    
    def wait_cluster_ready(self, client, cluster_id):
        # Going to give up after 40 times 20 seconds.  
        cnt=40
        while True:
            # Wait 20 seconds
            time.sleep(20)
            # Get cluster status
            response = client.describe_cluster_detail(cluster_id)
            status = response.body.state
            print("Cluster status: %s" % status)
            # Is it active? If so break out of loop
            if status == "running":
                break
            elif status == "failed":
                print("Cluster create failed.")
                sys.exit(1)
            # Maybe give up after so many goes.
            cnt = cnt - 1
            if cnt <= 0:
                print("Given up waiting for cluster to go RINNING.")
                sys.exit(1)


if __name__ == "__main__":
    """Parse command line flags and do operations."""
    parser = argparse.ArgumentParser(
        description="Script to launch a cluster on aliyun or aws."
    )
    parser.add_argument("--type", type=str)
    args = parser.parse_args()

    if args.type == "aws":
        kwargs = process_args(args.type)
        launcher = AWSLauncher(**kwargs)
    elif args.type == "aliyun":
        kwargs = process_args(args.type)
        launcher = AliyunLauncher(**kwargs)
    else:
        print("Not support cloud type %s" % args.type)

    launcher.create_cluster()

