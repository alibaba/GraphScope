#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Create a Kubernetes cluster on AWS/Aliyun using python sdk.  
#
# Notice: this script would require your AWS/Aliyun account's access_key_id and secret_access_key
# to get access your clusters' information or create a Kubernetes cluster automatically, finaly output
# a kube config of cluster. 
# 
# This assumes that kubectl and aws-iam-authenticator are in your PATH.
# 
# This script works incrementally.  It creates things in sequence, and assumes
# that if they already exist, they're good.  If you get things breaking
# during deployment, you'll want to make sure you don't have something
# broken.
# 
#


import json
import os
import sys
import subprocess
import time
import yaml

import click

try:
    import boto3
except ImportError:
    boto3 = None

try:
    from alibabacloud_cs20151215.client import Client as CS20151215Client
    from alibabacloud_ecs20140526.client import Client as Ecs20140526Client
    from alibabacloud_tea_openapi import models as open_api_models
    from alibabacloud_cs20151215 import models as cs20151215_models
    from alibabacloud_ecs20140526 import models as ecs_20140526_models
    from alibabacloud_vpc20160428.client import Client as Vpc20160428Client
    from alibabacloud_vpc20160428 import models as vpc_20160428_models
except ImportError:
    open_api_models = None
    CS20151215Client = None 
    Ecs20140526Client = None
    Vpc20160428Client = None


class Launcher(object):
    def launch_cluster(self):
        clusters = self._list_clusters()
        if clusters and click.confirm("Do you want to use existed clusters %s" % str(clusters)):
            cluster = click.prompt("The cluster name you want to use", 
                                   type=click.Choice(clusters, case_sensitive=False))
        else:
            config = self._get_cluster_config()
            cluster = self._create_cluster(**config)
        
        self._write_kube_config(cluster)
    
    def _list_clusters(self):
        raise NotImplementedError
    
    def _get_cluster_config(self):
        raise NotImplementedError

    def _create_cluster(**kw):
        raise NotImplementedError
    
    def _write_kube_config(self, cluster):
        raise NotImplementedError


class AWSLauncher(Launcher):
    vpc_template = "https://s3.us-west-2.amazonaws.com/amazon-eks/cloudformation/2020-10-29/amazon-eks-vpc-sample.yaml"
    workers_template = "https://s3.us-west-2.amazonaws.com/amazon-eks/cloudformation/2020-10-29/amazon-eks-nodegroup.yaml"

    def __init__(self,
                 access_key_id=None,
                 secret_access_key=None,
                 region=None,
                 output_path=None):
        sess = boto3.session.Session(aws_access_key_id=access_key_id,
                                     aws_secret_access_key=secret_access_key,
                                     region_name=region)
        self._eks = sess.client("eks")
        self._cf = sess.client("cloudformation")
        self._iam = sess.client("iam")
        self._ec2 = sess.client("ec2")
        self._region = region
        self._output_path = output_path
    
    def _list_clusters(self):
        list_clusters_res = self._eks.list_clusters()
        clusters = [arn.split("/")[-1] for arn in list_clusters_res["clusters"]]
        return clusters
    
    def _get_cluster_config(self):
        config = {}
        config["cluster_name"] = click.prompt("The cluster name you want to create")
        config["k8s_version"] = click.prompt("k8s version", type=click.Choice(["1.15", "1.16", "1.17", "1.18", "1.19"]),
                                             default="1.18")
        config["instance_type"] = click.prompt("Worker node instance type, defalut", default="t4g.large")
        config["node_num"] = click.prompt("Worker node num, default", type=int, default=4)
        return config
                    
    def _create_cluster(self, cluster_name=None, k8s_version=None, instance_type=None,
                        node_num=4, **kw):
        click.echo("*** EKS cluster")
        vpc_name = cluster_name + "-vpc"
        role_name = cluster_name + "-role"
        vpc_meta = self.get_vpc_stack(vpc_name)
        role_arn = self.get_role(role_name)

        click.echo("Creating cluster (ETA ~10 minutes)...")
        # Creating Kubernetes cluster.
        response = self._eks.create_cluster(
            name=cluster_name,
            version=k8s_version,
            roleArn=role_arn,
            resourcesVpcConfig={
                "subnetIds": vpc_meta["subnet_ids"],
                "securityGroupIds": [vpc_meta["security_group"]],
                'endpointPublicAccess': True
            },
        )
        click.echo("Cluster creation initiated.")
        click.echo("Waiting for completion (ETA 10 minutes)...")
        try:
            waiter = self._eks.get_waiter('cluster_active')
            res = waiter.wait(name=cluster_name)
        except Exception as e:
            click.echo("Error: %s, gave up waiting for cluster to create." % str(e))
            sys.exit(1)
        click.echo("Cluster active.")

        # write kube config file
        self._write_kube_config(cluster_name)

        # Create worker stack
        self.create_worker_stack(cluster_name, vpc_meta["id"], vpc_meta["security_group"], vpc_meta["subnet_ids"],
                                 instance_type, node_num)
        
        return cluster_name
    
    def get_role(self, role_name):
        click.echo("*** IAM role")
        try:
            # See if role exists.
            role = self._iam.get_role(RoleName=role_name)
            click.echo("IAM role exists.")
        except:
            click.echo("IAM role does not exist.  Creating...")
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
            self._iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=trust_policy,
                Description="Role providing access to EKS resources from EKS"
            )
            # Add policies allowing access to EKS API.
            self._iam.attach_role_policy(
                RoleName=role_name,
                PolicyArn="arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
            )
            self._iam.attach_role_policy(
                RoleName=role_name,
                PolicyArn="arn:aws:iam::aws:policy/AmazonEKSServicePolicy"
            )

        role = self._iam.get_role(RoleName=role_name)
        return role["Role"]["Arn"]
    
    def stack_exists(self, name):
        try:
            stack = self._cf.describe_stacks(StackName=name)
            return True
        except:
            return False

    def get_vpc_stack(self, vpc_name):
        # The VPC stack is a VPC and subnetworks to allow K8s communication.
        click.echo("*** VPC stack")
        if self.stack_exists(vpc_name):
            # stack exists, do nothing.
            click.echo("VPC stack already exists.")
        else:
            click.echo("Creating VPC stack...")
            # Create VPC stack.
            response = self._cf.create_stack(
                StackName=vpc_name,
                TemplateURL=self.vpc_template,
                Parameters=[],
                TimeoutInMinutes=15,
                OnFailure='DELETE'
            )
            if response == None:
                click.echo("Response is None, create VPC stack failed.")
                sys.exit(1)
            if not "StackId" in response:
                click.echo("StackId not in response, create VPC stack failed.")
                sys.exit(1)

            # Get stack ID for later.
            stack_id=response["StackId"]

            click.echo("Created stack " + vpc_name)
            click.echo("Waiting for VPC stack creation to complete...")

            try:
                # A waiter is something which polls AWS to find out if an operation
                # has completed.
                waiter = self._cf.get_waiter('stack_create_complete')
                # Wait for stack creation to complet
                res = waiter.wait(StackName=vpc_name)
            except Exception as e:
                # If waiter fails, that'll be the thing taking too long to deploy.
                click.echo("Error: %s, gave up waiting for stack to create." % str(e))
                sys.exit(1)
            click.echo("VPC stack created")

        # Get output information from the stack: VPC ID, security group and subnet IDs.
        stack = self._cf.describe_stacks(StackName=vpc_name)
        vpc_sg=None
        vpc_subnet_ids=None
        vpc_id=None
        # Loop over outputs grabbing information.
        for v in stack["Stacks"][0]["Outputs"]:
            if v["OutputKey"] == "SecurityGroups": vpc_sg=v["OutputValue"]
            if v["OutputKey"] == "VpcId": vpc_id=v["OutputValue"]
            if v["OutputKey"] == "SubnetIds": vpc_subnet_ids=v["OutputValue"]

        click.echo("VPC ID: %s" % vpc_id)
        click.echo("VPC security group: %s" % vpc_sg)
        click.echo("VPC subnet IDs: %s" % vpc_subnet_ids)
        # Split subnet IDs - it's comma separated.
        vpc_subnet_ids = vpc_subnet_ids.split(",")
        vpc_meta = {
            "security_group": vpc_sg,
            "subnet_ids": vpc_subnet_ids,
            "id": vpc_id,
        }

        return vpc_meta 

    def create_worker_stack(self, cluster_name, vpc_id, vpc_sg, vpc_subnet_ids, instance_type, node_num):
        # a stack of worker instances is created using CloudFormation.
        click.echo("*** Workers stack.")
        workers_name = cluster_name + "-workers"
        if self.stack_exists(workers_name):
            click.echo("Workers stack already exists.")
        else:
            click.echo("Creating workers stack...")
            keypair_name = cluster_name + "-keypair"
            # Create key pair
            resp = self._ec2.create_key_pair(KeyName=keypair_name)

            # Create stack
            response = self._cf.create_stack(
                StackName=workers_name,
                TemplateURL=self.workers_template,
                Capabilities=["CAPABILITY_IAM"],
                Parameters=[
                    {
                        "ParameterKey": "ClusterName",
                        "ParameterValue": cluster_name
                    },
                    {
                        "ParameterKey": "ClusterControlPlaneSecurityGroup",
                        "ParameterValue": vpc_sg
                    },
                    {
                        "ParameterKey": "NodeGroupName",
                        "ParameterValue": cluster_name + "-worker-group"
                    },
                    {
                        "ParameterKey": "NodeAutoScalingGroupMinSize",
                        "ParameterValue": str(1)
                    },
                    {
                        "ParameterKey": "NodeAutoScalingGroupMaxSize",
                        "ParameterValue": str(node_num)
                    },
                    {
                        "ParameterKey": "NodeInstanceType",
                        "ParameterValue": instance_type
                    },
                    {
                        "ParameterKey": "KeyName",
                        "ParameterValue": keypair_name
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

            if response is None:
                click.echo("Response is None, create worker group stack failed.")
                sys.exit(1)
            if not "StackId" in response:
                click.echo("StackId not in response, create worker group stack failed.")
                sys.exit(1)
            click.echo("Initiated workers (ETA 5-20 mins)...")
            click.echo("Waiting for workers stack creation to complete...")

            try:
                # This is a water which waits for the stack deployment to complete.
                waiter = self._cf.get_waiter('stack_create_complete')
                res = waiter.wait(
                    StackName=workers_name
                )
            except Exception as e:
                click.echo("Error: %s, gave up waiting for stack to create." % str(e))
                sys.exit(1)
            click.echo("Worker stack created.")

        stack = self._cf.describe_stacks(StackName=workers_name)
        node_instance_role=None
        # We need NodeInstanceRole output.
        for v in stack["Stacks"][0]["Outputs"]:
            if v["OutputKey"] == "NodeInstanceRole": node_instance_role=v["OutputValue"]

        click.echo("Node instance role: %s" % node_instance_role)
        click.echo("*** Update worker auth.")

        config = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: aws-auth\n" + \
                "  namespace: kube-system\ndata:\n  mapRoles: |\n" + \
                "    - rolearn: " + node_instance_role + "\n" + \
                "      username: system:node:{{EC2PrivateDNSName}}\n" + \
                "      groups:\n        - system:bootstrappers\n        - system:nodes\n"

        click.echo("Write config map...")
        worker_auth_file =  os.path.dirname(self._output_path) + "/aws-auth-cm.yaml"
        with open(worker_auth_file, "w") as f:
            f.write(config)
        
        # add the worker resource to cluster
        try:
            subprocess.run(["kubectl", "--kubeconfig=%s" % self._output_path, "apply",
                           "-f", worker_auth_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except Exception as e:
            click.echo("Error: %s" % str(e)) 
            sys.exits(1)
        
        click.echo("kube config generated. Try:")
        click.echo("  kubectl --kubeconfig=%s get nodes" % self._output_path)

    def _write_kube_config(self, cluster_name):
        click.echo("*** EKS configuration.")
        # Get cluster stuff
        cluster = self._eks.describe_cluster(name=cluster_name)

        # This spots the case where the cluster isn't in an expected state.
        status = cluster["cluster"]["status"]
        if status != "ACTIVE":
            click.echo("Cluster status %s, should be ACTIVE!" % status)
            sys.exit(1)

        # Get cluster endpoint and security info.
        cluster_cert = cluster["cluster"]["certificateAuthority"]["data"]
        cluster_ep = cluster["cluster"]["endpoint"]
        click.echo("Cluster: %s" % cluster_ep)
        # This section creates a Kubernetes kubectl configuration file if one does
        # not exist.
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
                                "token", "-i", cluster_name
                            ]
                        }
                    }
                }
            ]
        }

        # Write in YAML.
        config_text=yaml.dump(cluster_config, default_flow_style=False)
        with open(self._output_path, "w") as f:
            f.write(config_text)
        click.echo("Written to %s." % self._output_path)


class AliyunLauncher(Launcher):
    def __init__(self,
                 access_key_id=None,
                 secret_access_key=None,
                 region=None,
                 output_path=None):
        config = open_api_models.Config(
            access_key_id=access_key_id,
            access_key_secret=secret_access_key
        )
        config.endpoint = "cs.%s.aliyuncs.com" % region
        self._eks = CS20151215Client(config)
        config.endpoint = "ecs.%s.aliyuncs.com" % region
        self._ecs = Ecs20140526Client(config)
        config.endpoint = 'vpc.aliyuncs.com'
        self._vpc = Vpc20160428Client(config)
        self._region = region
        self._output_path = output_path

    def _list_clusters(self):
        describe_clusters_v1request = cs20151215_models.DescribeClustersV1Request()
        desc_clusters_res = self._eks.describe_clusters_v1(describe_clusters_v1request).body.clusters
        clusters = [item.name for item in desc_clusters_res]
        return clusters

    def _get_cluster_config(self):
        config = {}
        config["cluster_name"] = click.prompt("The cluster name you want to create")
        config["k8s_version"] = click.prompt("k8s version", type=click.Choice(["1.16.9-aliyun.1", "1.18.8-aliyun.1"], case_sensitive=False),
                                             default="1.18.8-aliyun.1")
        config["container_cidr"] = click.prompt("Container CIDR", default="172.20.0.0/16")
        config["service_cidr"] = click.prompt("Service CIDR", default="172.21.0.0/20")
        config["vpc_cidr_block"] = click.prompt("VPC CIDR block", default="192.168.0.0/16")
        config["vswitch_cidr_block"] = click.prompt("VSwitch CIDR block", default="192.168.0.0/19")
        config["instance_type"] = click.prompt("Worker node instance type",
                                               type=str, default="ecs.g5.large")
        config["node_num"] = click.prompt("Worker node num", type=int, default=4)
        return config

    def _create_cluster(self, cluster_name=None, k8s_version=None, container_cidr=None,
                        instance_type=None, service_cidr=None, node_num=2, **kw):
        keypair_name = cluster_name + '-keypair'
        click.echo("Create key-pair.")
        create_key_pair_request = ecs_20140526_models.CreateKeyPairRequest(
            region_id=self._region,
            key_pair_name=keypair_name
        )
        self._ecs.create_key_pair(create_key_pair_request)

        click.echo("Creating Cluster.")
        taint_0 = cs20151215_models.Taint()
        addon_0 = cs20151215_models.Addon()
        runtime = cs20151215_models.Runtime()
        vpc_name = cluster_name + "-vpc"
        vpc_cidr_block = kw.pop("vpc_cidr_block")
        vswitch_cidr_block = kw.pop("vswitch_cidr_block")
        vpc_id, vswitch_ids = self.get_vpc(vpc_name, vpc_cidr_block, vswitch_cidr_block)
        create_cluster_request = cs20151215_models.CreateClusterRequest(
            name=cluster_name,
            region_id=self._region,
            cluster_type='ManagedKubernetes',
            runtime=runtime,
            vpcid=vpc_id,
            container_cidr=container_cidr,
            service_cidr=service_cidr,
            num_of_nodes=node_num,
            key_pair=keypair_name,
            addons=[
                addon_0
            ],
            taints=[
                taint_0
            ],
            vswitch_ids=vswitch_ids,
            worker_instance_types=[
                instance_type
            ],
            worker_system_disk_size=120,
            kubernetes_version=k8s_version,
            endpoint_public_access=True,
        )
        response = self._eks.create_cluster(create_cluster_request)
        cluster_id = response.body.cluster_id
        # Wait for two minutes before doing the checking.
        time.sleep(120)
        self.wait_cluster_ready(cluster_id)

        return cluster_name

    def _write_kube_config(self, cluster_name):
        # get cluster id
        describe_clusters_v1request = cs20151215_models.DescribeClustersV1Request(
            name=cluster_name
        )
        desc_cluster_res = self._eks.describe_clusters_v1(describe_clusters_v1request).body.clusters[0]
        cluster_id = desc_cluster_res.cluster_id

        # get kube config of cluster
        describe_cluster_user_kubeconfig_request = cs20151215_models.DescribeClusterUserKubeconfigRequest()
        config = self._eks.describe_cluster_user_kubeconfig(cluster_id, describe_cluster_user_kubeconfig_request).body.config
        with open(self._output_path, "w") as f:
            f.write(config)

        click.echo("kube config generated. Try:")
        click.echo("  kubectl --kubeconfig=%s get nodes" % self._output_path)

    def get_vpc(self, name=None, vpc_cidr_block=None, vswitch_cidr_block=None):
        click.echo("*** VPC")
        # check vpc exists
        describe_vpcs_request = vpc_20160428_models.DescribeVpcsRequest(
            region_id=self._region,
            vpc_name=name
        )
        desc_response = self._vpc.describe_vpcs(describe_vpcs_request)
        if desc_response.body.total_count > 0:
            click.echo("VPC already exists.")
            vpc_id = desc_response.body.vpcs.vpc[0].vpc_id
            vswitch_ids=desc_response.body.vpcs.vpc[0].v_switch_ids.v_switch_id
        else:
            # create vpc
            click.echo("Creating VPC and Switch...")
            create_vpc_request = vpc_20160428_models.CreateVpcRequest(
                region_id=self._region,
                vpc_name=name,
                cidr_block=vpc_cidr_block
            )
            vpc_id = self._vpc.create_vpc(create_vpc_request).body.vpc_id

            # check vpc create complete
            describe_vpcs_request = vpc_20160428_models.DescribeVpcsRequest(
                vpc_id=vpc_id,
                region_id=self._region
            )
            while True:
                res = self._vpc.describe_vpcs(describe_vpcs_request)
                if res.body.vpcs.vpc[0].status == "Available":
                    break

            # get zones of region
            click.echo("Creating switch...")
            describe_zones_request = vpc_20160428_models.DescribeZonesRequest(region_id=self._region)
            desc_zone_res = self._vpc.describe_zones(describe_zones_request)
            zone_id = desc_zone_res.body.zones.zone[0].zone_id
            # create switch
            create_vswitch_request = vpc_20160428_models.CreateVSwitchRequest(
                zone_id=zone_id,
                cidr_block=vswitch_cidr_block,
                vpc_id=vpc_id,
            )
            vswitch_res = self._vpc.create_vswitch(create_vswitch_request)
            vswitch_ids = [vswitch_res.body.v_switch_id]
            
            # check vswitch create complete
            describe_vswitch_request = vpc_20160428_models.DescribeVSwitchAttributesRequest(
                v_switch_id=vswitch_ids[0]
            )
            while True:
                res = self._vpc.describe_vswitch_attributes(describe_vswitch_request)
                if res.body.status == "Available":
                    break

        click.echo("Get vpc ID: " + vpc_id)
        click.echo("Get vswitch ids: " + str(vswitch_ids))
        return vpc_id, vswitch_ids
    
    def wait_cluster_ready(self, cluster_id):
        click.echo("Cluster creation initiated.")
        click.echo("Waiting for completion (ETA 10 minutes)...")
        # Going to give up after 40 times 20 seconds.  
        cnt=40
        while True:
            # Wait 20 seconds
            time.sleep(20)
            # Get cluster status
            response = self._eks.describe_cluster_detail(cluster_id)
            status = response.body.state
            click.echo("Cluster status: %s" % status)
            # Is it active? If so break out of loop
            if status == "running":
                break
            elif status == "failed":
                click.echo("Cluster create failed.")
                sys.exit(1)
            # Maybe give up after so many goes.
            cnt = cnt - 1
            if cnt <= 0:
                click.echo("Given up waiting for cluster to go RINNING.")
                sys.exit(1)


@click.command()
@click.option("--cloud_type", type=click.Choice(["aws", "aliyun"], case_sensitive=False), 
              help="Cloud type to launch cluster.")
def launch(cloud_type):
    """Utility script to launch cluster on AWS or Aliyun and output kube config file."""

    access_key_id = click.prompt("Your access_key_id", type=str)
    secret_access_key = click.prompt("Your secret_access_key", type=str)
    region = click.prompt("Your region", type=str)
    output_path = click.prompt("kube config output path, default",
                               type=str, default=os.environ["HOME"] + "/.kube/config" )
    if cloud_type == "aws":
        launcher = AWSLauncher(access_key_id, secret_access_key, region, output_path)
    elif cloud_type == "aliyun":
        launcher = AliyunLauncher(access_key_id, secret_access_key, region, output_path)
    else:
        click.echo("Not support cloud type %s" % args.type)

    launcher.launch_cluster()


if __name__ == "__main__":
    launch()
