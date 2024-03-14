# Copyright 2022 Alibaba Group Holding Limited. All Rights Reserved.
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
# ==============================================================================

import yaml
from kubernetes import client
from kubernetes import watch
from kubernetes.client.rest import ApiException


def fill_params_in_yaml(file_path, params):
    with open(file_path, "r") as file:
        yaml_content = file.read()
        for param_key, param_value in params.items():
            yaml_content = yaml_content.replace(
                "${" + param_key + "}", str(param_value)
            )
        return yaml.safe_load(yaml_content)


def launch_client(api_instance, pytorch_job_manifest):
    group = pytorch_job_manifest["apiVersion"].split("/")[0]
    version = pytorch_job_manifest["apiVersion"].split("/")[1]
    name = pytorch_job_manifest["metadata"]["name"]
    namespace = pytorch_job_manifest["metadata"]["namespace"]
    plural = "pytorchjobs"  # This is PyTorchJob CRD's plural name

    try:
        # create PyTorchJob
        api_response = api_instance.create_namespaced_custom_object(
            group=group,
            version=version,
            namespace=namespace,
            plural=plural,
            body=pytorch_job_manifest,
        )
        print(api_response)
    except ApiException as e:
        print(
            f"Exception when calling CustomObjectsApi->create_namespaced_custom_object: {e}"
        )

    # set Watcher to monitor the state of the PyTorchJob
    w = watch.Watch()

    # loop checking the state of PyTorchJob
    for event in w.stream(
        api_instance.list_namespaced_custom_object, group, version, namespace, plural
    ):
        pytorch_job = event["object"]
        if pytorch_job.get("metadata", {}).get("name") == name:
            status = pytorch_job.get("status", {})
            if status:  # check status existence
                conditions = status.get("conditions", [])
                for condition in conditions:
                    if (
                        condition.get("type") == "Succeeded"
                        and condition.get("status") == "True"
                    ):
                        print(f"PyTorchJob {name} has succeeded!")
                        w.stop()
                        break
                    elif (
                        condition.get("type") == "Failed"
                        and condition.get("status") == "True"
                    ):
                        print(f"PyTorchJob {name} has failed!")
                        w.stop()
                        break

    # clear PyTorchJob
    print(f"Deleting PyTorchJob {name}...")
    try:
        response = api_instance.delete_namespaced_custom_object(
            group=group,
            name=name,
            version=version,
            plural=plural,
            namespace=namespace,
            body=client.V1DeleteOptions(
                propagation_policy="Foreground",
            ),
        )
        print(f"PyTorchJob {name} deleted. Response: {response}")
    except ApiException as e:
        print(
            f"Exception when calling CustomObjectsApi->delete_namespaced_custom_object: {e}"
        )
