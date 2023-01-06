import base64
import json
import logging
import os

try:
    from kubernetes import client as kube_client
    from kubernetes import config as kube_config
    from kubernetes import watch as kube_watch
    from kubernetes.client import AppsV1Api
    from kubernetes.client import CoreV1Api
    from kubernetes.client.rest import ApiException as K8SApiException
    from kubernetes.config import ConfigException as K8SConfigException
except ImportError:
    kube_client = None
    kube_config = None
    kube_watch = None
    AppsV1Api = None
    CoreV1Api = None
    K8SApiException = None
    K8SConfigException = None

from pprint import pprint

from graphscope.deploy.kubernetes.resource_builder import ResourceBuilder
from graphscope.deploy.kubernetes.utils import get_service_endpoints

from gscoordinator.version import __version__

logger = logging.getLogger("graphscope")


BASE_MACHINE_ENVS = {
    "MY_NODE_NAME": "spec.nodeName",
    "MY_POD_NAME": "metadata.name",
    "MY_POD_NAMESPACE": "metadata.namespace",
    "MY_POD_IP": "status.podIP",
    "MY_HOST_NAME": "status.podIP",
}


_annotations = {
    "service.beta.kubernetes.io/alibaba-cloud-loadbalancer-health-check-type": "tcp",
    "service.beta.kubernetes.io/alibaba-cloud-loadbalancer-health-check-connect-timeout": "8",
    "service.beta.kubernetes.io/alibaba-cloud-loadbalancer-healthy-threshold": "2",
    "service.beta.kubernetes.io/alibaba-cloud-loadbalancer-unhealthy-threshold": "2",
    "service.beta.kubernetes.io/alibaba-cloud-loadbalancer-health-check-interval": "1",
}


class EngineCluster:
    def __init__(
        self,
        engine_cpu,
        engine_mem,
        engine_pod_node_selector,
        glog_level,
        image_pull_policy,
        image_pull_secrets,
        image_registry,
        image_repository,
        image_tag,
        instance_id,
        namespace,
        num_workers,
        preemptive,
        service_type,
        vineyard_cpu,
        vineyard_daemonset,
        vineyard_image,
        vineyard_mem,
        vineyard_shared_mem,
        volumes,
        with_analytical,
        with_analytical_java,
        with_dataset,
        with_interactive,
        with_learning,
        with_mars,
    ):
        self._gs_prefix = "gs-engine-"
        self._analytical_prefix = "gs-analytical-"
        self._interactive_frontend_prefix = "gs-interactive-frontend-"

        self._learning_prefix = "gs-learning-"
        self._learning_service_name_prefix = "gs-graphlearn-service-"

        self._vineyard_prefix = "vineyard-"
        self._vineyard_service_name_prefix = "gs-vineyard-service-"

        self._mars_scheduler_name_prefix = "mars-scheduler-"
        self._mars_service_name_prefix = "mars-"

        self._instance_id = instance_id

        self._namespace = namespace
        self._engine_labels = {
            "app.kubernetes.io/name": "graphscope",
            "app.kubernetes.io/instance": self._instance_id,
            "app.kubernetes.io/version": __version__,
            "app.kubernetes.io/component": "engine",
        }
        self._frontend_labels = self._engine_labels.copy()
        self._frontend_labels["app.kubernetes.io/component"] = "frontend"

        self._with_dataset = with_dataset
        if not image_registry:
            image_prefix = image_repository
        else:
            image_prefix = f"{image_registry}/{image_repository}"
        self._analytical_image = f"{image_prefix}/analytical:{image_tag}"
        self._analytical_java_image = f"{image_prefix}/analytical-java:{image_tag}"
        self._interactive_frontend_image = (
            f"{image_prefix}/interactive-frontend:{image_tag}"
        )
        self._interactive_executor_image = (
            f"{image_prefix}/interactive-executor:{image_tag}"
        )
        self._learning_image = f"{image_prefix}/learning:{image_tag}"
        self._dataset_image = f"{image_prefix}/dataset:{image_tag}"

        self._vineyard_image = vineyard_image

        self._image_pull_policy = image_pull_policy
        self._image_pull_secrets = image_pull_secrets

        self._vineyard_daemonset = vineyard_daemonset

        if with_analytical and with_analytical_java:
            logger.warning(
                "Cannot setup `with_analytical` and `with_analytical_java` at the same time"
            )
            logger.warning("Disabled `analytical`.")
            self._with_analytical = False

        self._with_analytical = with_analytical
        self._with_analytical_java = with_analytical_java
        self._with_interactive = with_interactive
        self._with_learning = with_learning
        self._with_mars = with_mars

        self._glog_level = glog_level
        self._preemptive = preemptive
        self._vineyard_shared_mem = vineyard_shared_mem

        self._node_selector = (
            json.loads(self.base64_decode(engine_pod_node_selector))
            if engine_pod_node_selector
            else None
        )
        self._num_workers = num_workers
        self._volumes = json.loads(self.base64_decode(volumes)) if volumes else None

        self._sock = "/tmp/vineyard_workspace/vineyard.sock"

        self._vineyard_requests = {"cpu": vineyard_cpu, "memory": vineyard_mem}
        self._analytical_requests = {"cpu": engine_cpu, "memory": engine_mem}
        self._executor_requests = {"cpu": "2000m", "memory": engine_mem}
        self._learning_requests = {"cpu": "1000m", "memory": "256Mi"}
        self._frontend_requests = {"cpu": "200m", "memory": "512Mi"}
        self._dataset_requests = {"cpu": "200m", "memory": "64Mi"}

        self._service_type = service_type
        self._vineyard_service_port = 9600  # fixed
        self._etcd_port = 2379

        # This must be same with v6d:modules/io/python/drivers/io/kube_ssh.sh
        self.analytical_container_name = "engine"
        self.interactive_frontend_container_name = "frontend"
        self.interactive_executor_container_name = "executor"
        self.learning_container_name = "learning"
        self.dataset_container_name = "dataset"
        self.mars_container_name = "mars"
        self.vineyard_container_name = "vineyard"

    @property
    def vineyard_ipc_socket(self):
        return self._sock

    def base64_decode(self, string):
        return base64.b64decode(string).decode("utf-8")

    def get_common_env(self):
        def put_if_exists(env: dict, key: str):
            if key in os.environ:
                env[key] = os.environ[key]

        env = {
            "GLOG_v": str(self._glog_level),
            "VINEYARD_IPC_SOCKET": self.vineyard_ipc_socket,
            "WITH_VINEYARD": "ON",
        }
        put_if_exists(env, "OPAL_PREFIX")
        put_if_exists(env, "OPAL_BINDIR")
        env = [kube_client.V1EnvVar(name=k, value=v) for k, v in env.items()]
        return env

    def get_base_machine_env(self):
        env = [
            ResourceBuilder.get_value_from_field_ref(key, value)
            for key, value in BASE_MACHINE_ENVS.items()
        ]
        return env

    def get_vineyard_socket_volume(self):
        name = "vineyard-ipc-socket"
        volume = kube_client.V1Volume(name=name)
        if self._vineyard_daemonset is None:
            empty_dir = kube_client.V1EmptyDirVolumeSource()
            volume.empty_dir = empty_dir
        else:
            path = f"/var/run/vineyard-{self._namespace}-{self._vineyard_daemonset}"
            host_path = kube_client.V1HostPathVolumeSource(path=path)
            host_path.type = "Directory"
            volume.host_path = host_path

        source_volume_mount = kube_client.V1VolumeMount(
            name=name, mount_path="/tmp/vineyard_workspace"
        )
        destination_volume_mount = source_volume_mount

        return volume, source_volume_mount, destination_volume_mount

    def get_shm_volume(self):
        name = "host-shm"
        volume = kube_client.V1Volume(name=name)
        volume.empty_dir = kube_client.V1EmptyDirVolumeSource()
        volume.empty_dir.medium = "Memory"

        source_volume_mount = kube_client.V1VolumeMount(
            name=name, mount_path="/dev/shm"
        )
        destination_volume_mount = source_volume_mount

        return volume, source_volume_mount, destination_volume_mount

    def get_dataset_volume(self):
        name = "dataset"
        volume = kube_client.V1Volume(name=name)
        volume.empty_dir = kube_client.V1EmptyDirVolumeSource()

        source_volume_mount = kube_client.V1VolumeMount(
            name=name, mount_path="/dataset"
        )
        source_volume_mount.mount_propagation = "Bidirectional"

        # volume mount in engine container
        destination_volume_mount = kube_client.V1VolumeMount(
            name=name, mount_path="/dataset"
        )
        destination_volume_mount.read_only = True
        destination_volume_mount.mount_propagation = "HostToContainer"

        return volume, source_volume_mount, destination_volume_mount

    def get_engine_container_helper(
        self, name, image, args, volume_mounts, requests, limits
    ):
        container = kube_client.V1Container(
            name=name, image=image, args=args, volume_mounts=volume_mounts
        )
        container.image_pull_policy = self._image_pull_policy
        # container.env = self.get_common_env() + self.get_base_machine_env()
        container.env = self.get_common_env()
        container.resources = ResourceBuilder.get_resources(
            requests, None, self._preemptive
        )
        return container

    def get_analytical_container(self, volume_mounts, with_java=False):
        name = self.analytical_container_name
        image = self._analytical_image if not with_java else self._analytical_java_image
        args = ["tail", "-f", "/dev/null"]
        container = self.get_engine_container_helper(
            name,
            image,
            args,
            volume_mounts,
            self._analytical_requests,
            self._analytical_requests,
        )

        readiness_probe = kube_client.V1Probe()
        command = ["/bin/bash", "-c", f"ls {self._sock} 2>/dev/null"]
        readiness_probe._exec = kube_client.V1ExecAction(command=command)
        readiness_probe.initial_delay_seconds = 5
        readiness_probe.period_seconds = 2
        readiness_probe.failure_threshold = 3
        container.readiness_probe = readiness_probe

        # container.lifecycle = self.get_lifecycle()
        return container

    def get_interactive_executor_container(self, volume_mounts):
        name = self.interactive_executor_container_name
        image = self._interactive_executor_image
        args = ["tail", "-f", "/dev/null"]
        container = self.get_engine_container_helper(
            name,
            image,
            args,
            volume_mounts,
            self._executor_requests,
            self._executor_requests,
        )
        return container

    def get_learning_container(self, volume_mounts):
        name = self.learning_container_name
        image = self._learning_image
        args = ["tail", "-f", "/dev/null"]
        container = self.get_engine_container_helper(
            name,
            image,
            args,
            volume_mounts,
            self._learning_requests,
            self._learning_requests,
        )
        return container

    def get_vineyard_container(self, volume_mounts):
        name = self.vineyard_container_name
        image = self._vineyard_image
        sts_name = self.engine_stateful_set_name
        svc_name = sts_name + "-headless"
        pod0_dns = f"{sts_name}-0.{svc_name}.{self._namespace}.svc.cluster.local"
        vineyard_cmd = (
            f"vineyardd -size {self._vineyard_shared_mem} -socket {self._sock}"
        )
        args = f"""
            [[ `hostname` =~ -([0-9]+)$ ]] || exit 1;
            ordinal=${{BASH_REMATCH[1]}};
            if (( $ordinal == 0 )); then
                {vineyard_cmd} -etcd_endpoint http://0.0.0.0:{self._etcd_port}
            else
                until nslookup {pod0_dns}; do sleep 1; done;
                {vineyard_cmd} -etcd_endpoint http://{pod0_dns}:{self._etcd_port}
            fi;
            """
        args = ["bash", "-c", args]
        container = self.get_engine_container_helper(
            name,
            image,
            args,
            volume_mounts,
            self._vineyard_requests,
            self._vineyard_requests,
        )
        container.ports = [
            kube_client.V1ContainerPort(container_port=self._vineyard_service_port),
            kube_client.V1ContainerPort(container_port=self._etcd_port),
        ]
        return container

    def get_mars_container(self):
        _ = self.mars_container_name
        return

    def get_dataset_container(self, volume_mounts):
        name = self.dataset_container_name
        container = kube_client.V1Container(name=name)
        container.image = self._dataset_image
        container.image_pull_policy = self._image_pull_policy

        container.resources = ResourceBuilder.get_resources(
            self._dataset_requests, self._dataset_requests
        )

        container.volume_mounts = volume_mounts

        container.security_context = kube_client.V1SecurityContext(privileged=True)
        return container

    def get_engine_pod_spec(self):
        containers = []
        volumes = []

        socket_volume = self.get_vineyard_socket_volume()
        shm_volume = self.get_shm_volume()

        volumes.extend([socket_volume[0], shm_volume[0]])
        if self._vineyard_daemonset is None:
            containers.append(
                self.get_vineyard_container(
                    volume_mounts=[socket_volume[1], shm_volume[1]]
                )
            )

        engine_volume_mounts = [socket_volume[2], shm_volume[2]]

        if self._volumes and self._volumes is not None:
            udf_volumes = ResourceBuilder.get_user_defined_volumes(self._volumes)
            volumes.extend(udf_volumes[0])
            engine_volume_mounts.extend(udf_volumes[2])

        if self._with_dataset:
            dataset_volume = self.get_dataset_volume()
            volumes.append(dataset_volume[0])
            containers.append(
                self.get_dataset_container(volume_mounts=[dataset_volume[1]])
            )
            engine_volume_mounts.append(dataset_volume[2])
        if self._with_analytical:
            containers.append(
                self.get_analytical_container(volume_mounts=engine_volume_mounts)
            )
        if self._with_analytical_java:
            containers.append(
                self.get_analytical_container(
                    volume_mounts=engine_volume_mounts, with_java=True
                )
            )
        if self._with_interactive:
            containers.append(
                self.get_interactive_executor_container(
                    volume_mounts=engine_volume_mounts
                )
            )
        if self._with_learning:
            containers.append(
                self.get_learning_container(volume_mounts=engine_volume_mounts)
            )
        if self._with_mars:
            containers.append(self.get_mars_container())
        return ResourceBuilder.get_pod_spec(
            containers=containers,
            image_pull_secrets=self._image_pull_secrets,
            node_selector=self._node_selector,
            volumes=volumes,
        )

    def get_engine_pod_template_spec(self):
        spec = self.get_engine_pod_spec()
        return ResourceBuilder.get_pod_template_spec(spec, self._engine_labels)

    def get_engine_stateful_set(self):
        name = self.engine_stateful_set_name
        template = self.get_engine_pod_template_spec()
        replicas = self._num_workers
        service_name = name + "-headless"
        spec = ResourceBuilder.get_stateful_set_spec(
            template, replicas, self._engine_labels, service_name
        )
        return ResourceBuilder.get_stateful_set(
            self._namespace, name, spec, self._engine_labels
        )

    def get_engine_headless_service(self):
        name = self.engine_stateful_set_name + "-headless"
        ports = [kube_client.V1ServicePort(name="etcd", port=self._etcd_port)]
        service_spec = ResourceBuilder.get_service_spec(
            "ClusterIP", ports, self._engine_labels, None
        )
        # Necessary, create a headless service for statefulset
        service_spec.cluster_ip = "None"
        service = ResourceBuilder.get_service(
            self._namespace, name, service_spec, self._engine_labels
        )
        return service

    def get_vineyard_service(self):
        service_type = self._service_type
        name = f"{self._vineyard_prefix}{self._instance_id}"
        ports = [kube_client.V1ServicePort(name=name, port=self._vineyard_service_port)]
        service_spec = ResourceBuilder.get_service_spec(
            service_type, ports, self._engine_labels, None
        )
        service = ResourceBuilder.get_service(
            self._namespace, name, service_spec, self._engine_labels
        )
        return service

    def get_learning_service(self, object_id, start_port):
        service_type = self._service_type
        num_workers = self._num_workers
        name = f"{self._learning_prefix}{object_id}"
        ports = []
        for i in range(start_port, start_port + num_workers):
            port = kube_client.V1ServicePort(name=f"{name}-{i}", port=i, protocol="TCP")
            ports.append(port)
        service_spec = ResourceBuilder.get_service_spec(
            service_type, ports, self._engine_labels, "Local"
        )
        service = ResourceBuilder.get_service(
            self._namespace, name, service_spec, self._engine_labels
        )
        return service

    def get_learning_ports(self, start_port):
        num_workers = self._num_workers
        return [i for i in range(start_port, start_port + num_workers)]

    @property
    def engine_stateful_set_name(self):
        return f"{self._gs_prefix}{self._instance_id}"

    @property
    def frontend_deployment_name(self):
        return f"{self._interactive_frontend_prefix}{self._instance_id}"

    @property
    def vineyard_service_name(self):
        return f"{self._vineyard_prefix}{self._instance_id}"

    def get_vineyard_service_endpoint(self, api_client):
        # return f"{self.vineyard_service_name}:{self._vineyard_service_port}"
        service_type = self._service_type
        service_name = self.vineyard_service_name
        endpoints = get_service_endpoints(
            api_client=api_client,
            namespace=self._namespace,
            name=service_name,
            service_type=service_type,
        )
        assert len(endpoints) > 0
        return endpoints[0]

    def get_learning_service_name(self, object_id):
        return f"{self._learning_service_name_prefix}{object_id}"

    def get_graphlearn_service_endpoint(self, api_client, object_id, pod_host_ip_list):
        service_name = self.get_learning_service_name(object_id)
        service_type = self._service_type
        core_api = kube_client.CoreV1Api(api_client)
        if service_type == "NodePort":
            # TODO: add label_selector to filter the service
            services = core_api.list_namespaced_service(self._namespace)
            for svc in services.items:
                if svc.metadata.name == service_name:
                    endpoints = []
                    for ip, port_spec in zip(pod_host_ip_list, svc.spec.ports):
                        endpoints.append(
                            (
                                f"{ip}:{port_spec.node_port}",
                                int(port_spec.name.split("-")[-1]),
                            )
                        )
                    endpoints.sort(key=lambda ep: ep[1])
                    return [ep[0] for ep in endpoints]
        elif service_type == "LoadBalancer":
            endpoints = get_service_endpoints(
                api_client=api_client,
                namespace=self._namespace,
                name=service_name,
                service_type=service_type,
            )
            return endpoints
        raise RuntimeError("Get graphlearn service endpoint failed.")

    def get_interactive_frontend_container(self):
        name = self.interactive_frontend_container_name
        image = self._interactive_frontend_image
        args = ["tail", "-f", "/dev/null"]
        container = kube_client.V1Container(name=name, image=image, args=args)
        container.image_pull_policy = self._image_pull_policy
        container.resources = ResourceBuilder.get_resources(
            self._frontend_requests, None
        )
        return container

    def get_interactive_frontend_deployment(self, replicas=1):
        name = self.frontend_deployment_name
        container = self.get_interactive_frontend_container()
        pod_spec = ResourceBuilder.get_pod_spec(containers=[container])
        template_spec = ResourceBuilder.get_pod_template_spec(
            pod_spec, self._frontend_labels
        )
        deployment_spec = ResourceBuilder.get_deployment_spec(
            template_spec, replicas, self._frontend_labels
        )
        return ResourceBuilder.get_deployment(
            self._namespace, name, deployment_spec, self._frontend_labels
        )

    def get_interactive_frontend_service(self, port):
        name = self.frontend_deployment_name
        service_type = self._service_type
        ports = [kube_client.V1ServicePort(name="gremlin", port=port)]
        service_spec = ResourceBuilder.get_service_spec(
            service_type, ports, self._frontend_labels, None
        )
        service = ResourceBuilder.get_service(
            self._namespace, name, service_spec, self._frontend_labels, _annotations
        )
        return service


class MarsCluster:
    def __init__(self, instance_id, namespace, service_type):
        self._mars_prefix = "mars-"
        self._mars_scheduler_port = 7103  # fixed
        self._mars_scheduler_web_port = 7104  # fixed
        self._mars_worker_port = 7105  # fixed
        self._instance_id = instance_id
        self._namespace = namespace
        self._service_type = service_type

        self._mars_worker_requests = {"cpu": "200m", "memory": "512Mi"}
        self._mars_scheduler_requests = {"cpu": "200m", "memory": "512Mi"}

    def get_mars_deployment(self):
        pass

    def get_mars_service(self):
        pass

    @property
    def mars_scheduler_service_name(self):
        return f"{self._mars_prefix}{self._instance_id}"

    @property
    def mars_scheduler_web_port(self):
        return self._mars_scheduler_web_port

    def get_mars_service_endpoint(self, api_client):
        # Always len(endpoints) >= 1
        service_name = self.mars_scheduler_service_name
        service_type = self._service_type
        web_port = self.mars_scheduler_web_port
        endpoints = get_service_endpoints(
            api_client=api_client,
            namespace=self._namespace,
            name=service_name,
            service_type=service_type,
            query_port=web_port,
        )
        assert len(endpoints) > 0
        return f"http://{endpoints[0]}"
