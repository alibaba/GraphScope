import time
from gs_interactive_admin.core.config import Config
import logging

from gs_interactive_admin.models.start_service_request import StartServiceRequest
from gs_interactive_admin.core.launcher.k8s_launcher import K8sLauncher
from gs_interactive_admin.core.launcher.abstract_launcher import InteractiveCluster
from gs_interactive_admin.core.metadata.metadata_store import get_metadata_store
import yaml
from gs_interactive_admin.util import get_current_time_stamp_ms

logger = logging.getLogger("interactive")


class ServiceManager(object):
    def __init__(self, config: Config):
        self._query_port = config.http_service.query_port
        self._admin_port = config.master.port
        # get start time in unix timestamp
        self._start_time = get_current_time_stamp_ms()
        self._bolt_port = config.compiler.endpoint.bolt_connector.port
        self._config = config
        if config.master.k8s_launcher_config:
            logger.info("Using K8sLauncher")
            self._launcher = K8sLauncher(config)

        self._clusters = {}  # graph_id -> cluster
        self._metadata_store = get_metadata_store()

    def check_service_ready(self):
        return "Service is ready"

    def get_service_status(self):
        return {
            "status": "running",
            "hqps_port": self._query_port,
            "bolt_port": self._bolt_port,
            "statistics_enabled": False,
            "graph": {},
            "start_time": self._start_time,
            "deploy_mode": "k8s",
        }

    def start_service(self, start_service_request: StartServiceRequest):
        """
        Start service on a specified graph. This assumes the graph has already been created.
        A new couple of pods will be created to serve the graph.
        TODO: Avoid creating new pods if the graph is already running.
        TODO: Delete the pod if the graph is deleted.

        Args:
            start_service_request (StartServiceRequest): _description_

        Returns:
            _type_: _description_
        """
        graph_id = start_service_request.graph_id
        logger.info("Starting service for graph %s", graph_id)
        if graph_id is None or graph_id == "":
            return "Invalid graph id"

        graph_meta = self._metadata_store.get_graph_meta(graph_id)
        if graph_meta is None:
            raise RuntimeError(f"Graph {graph_id} does not exist")

        # check whether the graph has been loaded with data
        if "remote_path" not in graph_meta:
            raise RuntimeError(f"Graph {graph_id} has not been loaded with data")

        # we need serialize the graph_meta into a yaml file, and mount it to the pod that we are going to create
        custom_graph_name = f"graph-{graph_id}.yaml"
        custom_graph_file_mount_path = f"/etc/interactive/{custom_graph_name}"
        custom_graph_file_sub_path = custom_graph_name
        custom_graph_statistics_mount_path = f"{self._config.workspace}/data/gs_interactive_default_graph/indices/statistics.json"
        custom_graph_file_data = yaml.dump(graph_meta, default_flow_style=False)
        logger.info("Custom graph file data: %s", custom_graph_file_data)
        
        custom_engine_config_name = "interactive_config.yaml"
        custom_engine_config_mount_path = "/opt/flex/share/interactive_config.yaml"
        custom_engine_config_sub_path = "interactive_config.yaml"
        custom_engine_config_data = self._config.to_dict()
        custom_engine_config_data["compiler"]["meta"]["reader"]["schema"]["uri"] = custom_graph_file_mount_path
        custom_engine_config_data["compiler"]["meta"]["reader"]["statistics"]["uri"] = custom_graph_statistics_mount_path
        custom_engine_config_data = yaml.dump(custom_engine_config_data, default_flow_style=False)

        cluster = self._launcher.launch_cluster(
            graph_id=graph_id,
            config=self._config,
            custom_graph_schema_mount_path=custom_graph_file_mount_path,
            custom_graph_statistics_mount_path=custom_graph_statistics_mount_path,
            wait_service_ready=False,
            additional_config=[
                (
                    custom_graph_name,
                    custom_graph_file_mount_path,
                    custom_graph_file_sub_path,
                    custom_graph_file_data,
                ),
                (
                    custom_engine_config_name,
                    custom_engine_config_mount_path,
                    custom_engine_config_sub_path,
                    custom_engine_config_data,
                )
            ],
        )

        self._clusters[graph_id] = cluster
        return "Service started successfully"

    def stop_service(self, stop_service_request: StartServiceRequest):
        """
        Stop the service for a specified graph.

        Args:
            stop_service_request (StartServiceRequest): _description_

        Returns:
            _type_: _description_
        """
        graph_id = stop_service_request.graph_id
        logger.info("Stopping service for graph %s", graph_id)
        if graph_id is None or graph_id == "":
            raise RuntimeError("graph_id is empty")

        if graph_id not in self._clusters:
            raise RuntimeError(f"The specified graph {graph_id} is not running")

        cluster: InteractiveCluster = self._clusters[graph_id]
        cluster.stop()
        del self._clusters[graph_id]
        return "Service stopped successfully"
    
    def is_graph_running(self, graph_id: str):
        """Returns whether a graph is serving in this deployment.

        Args:
            graph_id (str): _description_

        Returns:
            _type_: _description_
        """


service_manager = None


def get_service_manager():
    global service_manager
    return service_manager


def init_service_manager(config: Config):
    global service_manager
    service_manager = ServiceManager(config)
    return service_manager
