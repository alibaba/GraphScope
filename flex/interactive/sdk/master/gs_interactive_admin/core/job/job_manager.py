from gs_interactive_admin.core.metadata.metadata_store import IMetadataStore
from gs_interactive_admin.core.config import Config, OSS_BUCKET_NAME, OSS_BUCKET_DATA_DIR
from gs_interactive_admin.util import remove_nones, SubProcessRunner
from abc import ABCMeta
from abc import abstractmethod
import os
import shutil

import logging
import subprocess
import time
import yaml

logger = logging.getLogger("interactive")


class JobProcessCallback(object):
    """
    This class is used as a callback function for the data loading subprocess.
    """
    def __init__(self,meta_store, graph_id, process_id, log_path, job_id, oss_graph_path):
        self.metadata_store = meta_store
        self.graph_id = graph_id
        self.process_id = process_id
        self.log_path = log_path
        self.job_id = job_id
        self.oss_graph_path = oss_graph_path
        
    
        
    def _update_remote_storage_path(self):
        """
        This method should be called when the job is successfully finished.
        We need to update the remote storage path of the graph, such that the graph can be accessed by the users.
        """
        logger.info("Update remote storage path of the graph")
        def _update_remote_storage_path_of_graph(graph_meta : str):
            old_meta = yaml.safe_load(graph_meta)
            old_name = None
            old_path = None
            logger.info("old meta: %s", old_meta)
            if "remote_path" in old_meta:
                old_path = old_meta["remote_path"]
            logger.info(f"old path: {old_path}")

            if old_path and old_path.startswith("oss://"):
                # Get the object name 
                split_paths = old_path[5:].split("/")
                old_name = split_paths[-1]
            if old_name:
                # new name should be larger than the old name in timestamp
                new_name = self.oss_graph_path.split("/")[-1]
                if new_name <= old_name:
                    logger.warning(f"New path {self.oss_graph_path} is not larger than the old path {old_path}")
                    return graph_meta
            else:
                old_meta["remote_path"] = self.oss_graph_path
            res = yaml.dump(old_meta)
            logger.info("new meta: %s", res)
            return res

        self.metadata_store.update_graph_meta_with_func(self.graph_id, _update_remote_storage_path_of_graph)        
        
    def __call__(self, process: subprocess.CompletedProcess):
        logger.info(f"Job process {self.process_id} finished with code {process.returncode}")
        if process.returncode == 0:
            status = "SUCCESS"
        else:
            status = "FAILED"
        job_meta = {
            "graph_id": self.graph_id,
            "process_id": self.process_id,
            "log": "@" + self.log_path,
            "status": status,
            "end_time": int(time.time() * 1000),
            "type": "BULK_LOADING",
        }
        logger.info(f"Update Job meta: {job_meta}")
        self.res_code = self.metadata_store.update_job_meta(job_id = self.job_id, job_meta = job_meta)
        logger.info(f"Job meta Update with id {self.res_code}")
        
        # We should also update graph meta to update the remote storage path of the graph.
        if status == "SUCCESS":
            self._update_remote_storage_path()
        

class JobManager(metaclass=ABCMeta):
    def __init__(self, config: Config, metadata_store: IMetadataStore):
        self.metadata_store = metadata_store

    @abstractmethod
    def list_jobs(self):
        pass

    @abstractmethod
    def get_job_by_id(self, job_id):
        pass

    @abstractmethod
    def delete_job_by_id(self, job_id):
        pass

    @abstractmethod
    def create_dataloading_job(self, graph_id, schema_mapping):
        pass


class DefaultJobManager(JobManager):
    def __init__(self, config: Config, metadata_store: IMetadataStore):
        super().__init__(config, metadata_store)
        self._data_loading_processes = {}
        self._process_call_backs = {}

    def list_jobs(self):
        return self.metadata_store.get_all_job_meta()

    def get_job_by_id(self, job_id) -> dict:
        job_meta_str =  self.metadata_store.get_job_meta(job_id)
        # convert the string to dict
        data = yaml.load(job_meta_str, Loader=yaml.FullLoader)
        logger.info(f"Get job by id: {job_id}, data: {data}")
        if "log" in data:
            if data["log"].startswith("@"):
                log_path = data["log"][1:]
                with open(log_path, "r") as f:
                    data["log"] = f.read()
        return data
        

    def delete_job_by_id(self, job_id):
        if job_id in self._data_loading_processes:
            logger.info(f"Terminating job {job_id}")
            self._data_loading_processes[job_id].terminate()
        return f"Successfully deleted job {job_id}."

    def create_dataloading_job(self, graph_id, schema_mapping):
        """
        Create a dataloading job which running in a child process.
        """
        bulk_loader = self._get_bulk_loader()
        # dump the schema_mapping to a temp file
        schema_mapping = remove_nones(schema_mapping)
        logger.info("schema mapping: %s", schema_mapping)
        os.makedirs(os.path.join("/tmp", graph_id), exist_ok=True)
        temp_mapping_file = os.path.join("/tmp", graph_id, "schema_mapping.yaml")
        with open(temp_mapping_file, "w") as f:
            # write the dict in yaml format
            yaml.dump(schema_mapping, f)
            
        # Get the metadata of the graph, and dump the graph to a temp file
        graph_metadata = self.metadata_store.get_graph_meta(graph_id)
        logger.info("graph metadata: %s", graph_metadata)
        temp_graph_file = os.path.join("/tmp", graph_id, "graph.yaml")
        with open(temp_graph_file, "w") as f:
            yaml.dump(graph_metadata, f, default_flow_style=False)
        # Create a log file for the process
        log_path = os.path.join("/tmp", graph_id, "bulk_loader.log")
        if "loading_config" in schema_mapping and "destination" in schema_mapping["loading_config"]:
            oss_graph_path = schema_mapping["loading_config"]["destination"]
        else:
            cur_time_stamp = int(time.time() * 1000)
            oss_graph_path = f"oss://{OSS_BUCKET_NAME}/{OSS_BUCKET_DATA_DIR}/{graph_id}/{cur_time_stamp}"
        logger.info(f"oss_graph_path: {oss_graph_path}")
        
        cmds = [
            bulk_loader,
            "-l",
            temp_mapping_file,
            "-g",
            temp_graph_file,
            "-d",
            oss_graph_path, # The path where the graph data is stored
        ]
        logger.info(f"Running bulk loader with command {cmds}")
        job_meta = self._new_job_meta(
            graph_id=graph_id,
            process_id=0,
            log_path=log_path,
            type="BULK_LOADING",
            status="RUNNING",
        )
        job_id = self.metadata_store.create_job_meta(str(job_meta))
        logger.info(f"Data loading job created with {job_meta}")
        runner = SubProcessRunner(cmds, JobProcessCallback(self.metadata_store, graph_id, 0, log_path, job_id, oss_graph_path), log_path)
        self._data_loading_processes[job_id] = runner
        
        runner.start()
        logger.info(f"Job id {job_id} created for data loading job")
        return job_id

    def _get_bulk_loader(self):
        """
        Try to find the bulk loader in the current environment.
        """
        # First try find from PATH
        if shutil.which("bulk_loader"):
            return "bulk_loader"
        # Then try to find from /opt/flex/bin and /opt/graphscope/bin, check it is excutable
        if os.path.exists("/opt/flex/bin/bulk_loader") and os.access(
            "/opt/flex/bin/bulk_loader", os.X_OK
        ):
            return "/opt/flex/bin/bulk_loader"
        if os.path.exists("/opt/graphscope/bin/bulk_loader") and os.access(
            "/opt/graphscope/bin/bulk_loader", os.X_OK
        ):
            return "/opt/graphscope/bin/bulk_loader"

        # Then try to find via the relative path, works for local development
        relative_path = os.path.join(
            os.path.dirname(__file__), "../../../../../../build/bin/bulk_loader"
        )
        if os.path.exists(relative_path) and os.access(relative_path, os.X_OK):
            return relative_path
        raise RuntimeError("Cannot find bulk_loader in the current environment.")

    def _new_job_meta(self, graph_id, process_id, log_path, type, status):
        return {
            "graph_id": graph_id,
            "process_id": process_id,
            "log": "@" + log_path,
            "status": status,
            # in milliseconds timestamp
            "start_time": int(time.time() * 1000),
            "end_time": 0,
            "type": type,
        }


job_manager = None


def get_job_manager():
    global job_manager
    return job_manager


def init_job_manager(config: Config, metadata_store: IMetadataStore):
    global job_manager
    job_manager = DefaultJobManager(config, metadata_store)
