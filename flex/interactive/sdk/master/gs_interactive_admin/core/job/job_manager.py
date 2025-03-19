from gs_interactive_admin.core.metadata.metadata_store import IMetadataStore
from gs_interactive_admin.core.config import Config
from abc import ABCMeta
from abc import abstractmethod
import os
import shutil

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

    def list_jobs(self):
        pass

    def get_job_by_id(self, job_id):
        pass

    def delete_job_by_id(self, job_id):
        pass

    def create_dataloading_job(self, graph_id, schema_mapping):
        """
        Create a dataloading job which running in a child process.
        """
        bulk_loader = self._get_bulk_loader()
        # dump the schema_mapping to a temp file
        temp_mapping_file = os.path.join("/tmp", "graph_id", "schema_mapping.yaml")
        
        
    def _get_bulk_loader(self):
        """
        Try to find the bulk loader in the current environment.
        """
        # First try find from PATH
        if shutil.which("bulk_loader"):
            return "bulk_loader"
        # Then try to find from /opt/flex/bin and /opt/graphscope/bin, check it is excutable
        if os.path.exists("/opt/flex/bin/bulk_loader") and os.access("/opt/flex/bin/bulk_loader", os.X_OK):
            return "/opt/flex/bin/bulk_loader"
        if os.path.exists("/opt/graphscope/bin/bulk_loader") and os.access("/opt/graphscope/bin/bulk_loader", os.X_OK):
            return "/opt/graphscope/bin/bulk_loader"
        
        # Then try to find via the relative path, works for local development
        relative_path = os.path.join(os.path.dirname(__file__), "../../../../../../build/bin/bulk_loader")
        if os.path.exists(relative_path) and os.access(relative_path, os.X_OK):
            return relative_path
        raise RuntimeError("Cannot find bulk_loader in the current environment.")
    
job_manager = None

def get_job_manager():
    global job_manager
    return job_manager

def init_job_manager(config: Config, metadata_store: IMetadataStore):
    global job_manager
    job_manager = DefaultJobManager(config, metadata_store)