from gs_interactive_admin.core.metadata.metadata_store import IMetadataStore
from gs_interactive_admin.core.config import Config
class JobManager(object):
    def __init__(self, config: Config, metadata_store: IMetadataStore):
        self.metadata_store = metadata_store

    def list_jobs(self):
        pass

    def get_job_by_id(self, job_id):
        pass

    def delete_job_by_id(self, job_id):
        pass

    def create_dataloading_job(self, graph_id, schema_mapping):
        pass

job_manager = None

def get_job_manager():
    global job_manager
    return job_manager

def init_job_manager(config: Config, metadata_store: IMetadataStore):
    global job_manager
    job_manager = JobManager(config, metadata_store)