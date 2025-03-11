


class JobManager(object):
    def __init__(self, metadata_store):
        self.metadata_store = metadata_store
        
    def list_jobs(self):
        pass
    
    def get_job_by_id(self, job_id):
        pass
    
    def delete_job_by_id(self, job_id):
        pass
    
    def create_dataloading_job(self, graph_id, schema_mapping):
        pass
