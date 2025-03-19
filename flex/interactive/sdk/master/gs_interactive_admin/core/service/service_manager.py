import time
from gs_interactive_admin.core.config import Config

class ServiceManager(object):
    def __init__(self, config: Config):
        self._query_port = config.http_service.query_port
        self._admin_port = config.master.port
        # get start time in unix timestamp
        self._start_time = int(time.time() * 1000)
        self._bolt_port = config.compiler.endpoint.bolt_connector.port
    
    def check_service_ready(self):
        return "Service is ready"
    
    def get_service_status(self):
        return {
            "status": "running",
            "hqps_port": self._query_port,
            "bolt_port": self._bolt_port,
            "statistics_enabled" : False,
            "graph": {},
            "start_time":  self._start_time,
        }


service_manager = None


def get_service_manager():
    global service_manager
    return service_manager


def init_service_manager(config: Config):
    global service_manager
    service_manager = ServiceManager(config)
    return service_manager