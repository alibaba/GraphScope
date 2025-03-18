class ServiceManager(object):
    def __init__(self):
        pass
    
    def check_service_ready(self):
        return "Service is ready"


service_manager = ServiceManager()


def get_service_manager():
    return service_manager
