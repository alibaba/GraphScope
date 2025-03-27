# Those methods should not be implemented in AdminService, but is still kept here, cause the python flask app relies on the openpai_interactive.yaml to launch the service, which needs these function definitions.
# To create_edge/delete_edge/get_edge/add_edge/update, send requests to query service.
def create_edge():
    raise NotImplementedError("create_edge is not implemented in admin service, please send to query service")


def create_edge_type():
    raise NotImplementedError("create_edge_type is not implemented in admin service, please send to query service")


def delete_edge():
    raise NotImplementedError("delete_edge is not implemented in admin service, please send to query service")


def get_edge():
    raise NotImplementedError("get_edge is not implemented in admin service, please send to query service")


def add_edge():
    raise NotImplementedError("add_edge is not implemented in admin service, please send to query service")


def update_edge():
    raise NotImplementedError("update_edge is not implemented in admin service, please send to query service")
