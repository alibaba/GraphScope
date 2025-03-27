# Those methods should not be implemented in AdminService, but is still kept here, cause the python flask app relies on the openpai_interactive.yaml to launch the service, which needs these function definitions.
# To create_vertex/delete_vertex/get_vertex/add_vertex/update, send requests to query service.
def get_vertex():
    raise NotImplementedError("get_vertex is not implemented in admin service, please send to query service")


def create_vertex():
    raise NotImplementedError("create_vertex is not implemented in admin service, please send to query service")


def delete_vertex():
    raise NotImplementedError("delete_vertex is not implemented in admin service, please send to query service")


def update_vertex():
    raise NotImplementedError("update_vertex is not implemented in admin service, please send to query service")


def create_vertex_type():
    raise NotImplementedError("create_vertex_type is not implemented in admin service, please send to query service")


def delete_vertex_type():
    raise NotImplementedError("delete_vertex_type is not implemented in admin service, please send to query service")


def add_vertex():
    raise NotImplementedError("add_vertex is not implemented in admin service, please send to query service")
