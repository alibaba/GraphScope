class GLTorchGraph(object):
    def __init__(self, server_list):
        assert len(server_list) == 1
        self._master_addr, self._server_client_master_port = server_list[0].split(":")

    @property
    def master_addr(self):
        return self._master_addr

    @property
    def server_client_master_port(self):
        return self._server_client_master_port
