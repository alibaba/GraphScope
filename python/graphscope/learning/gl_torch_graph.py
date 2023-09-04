class GLTorchGraph(object):
    def __init__(self, handle, server_list):
        assert len(server_list) == 4
        self._master_addr, self._server_client_master_port = server_list[0].split(":")
        self._train_master_addr, self._train_loader_master_port = server_list[1].split(":")
        self._val_master_addr, self._val_loader_master_port = server_list[2].split(":")
        self._test_master_addr, self._test_loader_master_port = server_list[3].split(":")
        assert self._master_addr == self._train_master_addr == self._val_master_addr == self._test_master_addr

    @property
    def master_addr(self):
        return self._master_addr

    @property
    def server_client_master_port(self):
        return self._server_client_master_port
    
    @property
    def train_loader_master_port(self):
        return self._train_loader_master_port
    
    @property
    def val_loader_master_port(self):
        return self._val_loader_master_port
    
    @property
    def test_loader_master_port(self):
        return self._test_loader_master_port