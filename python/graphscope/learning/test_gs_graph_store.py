import base64
import json
import unittest
from unittest.mock import patch

import torch

from graphscope.learning.gs_graph_store import EdgeAttr
from graphscope.learning.gs_graph_store import GsGraphStore


class TestGsGraphStore(unittest.TestCase):

    def setUp(self):
        self.endpoints = [
            "localhost:1234",
            "localhost:1235",
            "localhost:1236",
            "localhost:1237",
        ]
        self.handle = base64.b64encode(
            json.dumps({"num_servers": 1}).encode("utf-8")
        ).decode("utf-8")
        self.config = base64.b64encode(
            json.dumps(
                {
                    "edges": [("node1", "edge_type", "node2")],
                    "edge_weights": [1.0],
                    "edge_dir": "out",
                    "random_node_split": False,
                }
            ).encode("utf-8")
        ).decode("utf-8")
        self.store = GsGraphStore(self.endpoints, self.handle, self.config)

    def test_initialization(self):
        self.assertEqual(self.store.handle, self.handle)
        self.assertEqual(self.store.config, self.config)
        self.assertEqual(self.store.endpoints, self.endpoints)
        self.assertEqual(self.store.master_addr, "localhost")
        self.assertEqual(self.store.server_client_master_port, "1234")
        self.assertEqual(self.store.train_master_addr, "localhost")
        self.assertEqual(self.store.train_loader_master_port, "1235")
        self.assertEqual(self.store.val_master_addr, "localhost")
        self.assertEqual(self.store.val_loader_master_port, "1236")
        self.assertEqual(self.store.test_master_addr, "localhost")
        self.assertEqual(self.store.test_loader_master_port, "1237")
        self.assertIn(
            (("node1", "edge_type", "node2"), "csr", False), self.store.edge_attrs
        )

    @patch("graphscope.learning.gs_graph_store._request_server")
    def test_get_edge_index(self, mock_request_server):
        mock_request_server.return_value = (
            (torch.tensor([0, 1]), torch.tensor([1, 2])),
            (2, 2),
        )
        edge_attr = EdgeAttr(
            edge_type=("node1", "edge_type", "node2"), layout="csr", is_sorted=False
        )
        edge_index = self.store.get_edge_index(edge_attr)
        self.assertIsNotNone(edge_index)
        self.assertTrue(torch.equal(edge_index[0], torch.tensor([0, 1])))
        self.assertTrue(torch.equal(edge_index[1], torch.tensor([1, 2])))

    @patch("graphscope.learning.gs_graph_store._request_server")
    def test_get_all_edge_attrs(self, mock_request_server):
        mock_request_server.return_value = None, (2, 2)
        edge_attrs = self.store.get_all_edge_attrs()
        self.assertEqual(len(edge_attrs), 1)
        self.assertEqual(edge_attrs[0].edge_type, ("node1", "edge_type", "node2"))
        self.assertEqual(edge_attrs[0].layout.value, "csr")
        self.assertFalse(edge_attrs[0].is_sorted)


if __name__ == "__main__":
    unittest.main()
