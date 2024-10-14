import base64
import json
import unittest
from unittest.mock import patch

import numpy as np
import torch

from graphscope.learning.gs_feature_store import GsFeatureStore
from graphscope.learning.gs_feature_store import GsTensorAttr


class TestGsFeatureStore(unittest.TestCase):

    def setUp(self):
        self.endpoints = ["localhost:1234"]
        self.handle = base64.b64encode(
            json.dumps({"num_servers": 1}).encode("utf-8")
        ).decode("utf-8")
        self.config = base64.b64encode(
            json.dumps(
                {
                    "edge_features": {
                        "edge_type": ["edge_feature_0", "edge_feature_1"]
                    },
                    "node_features": {
                        "node_type": ["node_feature_0", "node_feature_1"]
                    },
                    "node_labels": {"node_type": "node_label"},
                }
            ).encode("utf-8")
        ).decode("utf-8")
        self.store = GsFeatureStore(self.endpoints, self.handle, self.config)

    def test_initialization(self):
        self.assertEqual(self.store.num_servers, 1)
        self.assertIn(("node_type", "node_feature_0"), self.store.tensor_attrs)
        self.assertIn(("edge_type", "edge_feature_1"), self.store.tensor_attrs)
        self.assertIn(("node_type", "node_label"), self.store.tensor_attrs)

    @patch("graphscope.learning.gs_feature_store._request_server")
    def test_get_tensor(self, mock_request_server):
        mock_request_server.return_value = torch.tensor([[1, 2]])
        attr = GsTensorAttr(group_name="node_type", attr_name="node_feature_1", index=0)
        tensor = self.store.get_tensor(attr)
        self.assertTrue(torch.equal(tensor, torch.tensor([2])))

    @patch("graphscope.learning.gs_feature_store._request_server")
    def test_get_partition_id(self, mock_request_server):
        mock_request_server.return_value = 0
        attr = GsTensorAttr(group_name="node_type", attr_name="node_feature", index=0)
        partition_id = self.store._get_partition_id(attr)
        self.assertEqual(partition_id, 0)

    def test_check_attr(self):
        attr = GsTensorAttr(group_name="node_type", attr_name="node_feature_0", index=0)
        self.assertTrue(self.store._check_attr(attr))
        attr = GsTensorAttr(
            group_name="node_type", attr_name="non_existent_feature", index=0
        )
        self.assertFalse(self.store._check_attr(attr))

    def test_index_to_tensor(self):
        index = np.array([1, 2, 3])
        tensor = self.store.index_to_tensor(index)
        self.assertTrue(torch.equal(tensor, torch.tensor([1, 2, 3])))

        index = slice(0, 3, 1)
        tensor = self.store.index_to_tensor(index)
        self.assertTrue(torch.equal(tensor, torch.tensor([0, 1, 2])))

        index = 5
        tensor = self.store.index_to_tensor(index)
        self.assertTrue(torch.equal(tensor, torch.tensor([5])))

    def test_get_all_tensor_attrs(self):
        tensor_attrs = self.store.get_all_tensor_attrs()
        self.assertEqual(len(tensor_attrs), 5)
        self.assertIn(("node_type", "node_feature_0"), self.store.tensor_attrs)
        self.assertIn(("node_type", "node_feature_1"), self.store.tensor_attrs)
        self.assertIn(("edge_type", "edge_feature_0"), self.store.tensor_attrs)
        self.assertIn(("edge_type", "edge_feature_1"), self.store.tensor_attrs)
        self.assertIn(("node_type", "node_label"), self.store.tensor_attrs)


if __name__ == "__main__":
    unittest.main()
