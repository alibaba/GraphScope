import time
import unittest

from flask import json
import os
import logging
import etcd3

from gs_interactive_admin.models.job_status import JobStatus  # noqa: E501
from gs_interactive_admin.test import BaseTestCase

from gs_interactive_admin.core.service_discovery.service_registry import EtcdKeyHelper

logger = logging.getLogger("interactive")


class TestServiceRegistryServer(BaseTestCase):
    """AdminServiceJobManagementController integration test stubs"""

    def setup_class(self):
        self._helper = EtcdKeyHelper()
        if os.environ.get("ETCD_ENDPOINT") is None:
            raise Exception("ETCD_ENDPOINT is not set")
        self.etcd_endpoint = os.environ["ETCD_ENDPOINT"]
        self.host, self.port = self.etcd_endpoint.split(":")
        self.etcd_client = etcd3.client(host=self.host, port=int(self.port))

    def send_request(self, method, url, data=None):
        headers = {
            "Accept": "application/json",
        }
        logger.info("sending request to %s", url)
        response = self.client.open(
            url, method=method, headers=headers, data=json.dumps(data)
        )
        logger.info("response : %s", response.json)
        return response

    def test_get_service_registry_info(self):
        """
        Test case for get_service_registry_info
        """
        resp = self.send_request(method="get", url="/v1/service/registry")
        assert resp.status_code == 200 and resp.json == {}

        service_key1 = (
            self._helper.service_instance_list_prefix(
                "graph_id_example", "service_name_example"
            )
            + "/example.interactive.com:7687"
        )
        primary_key1 = self._helper.service_primary_key(
            "graph_id_example", "service_name_example"
        )
        service_key2 = (
            self._helper.service_instance_list_prefix(
                "graph_id_example", "service_name_example"
            )
            + "/example.interactive.com:7688"
        )
        primary_key2 = self._helper.service_primary_key(
            "graph_id_example", "service_name_example"
        )
        mock_metrics1 = '{"endpoint": "example.interactive.com:7687", "service_name": "service_name_example", "snapshot_id": "0"}'
        mock_metrics2 = '{"endpoint": "example.interactive.com:7688", "service_name": "service_name_example", "snapshot_id": "0"}'

        self.etcd_client.put(service_key1, mock_metrics1)
        self.etcd_client.put(primary_key1, mock_metrics1)
        time.sleep(1)
        # registry should immediately get the updated data
        resp = self.send_request(method="get", url="/v1/service/registry")
        assert resp.status_code == 200 and resp.json == {
            "graph_id_example": {
                "service_name_example": {
                    "instance_list": [
                        {
                            "endpoint": "example.interactive.com:7687",
                            "metrics": '{"endpoint": "example.interactive.com:7687", "service_name": "service_name_example", "snapshot_id": "0"}',
                        },
                    ],
                    "primary": {
                        "endpoint": "example.interactive.com:7687",
                         "metrics": '{"endpoint": "example.interactive.com:7687", "service_name": "service_name_example", "snapshot_id": "0"}',
                    },
                }
            }
        }
        
        # insert another service instance
        self.etcd_client.put(service_key2, mock_metrics2)
        time.sleep(1)
        # registry should immediately get the updated data
        resp = self.send_request(method="get", url="/v1/service/registry")
        assert resp.status_code == 200 and resp.json == {
            "graph_id_example": {
                "service_name_example": {
                    "instance_list": [
                        {
                            "endpoint": "example.interactive.com:7687",
                            "metrics": '{"endpoint": "example.interactive.com:7687", "service_name": "service_name_example", "snapshot_id": "0"}',
                        },
                        {
                            "endpoint": "example.interactive.com:7688",
                            "metrics": '{"endpoint": "example.interactive.com:7688", "service_name": "service_name_example", "snapshot_id": "0"}',
                        },
                    ],
                    "primary": {
                        "endpoint": "example.interactive.com:7687",
                        "metrics": '{"endpoint": "example.interactive.com:7687", "service_name": "service_name_example", "snapshot_id": "0"}',
                    },
                }
            }
        }

    def teardown_class(self):
        self.etcd_client.delete_prefix(self._helper.service_prefix())
        self.etcd_client.close()


if __name__ == "__main__":
    unittest.main()
