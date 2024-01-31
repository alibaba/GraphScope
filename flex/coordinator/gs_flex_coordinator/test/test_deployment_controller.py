import unittest

from flask import json

from gs_flex_coordinator.models.deployment_info import DeploymentInfo  # noqa: E501
from gs_flex_coordinator.models.node_status import NodeStatus  # noqa: E501
from gs_flex_coordinator.test import BaseTestCase


class TestDeploymentController(BaseTestCase):
    """DeploymentController integration test stubs"""

    def test_get_deployment_info(self):
        """Test case for get_deployment_info

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/deployment/info',
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_node_status(self):
        """Test case for get_node_status

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/node/status',
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
