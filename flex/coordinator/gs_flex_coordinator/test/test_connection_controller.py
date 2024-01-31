import unittest

from flask import json

from gs_flex_coordinator.models.connection import Connection  # noqa: E501
from gs_flex_coordinator.models.connection_status import ConnectionStatus  # noqa: E501
from gs_flex_coordinator.test import BaseTestCase


class TestConnectionController(BaseTestCase):
    """ConnectionController integration test stubs"""

    def test_close(self):
        """Test case for close

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/connection',
            method='DELETE',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_connect(self):
        """Test case for connect

        
        """
        connection = {"coordinator_endpoint":"coordinator_endpoint"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/connection',
            method='POST',
            headers=headers,
            data=json.dumps(connection),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
