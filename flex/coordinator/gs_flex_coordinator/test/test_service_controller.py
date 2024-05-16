import unittest

from flask import json

from gs_flex_coordinator.models.error import Error  # noqa: E501
from gs_flex_coordinator.models.service_status import ServiceStatus  # noqa: E501
from gs_flex_coordinator.models.start_service_request import StartServiceRequest  # noqa: E501
from gs_flex_coordinator.test import BaseTestCase


class TestServiceController(BaseTestCase):
    """ServiceController integration test stubs"""

    def test_get_service_status(self):
        """Test case for get_service_status

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/service/status',
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_restart_service(self):
        """Test case for restart_service

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/service/restart',
            method='POST',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_start_service(self):
        """Test case for start_service

        
        """
        start_service_request = {"graph_id":"graph_id"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/service/start',
            method='POST',
            headers=headers,
            data=json.dumps(start_service_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_stop_service(self):
        """Test case for stop_service

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/service/stop',
            method='POST',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
