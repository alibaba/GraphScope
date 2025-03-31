import unittest

from flask import json

from gs_interactive_admin.models.api_response_with_code import APIResponseWithCode  # noqa: E501
from gs_interactive_admin.models.service_status import ServiceStatus  # noqa: E501
from gs_interactive_admin.models.start_service_request import StartServiceRequest  # noqa: E501
from gs_interactive_admin.models.stop_service_request import StopServiceRequest  # noqa: E501
from gs_interactive_admin.test import BaseTestCase


class TestAdminServiceServiceManagementController(BaseTestCase):
    """AdminServiceServiceManagementController integration test stubs"""

    def test_check_service_ready(self):
        """Test case for check_service_ready

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/v1/service/ready',
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_service_status(self):
        """Test case for get_service_status

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/v1/service/status',
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
            '/v1/service/restart',
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
            '/v1/service/start',
            method='POST',
            headers=headers,
            data=json.dumps(start_service_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_stop_service(self):
        """Test case for stop_service

        
        """
        stop_service_request = {"graph_id":"graph_id"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/v1/service/stop',
            method='POST',
            headers=headers,
            data=json.dumps(stop_service_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
