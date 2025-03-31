import unittest

from flask import json

from gs_interactive_admin.models.graph_service_registry_record import GraphServiceRegistryRecord  # noqa: E501
from gs_interactive_admin.test import BaseTestCase


class TestAdminServiceServiceRegistryController(BaseTestCase):
    """AdminServiceServiceRegistryController integration test stubs"""

    def test_get_service_registry_info(self):
        """Test case for get_service_registry_info

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/v1/service/registry/{graph_id}/{service_name}'.format(graph_id='graph_id_example', service_name='service_name_example'),
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_list_service_registry_info(self):
        """Test case for list_service_registry_info

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/v1/service/registry',
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
