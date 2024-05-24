import unittest

from flask import json

from gs_flex_coordinator.models.create_stored_proc_request import CreateStoredProcRequest  # noqa: E501
from gs_flex_coordinator.models.create_stored_proc_response import CreateStoredProcResponse  # noqa: E501
from gs_flex_coordinator.models.error import Error  # noqa: E501
from gs_flex_coordinator.models.get_stored_proc_response import GetStoredProcResponse  # noqa: E501
from gs_flex_coordinator.models.update_stored_proc_request import UpdateStoredProcRequest  # noqa: E501
from gs_flex_coordinator.test import BaseTestCase


class TestStoredProcedureController(BaseTestCase):
    """StoredProcedureController integration test stubs"""

    def test_create_stored_procedure(self):
        """Test case for create_stored_procedure

        
        """
        create_stored_proc_request = {"query":"query","name":"name","description":"description","type":"cpp"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_id}/storedproc'.format(graph_id='graph_id_example'),
            method='POST',
            headers=headers,
            data=json.dumps(create_stored_proc_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_stored_procedure_by_id(self):
        """Test case for delete_stored_procedure_by_id

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_id}/storedproc/{stored_procedure_id}'.format(graph_id='graph_id_example', stored_procedure_id='stored_procedure_id_example'),
            method='DELETE',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_stored_procedure_by_id(self):
        """Test case for get_stored_procedure_by_id

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_id}/storedproc/{stored_procedure_id}'.format(graph_id='graph_id_example', stored_procedure_id='stored_procedure_id_example'),
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_list_stored_procedures(self):
        """Test case for list_stored_procedures

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_id}/storedproc'.format(graph_id='graph_id_example'),
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_stored_procedure_by_id(self):
        """Test case for update_stored_procedure_by_id

        
        """
        update_stored_proc_request = {"description":"description"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_id}/storedproc/{stored_procedure_id}'.format(graph_id='graph_id_example', stored_procedure_id='stored_procedure_id_example'),
            method='PUT',
            headers=headers,
            data=json.dumps(update_stored_proc_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
