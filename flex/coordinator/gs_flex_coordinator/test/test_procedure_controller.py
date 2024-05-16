import unittest

from flask import json

from gs_flex_coordinator.models.create_procedure_request import CreateProcedureRequest  # noqa: E501
from gs_flex_coordinator.models.create_procedure_response import CreateProcedureResponse  # noqa: E501
from gs_flex_coordinator.models.error import Error  # noqa: E501
from gs_flex_coordinator.models.get_procedure_response import GetProcedureResponse  # noqa: E501
from gs_flex_coordinator.models.update_procedure_request import UpdateProcedureRequest  # noqa: E501
from gs_flex_coordinator.test import BaseTestCase


class TestProcedureController(BaseTestCase):
    """ProcedureController integration test stubs"""

    def test_create_procedure(self):
        """Test case for create_procedure

        
        """
        create_procedure_request = {"query":"query","name":"name","description":"description","type":"cpp"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/procedure'.format(graph_id='graph_id_example'),
            method='POST',
            headers=headers,
            data=json.dumps(create_procedure_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_procedure_by_id(self):
        """Test case for delete_procedure_by_id

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/procedure/{procedure_id}'.format(graph_id='graph_id_example', procedure_id='procedure_id_example'),
            method='DELETE',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_procedure_by_id(self):
        """Test case for get_procedure_by_id

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/procedure/{procedure_id}'.format(graph_id='graph_id_example', procedure_id='procedure_id_example'),
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_list_procedures(self):
        """Test case for list_procedures

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/procedure'.format(graph_id='graph_id_example'),
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_procedure_by_id(self):
        """Test case for update_procedure_by_id

        
        """
        update_procedure_request = {"description":"description"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/procedure/{procedure_id}'.format(graph_id='graph_id_example', procedure_id='procedure_id_example'),
            method='PUT',
            headers=headers,
            data=json.dumps(update_procedure_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
