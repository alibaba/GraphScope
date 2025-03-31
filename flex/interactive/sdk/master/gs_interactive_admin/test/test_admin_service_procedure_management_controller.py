import unittest

from flask import json

from gs_interactive_admin.models.api_response_with_code import APIResponseWithCode  # noqa: E501
from gs_interactive_admin.models.create_procedure_request import CreateProcedureRequest  # noqa: E501
from gs_interactive_admin.models.create_procedure_response import CreateProcedureResponse  # noqa: E501
from gs_interactive_admin.models.get_procedure_response import GetProcedureResponse  # noqa: E501
from gs_interactive_admin.models.update_procedure_request import UpdateProcedureRequest  # noqa: E501
from gs_interactive_admin.test import BaseTestCase


class TestAdminServiceProcedureManagementController(BaseTestCase):
    """AdminServiceProcedureManagementController integration test stubs"""

    def test_create_procedure(self):
        """Test case for create_procedure

        
        """
        create_procedure_request = {"query":"MATCH(a) return COUNT(a);","name":"query1","description":"A sample stored procedure","type":"cpp"}
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

    def test_delete_procedure(self):
        """Test case for delete_procedure

        
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

    def test_get_procedure(self):
        """Test case for get_procedure

        
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

    def test_update_procedure(self):
        """Test case for update_procedure

        
        """
        update_procedure_request = {"description":"A sample stored procedure"}
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
