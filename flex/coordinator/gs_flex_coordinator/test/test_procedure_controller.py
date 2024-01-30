import unittest

from flask import json

from gs_flex_coordinator.models.procedure import Procedure  # noqa: E501
from gs_flex_coordinator.test import BaseTestCase


class TestProcedureController(BaseTestCase):
    """ProcedureController integration test stubs"""

    def test_create_procedure(self):
        """Test case for create_procedure

        
        """
        procedure = {"bound_graph":"bound_graph","enable":True,"query":"query","name":"name","description":"description","returns":[{"name":"name","type":"type"},{"name":"name","type":"type"}],"type":"cpp","params":[{"name":"name","type":"type"},{"name":"name","type":"type"}]}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_name}/procedure'.format(graph_name='graph_name_example'),
            method='POST',
            headers=headers,
            data=json.dumps(procedure),
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
            '/api/v1/graph/{graph_name}/procedure/{procedure_name}'.format(graph_name='graph_name_example', procedure_name='procedure_name_example'),
            method='DELETE',
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
            '/api/v1/procedure',
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_list_procedures_in_graph(self):
        """Test case for list_procedures_in_graph

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_name}/procedure'.format(graph_name='graph_name_example'),
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_procedure(self):
        """Test case for update_procedure

        
        """
        procedure = {"bound_graph":"bound_graph","enable":True,"query":"query","name":"name","description":"description","returns":[{"name":"name","type":"type"},{"name":"name","type":"type"}],"type":"cpp","params":[{"name":"name","type":"type"},{"name":"name","type":"type"}]}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_name}/procedure/{procedure_name}'.format(graph_name='graph_name_example', procedure_name='procedure_name_example'),
            method='PUT',
            headers=headers,
            data=json.dumps(procedure),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
