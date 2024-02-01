import unittest

from flask import json

from gs_flex_coordinator.models.query_statement import QueryStatement  # noqa: E501
from gs_flex_coordinator.test import BaseTestCase


class TestExtensionController(BaseTestCase):
    """ExtensionController integration test stubs"""

    def test_create_query_statements(self):
        """Test case for create_query_statements

        
        """
        query_statement = {"query":"query","name":"name","description":"description","statement_id":"statement_id"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/statement',
            method='POST',
            headers=headers,
            data=json.dumps(query_statement),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_queryby_name(self):
        """Test case for delete_queryby_name

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/statement/{statement_id}'.format(statement_id='statement_id_example'),
            method='DELETE',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_list_query_statement(self):
        """Test case for list_query_statement

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/statement',
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_query_statement_by_id(self):
        """Test case for update_query_statement_by_id

        
        """
        query_statement = {"query":"query","name":"name","description":"description","statement_id":"statement_id"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/statement/{statement_id}'.format(statement_id='statement_id_example'),
            method='PUT',
            headers=headers,
            data=json.dumps(query_statement),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
