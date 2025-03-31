import unittest

from flask import json

from gs_interactive_admin.models.api_response_with_code import APIResponseWithCode  # noqa: E501
from gs_interactive_admin.test import BaseTestCase


class TestQueryServiceController(BaseTestCase):
    """QueryServiceController integration test stubs"""

    @unittest.skip("text/plain not supported by Connexion")
    def test_call_proc(self):
        """Test case for call_proc

        run queries on graph
        """
        body = 'body_example'
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'text/plain',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/query'.format(graph_id='graph_id_example'),
            method='POST',
            headers=headers,
            data=json.dumps(body),
            content_type='text/plain')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    @unittest.skip("text/plain not supported by Connexion")
    def test_call_proc_current(self):
        """Test case for call_proc_current

        run queries on the running graph
        """
        body = 'body_example'
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'text/plain',
        }
        response = self.client.open(
            '/v1/graph/current/query',
            method='POST',
            headers=headers,
            data=json.dumps(body),
            content_type='text/plain')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    @unittest.skip("text/plain not supported by Connexion")
    def test_run_adhoc(self):
        """Test case for run_adhoc

        Submit adhoc query to the Interactive Query Service.
        """
        body = 'body_example'
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'text/plain',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/adhoc_query'.format(graph_id='graph_id_example'),
            method='POST',
            headers=headers,
            data=json.dumps(body),
            content_type='text/plain')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    @unittest.skip("text/plain not supported by Connexion")
    def test_run_adhoc_current(self):
        """Test case for run_adhoc_current

        Submit adhoc query to the Interactive Query Service.
        """
        body = 'body_example'
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'text/plain',
        }
        response = self.client.open(
            '/v1/graph/current/adhoc_query',
            method='POST',
            headers=headers,
            data=json.dumps(body),
            content_type='text/plain')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
