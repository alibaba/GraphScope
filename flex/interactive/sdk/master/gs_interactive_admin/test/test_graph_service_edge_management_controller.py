import unittest

from flask import json

from gs_interactive_admin.models.api_response_with_code import APIResponseWithCode  # noqa: E501
from gs_interactive_admin.models.delete_edge_request import DeleteEdgeRequest  # noqa: E501
from gs_interactive_admin.models.edge_data import EdgeData  # noqa: E501
from gs_interactive_admin.models.edge_request import EdgeRequest  # noqa: E501
from gs_interactive_admin.test import BaseTestCase


class TestGraphServiceEdgeManagementController(BaseTestCase):
    """GraphServiceEdgeManagementController integration test stubs"""

    def test_add_edge(self):
        """Test case for add_edge

        Add edge to the graph
        """
        edge_request = {"src_label":"person","dst_primary_key_values":[{"name":"id","value":""},{"name":"id","value":""}],"dst_label":"software","src_primary_key_values":[{"name":"id","value":""},{"name":"id","value":""}],"edge_label":"created","properties":[{"name":"id","value":""},{"name":"id","value":""}]}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/edge'.format(graph_id='graph_id_example'),
            method='POST',
            headers=headers,
            data=json.dumps(edge_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_edge(self):
        """Test case for delete_edge

        Remove edge from the graph
        """
        delete_edge_request = {"src_label":"person","dst_primary_key_values":[{"name":"id","value":""},{"name":"id","value":""}],"dst_label":"software","src_primary_key_values":[{"name":"id","value":""},{"name":"id","value":""}],"edge_label":"created","properties":[{"name":"id","value":""},{"name":"id","value":""}]}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/edge'.format(graph_id='graph_id_example'),
            method='DELETE',
            headers=headers,
            data=json.dumps(delete_edge_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_edge(self):
        """Test case for get_edge

        Get the edge's properties with src and dst vertex primary keys.
        """
        query_string = [('edge_label', 'created'),
                        ('src_label', 'person'),
                        ('src_primary_key_value', 1),
                        ('dst_label', 'software'),
                        ('dst_primary_key_value', 3)]
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/edge'.format(graph_id='graph_id_example'),
            method='GET',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_edge(self):
        """Test case for update_edge

        Update edge's property
        """
        edge_request = {"src_label":"person","dst_primary_key_values":[{"name":"id","value":""},{"name":"id","value":""}],"dst_label":"software","src_primary_key_values":[{"name":"id","value":""},{"name":"id","value":""}],"edge_label":"created","properties":[{"name":"id","value":""},{"name":"id","value":""}]}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/edge'.format(graph_id='graph_id_example'),
            method='PUT',
            headers=headers,
            data=json.dumps(edge_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
