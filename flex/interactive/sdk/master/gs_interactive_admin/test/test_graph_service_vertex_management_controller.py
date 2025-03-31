import unittest

from flask import json

from gs_interactive_admin.models.api_response_with_code import APIResponseWithCode  # noqa: E501
from gs_interactive_admin.models.delete_vertex_request import DeleteVertexRequest  # noqa: E501
from gs_interactive_admin.models.vertex_data import VertexData  # noqa: E501
from gs_interactive_admin.models.vertex_edge_request import VertexEdgeRequest  # noqa: E501
from gs_interactive_admin.test import BaseTestCase


class TestGraphServiceVertexManagementController(BaseTestCase):
    """GraphServiceVertexManagementController integration test stubs"""

    def test_add_vertex(self):
        """Test case for add_vertex

        Add vertex (and edge) to the graph
        """
        vertex_edge_request = {"vertex_request":[{"primary_key_values":[{"name":"id","value":""},{"name":"id","value":""}],"label":"person","properties":[{"name":"id","value":""},{"name":"id","value":""}]},{"primary_key_values":[{"name":"id","value":""},{"name":"id","value":""}],"label":"person","properties":[{"name":"id","value":""},{"name":"id","value":""}]}],"edge_request":[{"src_label":"person","dst_primary_key_values":[{"name":"id","value":""},{"name":"id","value":""}],"dst_label":"software","src_primary_key_values":[{"name":"id","value":""},{"name":"id","value":""}],"edge_label":"created","properties":[{"name":"id","value":""},{"name":"id","value":""}]},{"src_label":"person","dst_primary_key_values":[{"name":"id","value":""},{"name":"id","value":""}],"dst_label":"software","src_primary_key_values":[{"name":"id","value":""},{"name":"id","value":""}],"edge_label":"created","properties":[{"name":"id","value":""},{"name":"id","value":""}]}]}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/vertex'.format(graph_id='graph_id_example'),
            method='POST',
            headers=headers,
            data=json.dumps(vertex_edge_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_vertex(self):
        """Test case for delete_vertex

        Remove vertex from the graph
        """
        delete_vertex_request = {"primary_key_values":[{"name":"id","value":""},{"name":"id","value":""}],"label":"person"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/vertex'.format(graph_id='graph_id_example'),
            method='DELETE',
            headers=headers,
            data=json.dumps(delete_vertex_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_vertex(self):
        """Test case for get_vertex

        Get the vertex's properties with vertex primary key.
        """
        query_string = [('label', 'label_example'),
                        ('primary_key_value', None)]
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/vertex'.format(graph_id='graph_id_example'),
            method='GET',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_vertex(self):
        """Test case for update_vertex

        Update vertex's property
        """
        vertex_edge_request = {"vertex_request":[{"primary_key_values":[{"name":"id","value":""},{"name":"id","value":""}],"label":"person","properties":[{"name":"id","value":""},{"name":"id","value":""}]},{"primary_key_values":[{"name":"id","value":""},{"name":"id","value":""}],"label":"person","properties":[{"name":"id","value":""},{"name":"id","value":""}]}],"edge_request":[{"src_label":"person","dst_primary_key_values":[{"name":"id","value":""},{"name":"id","value":""}],"dst_label":"software","src_primary_key_values":[{"name":"id","value":""},{"name":"id","value":""}],"edge_label":"created","properties":[{"name":"id","value":""},{"name":"id","value":""}]},{"src_label":"person","dst_primary_key_values":[{"name":"id","value":""},{"name":"id","value":""}],"dst_label":"software","src_primary_key_values":[{"name":"id","value":""},{"name":"id","value":""}],"edge_label":"created","properties":[{"name":"id","value":""},{"name":"id","value":""}]}]}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/vertex'.format(graph_id='graph_id_example'),
            method='PUT',
            headers=headers,
            data=json.dumps(vertex_edge_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
