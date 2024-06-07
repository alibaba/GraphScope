import unittest

from flask import json

from gs_flex_coordinator.models.data_source import DataSource  # noqa: E501
from gs_flex_coordinator.models.edge_data_source import EdgeDataSource  # noqa: E501
from gs_flex_coordinator.models.vertex_data_source import VertexDataSource  # noqa: E501
from gs_flex_coordinator.test import BaseTestCase


class TestDatasourceController(BaseTestCase):
    """DatasourceController integration test stubs"""

    def test_bind_edge_datasource(self):
        """Test case for bind_edge_datasource

        
        """
        edge_data_source = {"source_pk_column_map":{"key":""},"type_name":"type_name","destination_pk_column_map":{"key":""},"source_vertex":"source_vertex","destination_vertex":"destination_vertex","property_mapping":{"key":""},"location":"location","data_source":"ODPS"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_name}/datasource/edge_datasource'.format(graph_name='graph_name_example'),
            method='POST',
            headers=headers,
            data=json.dumps(edge_data_source),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_bind_vertex_datasource(self):
        """Test case for bind_vertex_datasource

        
        """
        vertex_data_source = {"type_name":"type_name","property_mapping":{"key":""},"location":"location","data_source":"ODPS"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_name}/datasource/vertex_datasource'.format(graph_name='graph_name_example'),
            method='POST',
            headers=headers,
            data=json.dumps(vertex_data_source),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_datasource(self):
        """Test case for get_datasource

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_name}/datasource'.format(graph_name='graph_name_example'),
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_edge_datasource(self):
        """Test case for get_edge_datasource

        
        """
        query_string = [('source_vertex_type', 'source_vertex_type_example'),
                        ('destination_vertex_type', 'destination_vertex_type_example')]
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_name}/datasource/edge_datasource/{type_name}'.format(graph_name='graph_name_example', type_name='type_name_example'),
            method='GET',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_vertex_datasource(self):
        """Test case for get_vertex_datasource

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_name}/datasource/vertex_datasource/{type_name}'.format(graph_name='graph_name_example', type_name='type_name_example'),
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_import_datasource(self):
        """Test case for import_datasource

        
        """
        data_source = {"edges_datasource":[{"source_pk_column_map":{"key":""},"type_name":"type_name","destination_pk_column_map":{"key":""},"source_vertex":"source_vertex","destination_vertex":"destination_vertex","property_mapping":{"key":""},"location":"location","data_source":"ODPS"},{"source_pk_column_map":{"key":""},"type_name":"type_name","destination_pk_column_map":{"key":""},"source_vertex":"source_vertex","destination_vertex":"destination_vertex","property_mapping":{"key":""},"location":"location","data_source":"ODPS"}],"vertices_datasource":[{"type_name":"type_name","property_mapping":{"key":""},"location":"location","data_source":"ODPS"},{"type_name":"type_name","property_mapping":{"key":""},"location":"location","data_source":"ODPS"}]}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_name}/datasource'.format(graph_name='graph_name_example'),
            method='POST',
            headers=headers,
            data=json.dumps(data_source),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_unbind_edge_datasource(self):
        """Test case for unbind_edge_datasource

        
        """
        query_string = [('source_vertex_type', 'source_vertex_type_example'),
                        ('destination_vertex_type', 'destination_vertex_type_example')]
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_name}/datasource/edge_datasource/{type_name}'.format(graph_name='graph_name_example', type_name='type_name_example'),
            method='DELETE',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_unbind_vertex_datasource(self):
        """Test case for unbind_vertex_datasource

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_name}/datasource/vertex_datasource/{type_name}'.format(graph_name='graph_name_example', type_name='type_name_example'),
            method='DELETE',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
