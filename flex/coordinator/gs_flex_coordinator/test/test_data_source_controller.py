import unittest

from flask import json

from gs_flex_coordinator.models.error import Error  # noqa: E501
from gs_flex_coordinator.models.schema_mapping import SchemaMapping  # noqa: E501
from gs_flex_coordinator.test import BaseTestCase


class TestDataSourceController(BaseTestCase):
    """DataSourceController integration test stubs"""

    def test_bind_datasource_in_batch(self):
        """Test case for bind_datasource_in_batch

        
        """
        schema_mapping = {"edge_mappings":[{"inputs":["inputs","inputs"],"source_vertex_mappings":[{"column":{"name":"name","index":0},"property":"property"},{"column":{"name":"name","index":0},"property":"property"}],"destination_vertex_mappings":[{"column":{"name":"name","index":0},"property":"property"},{"column":{"name":"name","index":0},"property":"property"}],"column_mappings":[{"column":{"name":"name","index":0},"property":"property"},{"column":{"name":"name","index":0},"property":"property"}],"type_triplet":{"edge":"edge","source_vertex":"source_vertex","destination_vertex":"destination_vertex"}},{"inputs":["inputs","inputs"],"source_vertex_mappings":[{"column":{"name":"name","index":0},"property":"property"},{"column":{"name":"name","index":0},"property":"property"}],"destination_vertex_mappings":[{"column":{"name":"name","index":0},"property":"property"},{"column":{"name":"name","index":0},"property":"property"}],"column_mappings":[{"column":{"name":"name","index":0},"property":"property"},{"column":{"name":"name","index":0},"property":"property"}],"type_triplet":{"edge":"edge","source_vertex":"source_vertex","destination_vertex":"destination_vertex"}}],"vertex_mappings":[{"type_name":"type_name","inputs":["file:///path/to/file.csv","file:///path/to/file.csv"],"column_mappings":[{"column":{"name":"name","index":0},"property":"property"},{"column":{"name":"name","index":0},"property":"property"}]},{"type_name":"type_name","inputs":["file:///path/to/file.csv","file:///path/to/file.csv"],"column_mappings":[{"column":{"name":"name","index":0},"property":"property"},{"column":{"name":"name","index":0},"property":"property"}]}]}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_id}/datasource'.format(graph_id='graph_id_example'),
            method='POST',
            headers=headers,
            data=json.dumps(schema_mapping),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_datasource_by_id(self):
        """Test case for get_datasource_by_id

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_id}/datasource'.format(graph_id='graph_id_example'),
            method='GET',
            headers=headers)
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
            '/api/v1/graph/{graph_id}/datasource/edge/{type_name}'.format(graph_id='graph_id_example', type_name='type_name_example'),
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
            '/api/v1/graph/{graph_id}/datasource/vertex/{type_name}'.format(graph_id='graph_id_example', type_name='type_name_example'),
            method='DELETE',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
