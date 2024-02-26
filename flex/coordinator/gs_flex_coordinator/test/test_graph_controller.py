import unittest

from flask import json

from gs_flex_coordinator.models.edge_type import EdgeType  # noqa: E501
from gs_flex_coordinator.models.graph import Graph  # noqa: E501
from gs_flex_coordinator.models.model_schema import ModelSchema  # noqa: E501
from gs_flex_coordinator.models.vertex_type import VertexType  # noqa: E501
from gs_flex_coordinator.test import BaseTestCase


class TestGraphController(BaseTestCase):
    """GraphController integration test stubs"""

    def test_create_edge_type(self):
        """Test case for create_edge_type

        
        """
        edge_type = {"type_name":"type_name","type_id":1,"vertex_type_pair_relations":[{"source_vertex":"source_vertex","destination_vertex":"destination_vertex","x_csr_params":{"edge_storage_strategy":"ONLY_IN"},"relation":"MANY_TO_MANY"},{"source_vertex":"source_vertex","destination_vertex":"destination_vertex","x_csr_params":{"edge_storage_strategy":"ONLY_IN"},"relation":"MANY_TO_MANY"}],"properties":[{"property_type":{"primitive_type":"DT_DOUBLE"},"property_id":6,"property_name":"property_name"},{"property_type":{"primitive_type":"DT_DOUBLE"},"property_id":6,"property_name":"property_name"}]}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_name}/schema/edge_type'.format(graph_name='graph_name_example'),
            method='POST',
            headers=headers,
            data=json.dumps(edge_type),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_create_graph(self):
        """Test case for create_graph

        
        """
        graph = {"schema":{"vertex_types":[{"type_name":"type_name","primary_keys":["primary_keys","primary_keys"],"type_id":0,"properties":[{"property_type":{"primitive_type":"DT_DOUBLE"},"property_id":6,"property_name":"property_name"},{"property_type":{"primitive_type":"DT_DOUBLE"},"property_id":6,"property_name":"property_name"}]},{"type_name":"type_name","primary_keys":["primary_keys","primary_keys"],"type_id":0,"properties":[{"property_type":{"primitive_type":"DT_DOUBLE"},"property_id":6,"property_name":"property_name"},{"property_type":{"primitive_type":"DT_DOUBLE"},"property_id":6,"property_name":"property_name"}]}],"edge_types":[{"type_name":"type_name","type_id":1,"vertex_type_pair_relations":[{"source_vertex":"source_vertex","destination_vertex":"destination_vertex","x_csr_params":{"edge_storage_strategy":"ONLY_IN"},"relation":"MANY_TO_MANY"},{"source_vertex":"source_vertex","destination_vertex":"destination_vertex","x_csr_params":{"edge_storage_strategy":"ONLY_IN"},"relation":"MANY_TO_MANY"}],"properties":[{"property_type":{"primitive_type":"DT_DOUBLE"},"property_id":6,"property_name":"property_name"},{"property_type":{"primitive_type":"DT_DOUBLE"},"property_id":6,"property_name":"property_name"}]},{"type_name":"type_name","type_id":1,"vertex_type_pair_relations":[{"source_vertex":"source_vertex","destination_vertex":"destination_vertex","x_csr_params":{"edge_storage_strategy":"ONLY_IN"},"relation":"MANY_TO_MANY"},{"source_vertex":"source_vertex","destination_vertex":"destination_vertex","x_csr_params":{"edge_storage_strategy":"ONLY_IN"},"relation":"MANY_TO_MANY"}],"properties":[{"property_type":{"primitive_type":"DT_DOUBLE"},"property_id":6,"property_name":"property_name"},{"property_type":{"primitive_type":"DT_DOUBLE"},"property_id":6,"property_name":"property_name"}]}]},"stored_procedures":{"directory":"plugins"},"name":"name","store_type":"mutable_csr"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph',
            method='POST',
            headers=headers,
            data=json.dumps(graph),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_create_vertex_type(self):
        """Test case for create_vertex_type

        
        """
        vertex_type = {"type_name":"type_name","primary_keys":["primary_keys","primary_keys"],"type_id":0,"properties":[{"property_type":{"primitive_type":"DT_DOUBLE"},"property_id":6,"property_name":"property_name"},{"property_type":{"primitive_type":"DT_DOUBLE"},"property_id":6,"property_name":"property_name"}]}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_name}/schema/vertex_type'.format(graph_name='graph_name_example'),
            method='POST',
            headers=headers,
            data=json.dumps(vertex_type),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_edge_type(self):
        """Test case for delete_edge_type

        
        """
        query_string = [('source_vertex_type', 'source_vertex_type_example'),
                        ('destination_vertex_type', 'destination_vertex_type_example')]
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_name}/schema/edge_edge/{type_name}'.format(graph_name='graph_name_example', type_name='type_name_example'),
            method='DELETE',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_graph(self):
        """Test case for delete_graph

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_name}'.format(graph_name='graph_name_example'),
            method='DELETE',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_vertex_type(self):
        """Test case for delete_vertex_type

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_name}/schema/vertex_type/{type_name}'.format(graph_name='graph_name_example', type_name='type_name_example'),
            method='DELETE',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_schema(self):
        """Test case for get_schema

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_name}/schema'.format(graph_name='graph_name_example'),
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_list_graphs(self):
        """Test case for list_graphs

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph',
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
