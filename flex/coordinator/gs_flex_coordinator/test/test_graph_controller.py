import unittest

from flask import json

from gs_flex_coordinator.models.create_edge_type import CreateEdgeType  # noqa: E501
from gs_flex_coordinator.models.create_graph_request import CreateGraphRequest  # noqa: E501
from gs_flex_coordinator.models.create_graph_response import CreateGraphResponse  # noqa: E501
from gs_flex_coordinator.models.create_graph_schema_request import CreateGraphSchemaRequest  # noqa: E501
from gs_flex_coordinator.models.create_vertex_type import CreateVertexType  # noqa: E501
from gs_flex_coordinator.models.error import Error  # noqa: E501
from gs_flex_coordinator.models.get_graph_response import GetGraphResponse  # noqa: E501
from gs_flex_coordinator.models.get_graph_schema_response import GetGraphSchemaResponse  # noqa: E501
from gs_flex_coordinator.test import BaseTestCase


class TestGraphController(BaseTestCase):
    """GraphController integration test stubs"""

    def test_create_edge_type(self):
        """Test case for create_edge_type

        
        """
        create_edge_type = {"type_name":"type_name","directed":True,"primary_keys":["primary_keys","primary_keys"],"vertex_type_pair_relations":[{"source_vertex":"source_vertex","destination_vertex":"destination_vertex","x_csr_params":{"edge_storage_strategy":"ONLY_IN"},"relation":"MANY_TO_MANY"},{"source_vertex":"source_vertex","destination_vertex":"destination_vertex","x_csr_params":{"edge_storage_strategy":"ONLY_IN"},"relation":"MANY_TO_MANY"}],"description":"description","properties":[{"nullable":True,"property_type":{"primitive_type":"DT_SIGNED_INT32"},"description":"description","default_value":"","property_name":"property_name"},{"nullable":True,"property_type":{"primitive_type":"DT_SIGNED_INT32"},"description":"description","default_value":"","property_name":"property_name"}]}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_id}/schema/edge'.format(graph_id='graph_id_example'),
            method='POST',
            headers=headers,
            data=json.dumps(create_edge_type),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_create_graph(self):
        """Test case for create_graph

        
        """
        create_graph_request = {"schema":{"vertex_types":[{"type_name":"type_name","primary_keys":["primary_keys","primary_keys"],"x_csr_params":{"max_vertex_num":0},"description":"description","properties":[{"nullable":True,"property_type":{"primitive_type":"DT_SIGNED_INT32"},"description":"description","default_value":"","property_name":"property_name"},{"nullable":True,"property_type":{"primitive_type":"DT_SIGNED_INT32"},"description":"description","default_value":"","property_name":"property_name"}]},{"type_name":"type_name","primary_keys":["primary_keys","primary_keys"],"x_csr_params":{"max_vertex_num":0},"description":"description","properties":[{"nullable":True,"property_type":{"primitive_type":"DT_SIGNED_INT32"},"description":"description","default_value":"","property_name":"property_name"},{"nullable":True,"property_type":{"primitive_type":"DT_SIGNED_INT32"},"description":"description","default_value":"","property_name":"property_name"}]}],"edge_types":[{"type_name":"type_name","directed":True,"primary_keys":["primary_keys","primary_keys"],"vertex_type_pair_relations":[{"source_vertex":"source_vertex","destination_vertex":"destination_vertex","x_csr_params":{"edge_storage_strategy":"ONLY_IN"},"relation":"MANY_TO_MANY"},{"source_vertex":"source_vertex","destination_vertex":"destination_vertex","x_csr_params":{"edge_storage_strategy":"ONLY_IN"},"relation":"MANY_TO_MANY"}],"description":"description","properties":[{"nullable":True,"property_type":{"primitive_type":"DT_SIGNED_INT32"},"description":"description","default_value":"","property_name":"property_name"},{"nullable":True,"property_type":{"primitive_type":"DT_SIGNED_INT32"},"description":"description","default_value":"","property_name":"property_name"}]},{"type_name":"type_name","directed":True,"primary_keys":["primary_keys","primary_keys"],"vertex_type_pair_relations":[{"source_vertex":"source_vertex","destination_vertex":"destination_vertex","x_csr_params":{"edge_storage_strategy":"ONLY_IN"},"relation":"MANY_TO_MANY"},{"source_vertex":"source_vertex","destination_vertex":"destination_vertex","x_csr_params":{"edge_storage_strategy":"ONLY_IN"},"relation":"MANY_TO_MANY"}],"description":"description","properties":[{"nullable":True,"property_type":{"primitive_type":"DT_SIGNED_INT32"},"description":"description","default_value":"","property_name":"property_name"},{"nullable":True,"property_type":{"primitive_type":"DT_SIGNED_INT32"},"description":"description","default_value":"","property_name":"property_name"}]}]},"stored_procedures":[{"query":"query","name":"name","description":"description","type":"cpp"},{"query":"query","name":"name","description":"description","type":"cpp"}],"name":"name","description":"description"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph',
            method='POST',
            headers=headers,
            data=json.dumps(create_graph_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_create_vertex_type(self):
        """Test case for create_vertex_type

        
        """
        create_vertex_type = {"type_name":"type_name","primary_keys":["primary_keys","primary_keys"],"x_csr_params":{"max_vertex_num":0},"description":"description","properties":[{"nullable":True,"property_type":{"primitive_type":"DT_SIGNED_INT32"},"description":"description","default_value":"","property_name":"property_name"},{"nullable":True,"property_type":{"primitive_type":"DT_SIGNED_INT32"},"description":"description","default_value":"","property_name":"property_name"}]}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_id}/schema/vertex'.format(graph_id='graph_id_example'),
            method='POST',
            headers=headers,
            data=json.dumps(create_vertex_type),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_edge_type_by_name(self):
        """Test case for delete_edge_type_by_name

        
        """
        query_string = [('source_vertex_type', 'source_vertex_type_example'),
                        ('destination_vertex_type', 'destination_vertex_type_example')]
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_id}/schema/edge/{type_name}'.format(graph_id='graph_id_example', type_name='type_name_example'),
            method='DELETE',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_graph_by_id(self):
        """Test case for delete_graph_by_id

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_id}'.format(graph_id='graph_id_example'),
            method='DELETE',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_vertex_type_by_name(self):
        """Test case for delete_vertex_type_by_name

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_id}/schema/vertex/{type_name}'.format(graph_id='graph_id_example', type_name='type_name_example'),
            method='DELETE',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_graph_by_id(self):
        """Test case for get_graph_by_id

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_id}'.format(graph_id='graph_id_example'),
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_schema_by_id(self):
        """Test case for get_schema_by_id

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_id}/schema'.format(graph_id='graph_id_example'),
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_import_schema_by_id(self):
        """Test case for import_schema_by_id

        
        """
        create_graph_schema_request = {"vertex_types":[{"type_name":"type_name","primary_keys":["primary_keys","primary_keys"],"x_csr_params":{"max_vertex_num":0},"description":"description","properties":[{"nullable":True,"property_type":{"primitive_type":"DT_SIGNED_INT32"},"description":"description","default_value":"","property_name":"property_name"},{"nullable":True,"property_type":{"primitive_type":"DT_SIGNED_INT32"},"description":"description","default_value":"","property_name":"property_name"}]},{"type_name":"type_name","primary_keys":["primary_keys","primary_keys"],"x_csr_params":{"max_vertex_num":0},"description":"description","properties":[{"nullable":True,"property_type":{"primitive_type":"DT_SIGNED_INT32"},"description":"description","default_value":"","property_name":"property_name"},{"nullable":True,"property_type":{"primitive_type":"DT_SIGNED_INT32"},"description":"description","default_value":"","property_name":"property_name"}]}],"edge_types":[{"type_name":"type_name","directed":True,"primary_keys":["primary_keys","primary_keys"],"vertex_type_pair_relations":[{"source_vertex":"source_vertex","destination_vertex":"destination_vertex","x_csr_params":{"edge_storage_strategy":"ONLY_IN"},"relation":"MANY_TO_MANY"},{"source_vertex":"source_vertex","destination_vertex":"destination_vertex","x_csr_params":{"edge_storage_strategy":"ONLY_IN"},"relation":"MANY_TO_MANY"}],"description":"description","properties":[{"nullable":True,"property_type":{"primitive_type":"DT_SIGNED_INT32"},"description":"description","default_value":"","property_name":"property_name"},{"nullable":True,"property_type":{"primitive_type":"DT_SIGNED_INT32"},"description":"description","default_value":"","property_name":"property_name"}]},{"type_name":"type_name","directed":True,"primary_keys":["primary_keys","primary_keys"],"vertex_type_pair_relations":[{"source_vertex":"source_vertex","destination_vertex":"destination_vertex","x_csr_params":{"edge_storage_strategy":"ONLY_IN"},"relation":"MANY_TO_MANY"},{"source_vertex":"source_vertex","destination_vertex":"destination_vertex","x_csr_params":{"edge_storage_strategy":"ONLY_IN"},"relation":"MANY_TO_MANY"}],"description":"description","properties":[{"nullable":True,"property_type":{"primitive_type":"DT_SIGNED_INT32"},"description":"description","default_value":"","property_name":"property_name"},{"nullable":True,"property_type":{"primitive_type":"DT_SIGNED_INT32"},"description":"description","default_value":"","property_name":"property_name"}]}]}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_id}/schema'.format(graph_id='graph_id_example'),
            method='POST',
            headers=headers,
            data=json.dumps(create_graph_schema_request),
            content_type='application/json')
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
