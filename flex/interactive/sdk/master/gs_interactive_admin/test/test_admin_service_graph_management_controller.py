import unittest

from flask import json

from gs_interactive_admin.models.api_response_with_code import APIResponseWithCode  # noqa: E501
from gs_interactive_admin.models.create_edge_type import CreateEdgeType  # noqa: E501
from gs_interactive_admin.models.create_graph_request import CreateGraphRequest  # noqa: E501
from gs_interactive_admin.models.create_graph_response import CreateGraphResponse  # noqa: E501
from gs_interactive_admin.models.create_vertex_type import CreateVertexType  # noqa: E501
from gs_interactive_admin.models.get_graph_response import GetGraphResponse  # noqa: E501
from gs_interactive_admin.models.get_graph_schema_response import GetGraphSchemaResponse  # noqa: E501
from gs_interactive_admin.models.get_graph_statistics_response import GetGraphStatisticsResponse  # noqa: E501
from gs_interactive_admin.models.job_response import JobResponse  # noqa: E501
from gs_interactive_admin.models.schema_mapping import SchemaMapping  # noqa: E501
from gs_interactive_admin.models.snapshot_status import SnapshotStatus  # noqa: E501
from gs_interactive_admin.test import BaseTestCase


class TestAdminServiceGraphManagementController(BaseTestCase):
    """AdminServiceGraphManagementController integration test stubs"""

    def test_create_dataloading_job(self):
        """Test case for create_dataloading_job

        
        """
        schema_mapping = {"loading_config":{"x_csr_params":{"parallelism":0,"build_csr_in_mem":True,"use_mmap_vector":True},"format":{"metadata":{"key":""},"type":"type"},"destination":"destination","import_option":"init","data_source":{"scheme":"odps","location":"location"}},"edge_mappings":[{"inputs":["inputs","inputs"],"source_vertex_mappings":[{"column":{"name":"name","index":6},"property":"id"},{"column":{"name":"name","index":6},"property":"id"}],"destination_vertex_mappings":[{"column":{"name":"name","index":6},"property":"id"},{"column":{"name":"name","index":6},"property":"id"}],"column_mappings":[{"column":{"name":"name","index":6},"property":"property"},{"column":{"name":"name","index":6},"property":"property"}],"type_triplet":{"edge":"edge","source_vertex":"source_vertex","destination_vertex":"destination_vertex"}},{"inputs":["inputs","inputs"],"source_vertex_mappings":[{"column":{"name":"name","index":6},"property":"id"},{"column":{"name":"name","index":6},"property":"id"}],"destination_vertex_mappings":[{"column":{"name":"name","index":6},"property":"id"},{"column":{"name":"name","index":6},"property":"id"}],"column_mappings":[{"column":{"name":"name","index":6},"property":"property"},{"column":{"name":"name","index":6},"property":"property"}],"type_triplet":{"edge":"edge","source_vertex":"source_vertex","destination_vertex":"destination_vertex"}}],"vertex_mappings":[{"type_name":"type_name","inputs":["file:///path/to/person.csv","file:///path/to/person.csv"],"column_mappings":[{"column":{"name":"name","index":6},"property":"property"},{"column":{"name":"name","index":6},"property":"property"}]},{"type_name":"type_name","inputs":["file:///path/to/person.csv","file:///path/to/person.csv"],"column_mappings":[{"column":{"name":"name","index":6},"property":"property"},{"column":{"name":"name","index":6},"property":"property"}]}]}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/dataloading'.format(graph_id='graph_id_example'),
            method='POST',
            headers=headers,
            data=json.dumps(schema_mapping),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_create_edge_type(self):
        """Test case for create_edge_type

        
        """
        create_edge_type = null
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/schema/edge'.format(graph_id='graph_id_example'),
            method='POST',
            headers=headers,
            data=json.dumps(create_edge_type),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_create_graph(self):
        """Test case for create_graph

        
        """
        create_graph_request = {"schema":{"vertex_types":[null,null],"edge_types":[null,null]},"stored_procedures":[{"query":"MATCH(a) return COUNT(a);","name":"query1","description":"A sample stored procedure","type":"cpp"},{"query":"MATCH(a) return COUNT(a);","name":"query1","description":"A sample stored procedure","type":"cpp"}],"name":"modern_graph","description":"A default description"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/v1/graph',
            method='POST',
            headers=headers,
            data=json.dumps(create_graph_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_create_vertex_type(self):
        """Test case for create_vertex_type

        
        """
        create_vertex_type = null
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/schema/vertex'.format(graph_id='graph_id_example'),
            method='POST',
            headers=headers,
            data=json.dumps(create_vertex_type),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_edge_type(self):
        """Test case for delete_edge_type

        
        """
        query_string = [('type_name', 'type_name_example'),
                        ('source_vertex_type', 'source_vertex_type_example'),
                        ('destination_vertex_type', 'destination_vertex_type_example')]
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/schema/edge'.format(graph_id='graph_id_example'),
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
            '/v1/graph/{graph_id}'.format(graph_id='graph_id_example'),
            method='DELETE',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_vertex_type(self):
        """Test case for delete_vertex_type

        
        """
        query_string = [('type_name', 'type_name_example')]
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/schema/vertex'.format(graph_id='graph_id_example'),
            method='DELETE',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_graph(self):
        """Test case for get_graph

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}'.format(graph_id='graph_id_example'),
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_graph_statistic(self):
        """Test case for get_graph_statistic

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/statistics'.format(graph_id='graph_id_example'),
            method='GET',
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
            '/v1/graph/{graph_id}/schema'.format(graph_id='graph_id_example'),
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_snapshot_status(self):
        """Test case for get_snapshot_status

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/snapshot/{snapshot_id}/status'.format(graph_id='graph_id_example', snapshot_id=56),
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
            '/v1/graph',
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_edge_type(self):
        """Test case for update_edge_type

        
        """
        create_edge_type = null
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/schema/edge'.format(graph_id='graph_id_example'),
            method='PUT',
            headers=headers,
            data=json.dumps(create_edge_type),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_vertex_type(self):
        """Test case for update_vertex_type

        
        """
        create_vertex_type = null
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/v1/graph/{graph_id}/schema/vertex'.format(graph_id='graph_id_example'),
            method='PUT',
            headers=headers,
            data=json.dumps(create_vertex_type),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
