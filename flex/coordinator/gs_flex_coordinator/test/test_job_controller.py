import unittest

from flask import json

from gs_flex_coordinator.models.job_response import JobResponse  # noqa: E501
from gs_flex_coordinator.models.job_status import JobStatus  # noqa: E501
from gs_flex_coordinator.models.schema_mapping import SchemaMapping  # noqa: E501
from gs_flex_coordinator.test import BaseTestCase


class TestJobController(BaseTestCase):
    """JobController integration test stubs"""

    def test_create_dataloading_job(self):
        """Test case for create_dataloading_job

        
        """
        schema_mapping = {"loading_config":{"format":{"metadata":{"key":""},"type":"type"},"import_option":"init","data_source":{"scheme":"file"}},"edge_mappings":[{"inputs":["inputs","inputs"],"source_vertex_mappings":[{"column":{"name":"name","index":0}},{"column":{"name":"name","index":0}}],"destination_vertex_mappings":[{"column":{"name":"name","index":0}},{"column":{"name":"name","index":0}}],"column_mappings":[{"column":{"name":"name","index":0},"property":"property"},{"column":{"name":"name","index":0},"property":"property"}],"type_triplet":{"edge":"edge","source_vertex":"source_vertex","destination_vertex":"destination_vertex"}},{"inputs":["inputs","inputs"],"source_vertex_mappings":[{"column":{"name":"name","index":0}},{"column":{"name":"name","index":0}}],"destination_vertex_mappings":[{"column":{"name":"name","index":0}},{"column":{"name":"name","index":0}}],"column_mappings":[{"column":{"name":"name","index":0},"property":"property"},{"column":{"name":"name","index":0},"property":"property"}],"type_triplet":{"edge":"edge","source_vertex":"source_vertex","destination_vertex":"destination_vertex"}}],"graph":"graph","vertex_mappings":[{"type_name":"type_name","inputs":["inputs","inputs"],"column_mappings":[{"column":{"name":"name","index":0},"property":"property"},{"column":{"name":"name","index":0},"property":"property"}]},{"type_name":"type_name","inputs":["inputs","inputs"],"column_mappings":[{"column":{"name":"name","index":0},"property":"property"},{"column":{"name":"name","index":0},"property":"property"}]}]}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/graph/{graph_name}/dataloading'.format(graph_name='graph_name_example'),
            method='POST',
            headers=headers,
            data=json.dumps(schema_mapping),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_job_by_id(self):
        """Test case for delete_job_by_id

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/job/{job_id}'.format(job_id='job_id_example'),
            method='DELETE',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_job_by_id(self):
        """Test case for get_job_by_id

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/job/{job_id}'.format(job_id='job_id_example'),
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_list_jobs(self):
        """Test case for list_jobs

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/job',
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
