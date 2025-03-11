import unittest

from flask import json

from gs_interactive_admin.models.job_status import JobStatus  # noqa: E501
from gs_interactive_admin.test import BaseTestCase


class TestAdminServiceJobManagementController(BaseTestCase):
    """AdminServiceJobManagementController integration test stubs"""

    def test_delete_job_by_id(self):
        """Test case for delete_job_by_id

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/GRAPHSCOPE/InteractiveAPI/1.0.0/v1/job/{job_id}'.format(job_id='job_id_example'),
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
            '/GRAPHSCOPE/InteractiveAPI/1.0.0/v1/job/{job_id}'.format(job_id='job_id_example'),
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
            '/GRAPHSCOPE/InteractiveAPI/1.0.0/v1/job',
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
