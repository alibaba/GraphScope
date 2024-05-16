import unittest

from flask import json

from gs_flex_coordinator.models.error import Error  # noqa: E501
from gs_flex_coordinator.models.upload_file_response import UploadFileResponse  # noqa: E501
from gs_flex_coordinator.test import BaseTestCase


class TestUtilsController(BaseTestCase):
    """UtilsController integration test stubs"""

    @unittest.skip("multipart/form-data not supported by Connexion")
    def test_upload_file(self):
        """Test case for upload_file

        
        """
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'multipart/form-data',
        }
        data = dict(filestorage='/path/to/file')
        response = self.client.open(
            '/api/v1/file/uploading',
            method='POST',
            headers=headers,
            data=data,
            content_type='multipart/form-data')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
