import unittest

from flask import json

from gs_flex_coordinator.test import BaseTestCase


class TestUtilsController(BaseTestCase):
    """UtilsController integration test stubs"""

    @unittest.skip("application/octet-stream not supported by Connexion")
    def test_upload_file(self):
        """Test case for upload_file

        
        """
        body = '/path/to/file'
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/octet-stream',
        }
        response = self.client.open(
            '/api/v1/file/uploading',
            method='POST',
            headers=headers,
            data=json.dumps(body),
            content_type='application/octet-stream')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
