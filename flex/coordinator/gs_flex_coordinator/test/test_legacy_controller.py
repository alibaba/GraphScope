import unittest

from flask import json

from gs_flex_coordinator.models.groot_graph import GrootGraph  # noqa: E501
from gs_flex_coordinator.models.groot_schema import GrootSchema  # noqa: E501
from gs_flex_coordinator.test import BaseTestCase


class TestLegacyController(BaseTestCase):
    """LegacyController integration test stubs"""

    def test_get_groot_schema(self):
        """Test case for get_groot_schema

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/groot/graph/{graph_name}/schema'.format(graph_name='graph_name_example'),
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_import_schema(self):
        """Test case for import_schema

        
        """
        groot_schema = {"vertices":[{"label":"label","properties":[{"is_primary_key":True,"name":"name","id":0,"type":"STRING"},{"is_primary_key":True,"name":"name","id":0,"type":"STRING"}]},{"label":"label","properties":[{"is_primary_key":True,"name":"name","id":0,"type":"STRING"},{"is_primary_key":True,"name":"name","id":0,"type":"STRING"}]}],"edges":[{"label":"label","relations":[{"src_label":"src_label","dst_label":"dst_label"},{"src_label":"src_label","dst_label":"dst_label"}],"properties":[{"is_primary_key":True,"name":"name","id":0,"type":"STRING"},{"is_primary_key":True,"name":"name","id":0,"type":"STRING"}]},{"label":"label","relations":[{"src_label":"src_label","dst_label":"dst_label"},{"src_label":"src_label","dst_label":"dst_label"}],"properties":[{"is_primary_key":True,"name":"name","id":0,"type":"STRING"},{"is_primary_key":True,"name":"name","id":0,"type":"STRING"}]}]}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/groot/graph/{graph_name}/schema'.format(graph_name='graph_name_example'),
            method='POST',
            headers=headers,
            data=json.dumps(groot_schema),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_list_groot_graph(self):
        """Test case for list_groot_graph

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/groot/graph',
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
