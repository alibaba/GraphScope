import unittest

from flask import json
import pytest

from gs_interactive_admin.models.job_status import JobStatus  # noqa: E501
from gs_interactive_admin.test import BaseTestCase


class TestEndToEnd(BaseTestCase):
    """A comprehensive test case contains creating graph, importing data, running queries, and deleting graph"""
    
    def setUp():
        pass
    
    @pytest.mark.order(1)
    def test_create_graph(self):
        pass
    
    @pytest.mark.order(2)
    def test_import_data(self):
        pass
    
    @pytest.mark.order(3)
    def test_start_service(self):
        pass
    
    @pytest.mark.order(4)
    def test_run_query(self):
        pass
    
    
    def tearDown():
        pass



if __name__ == "__main__":
    unittest.main()
