import unittest

from flask import json

from gs_flex_coordinator.models.create_alert_receiver_request import CreateAlertReceiverRequest  # noqa: E501
from gs_flex_coordinator.models.create_alert_rule_request import CreateAlertRuleRequest  # noqa: E501
from gs_flex_coordinator.models.error import Error  # noqa: E501
from gs_flex_coordinator.models.get_alert_message_response import GetAlertMessageResponse  # noqa: E501
from gs_flex_coordinator.models.get_alert_receiver_response import GetAlertReceiverResponse  # noqa: E501
from gs_flex_coordinator.models.get_alert_rule_response import GetAlertRuleResponse  # noqa: E501
from gs_flex_coordinator.models.update_alert_message_status_request import UpdateAlertMessageStatusRequest  # noqa: E501
from gs_flex_coordinator.test import BaseTestCase


class TestAlertController(BaseTestCase):
    """AlertController integration test stubs"""

    def test_create_alert_receiver(self):
        """Test case for create_alert_receiver

        
        """
        create_alert_receiver_request = {"webhook_url":"webhook_url","is_at_all":True,"enable":True,"at_user_ids":["at_user_ids","at_user_ids"],"type":"webhook"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/alert/receiver',
            method='POST',
            headers=headers,
            data=json.dumps(create_alert_receiver_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_alert_message_in_batch(self):
        """Test case for delete_alert_message_in_batch

        
        """
        query_string = [('message_ids', 'message_ids_example')]
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/alert/message-collection',
            method='DELETE',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_alert_receiver_by_id(self):
        """Test case for delete_alert_receiver_by_id

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/alert/receiver/{receiver_id}'.format(receiver_id='receiver_id_example'),
            method='DELETE',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_alert_rule_by_id(self):
        """Test case for delete_alert_rule_by_id

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/alert/rule/{rule_id}'.format(rule_id='rule_id_example'),
            method='DELETE',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_list_alert_messages(self):
        """Test case for list_alert_messages

        
        """
        query_string = [('alert_type', 'alert_type_example'),
                        ('status', 'status_example'),
                        ('severity', 'severity_example'),
                        ('start_time', 'start_time_example'),
                        ('end_time', 'end_time_example'),
                        ('limit', 56)]
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/alert/message',
            method='GET',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_list_alert_receivers(self):
        """Test case for list_alert_receivers

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/alert/receiver',
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_list_alert_rules(self):
        """Test case for list_alert_rules

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/alert/rule',
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_alert_message_in_batch(self):
        """Test case for update_alert_message_in_batch

        
        """
        update_alert_message_status_request = {"message_ids":["message_ids","message_ids"],"status":"unsolved"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/alert/message-collection/status',
            method='PUT',
            headers=headers,
            data=json.dumps(update_alert_message_status_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_alert_receiver_by_id(self):
        """Test case for update_alert_receiver_by_id

        
        """
        create_alert_receiver_request = {"webhook_url":"webhook_url","is_at_all":True,"enable":True,"at_user_ids":["at_user_ids","at_user_ids"],"type":"webhook"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/alert/receiver/{receiver_id}'.format(receiver_id='receiver_id_example'),
            method='PUT',
            headers=headers,
            data=json.dumps(create_alert_receiver_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_alert_rule_by_id(self):
        """Test case for update_alert_rule_by_id

        
        """
        create_alert_rule_request = {"severity":"warning","conditions_description":"conditions_description","enable":True,"name":"name","metric_type":"node","frequency":0}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/alert/rule/{rule_id}'.format(rule_id='rule_id_example'),
            method='PUT',
            headers=headers,
            data=json.dumps(create_alert_rule_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
