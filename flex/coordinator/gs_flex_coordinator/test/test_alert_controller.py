import unittest

from flask import json

from gs_flex_coordinator.models.alert_message import AlertMessage  # noqa: E501
from gs_flex_coordinator.models.alert_receiver import AlertReceiver  # noqa: E501
from gs_flex_coordinator.models.alert_rule import AlertRule  # noqa: E501
from gs_flex_coordinator.models.update_alert_messages_request import UpdateAlertMessagesRequest  # noqa: E501
from gs_flex_coordinator.test import BaseTestCase


class TestAlertController(BaseTestCase):
    """AlertController integration test stubs"""

    def test_delete_alert_rule(self):
        """Test case for delete_alert_rule

        
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/api/v1/alert/rule/{rule_name}'.format(rule_name='rule_name_example'),
            method='DELETE',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_receiverby_id(self):
        """Test case for delete_receiverby_id

        
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

    def test_list_alert_messages(self):
        """Test case for list_alert_messages

        
        """
        query_string = [('type', 'type_example'),
                        ('status', 'status_example'),
                        ('severity', 'severity_example'),
                        ('start_time', 'start_time_example'),
                        ('end_time', 'end_time_example')]
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

    def test_list_receivers(self):
        """Test case for list_receivers

        
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

    def test_register_receiver(self):
        """Test case for register_receiver

        
        """
        alert_receiver = {"webhook_url":"webhook_url","is_at_all":True,"receiver_id":"receiver_id","enable":True,"at_user_ids":["at_user_ids","at_user_ids"],"type":"webhook","message":"message"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/alert/receiver',
            method='POST',
            headers=headers,
            data=json.dumps(alert_receiver),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_alert_messages(self):
        """Test case for update_alert_messages

        
        """
        update_alert_messages_request = gs_flex_coordinator.UpdateAlertMessagesRequest()
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/alert/message',
            method='PUT',
            headers=headers,
            data=json.dumps(update_alert_messages_request),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_alert_rule_by_name(self):
        """Test case for update_alert_rule_by_name

        
        """
        alert_rule = {"severity":"warning","conditions_description":"conditions_description","enable":True,"name":"name","metric_type":"node","frequency":0}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/alert/rule/{rule_name}'.format(rule_name='rule_name_example'),
            method='PUT',
            headers=headers,
            data=json.dumps(alert_rule),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_receiverby_id(self):
        """Test case for update_receiverby_id

        
        """
        alert_receiver = {"webhook_url":"webhook_url","is_at_all":True,"receiver_id":"receiver_id","enable":True,"at_user_ids":["at_user_ids","at_user_ids"],"type":"webhook","message":"message"}
        headers = { 
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        response = self.client.open(
            '/api/v1/alert/receiver/{receiver_id}'.format(receiver_id='receiver_id_example'),
            method='PUT',
            headers=headers,
            data=json.dumps(alert_receiver),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
