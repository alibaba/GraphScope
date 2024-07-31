import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gscoordinator.flex.models.create_alert_receiver_request import CreateAlertReceiverRequest  # noqa: E501
from gscoordinator.flex.models.create_alert_rule_request import CreateAlertRuleRequest  # noqa: E501
from gscoordinator.flex.models.error import Error  # noqa: E501
from gscoordinator.flex.models.get_alert_message_response import GetAlertMessageResponse  # noqa: E501
from gscoordinator.flex.models.get_alert_receiver_response import GetAlertReceiverResponse  # noqa: E501
from gscoordinator.flex.models.get_alert_rule_response import GetAlertRuleResponse  # noqa: E501
from gscoordinator.flex.models.update_alert_message_status_request import UpdateAlertMessageStatusRequest  # noqa: E501
from gscoordinator.flex import util


def create_alert_receiver(create_alert_receiver_request):  # noqa: E501
    """create_alert_receiver

    Create a new alert receiver # noqa: E501

    :param create_alert_receiver_request: 
    :type create_alert_receiver_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        create_alert_receiver_request = CreateAlertReceiverRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def delete_alert_message_in_batch(message_ids):  # noqa: E501
    """delete_alert_message_in_batch

    Delete alert message in batch # noqa: E501

    :param message_ids: A list of message id separated by comma, e.g. id1,id2,id3
    :type message_ids: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return 'do some magic!'


def delete_alert_receiver_by_id(receiver_id):  # noqa: E501
    """delete_alert_receiver_by_id

    Delete the alert receiver by ID # noqa: E501

    :param receiver_id: 
    :type receiver_id: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return 'do some magic!'


def delete_alert_rule_by_id(rule_id):  # noqa: E501
    """delete_alert_rule_by_id

     # noqa: E501

    :param rule_id: 
    :type rule_id: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return 'do some magic!'


def list_alert_messages(alert_type=None, status=None, severity=None, start_time=None, end_time=None, limit=None):  # noqa: E501
    """list_alert_messages

    List all alert messages # noqa: E501

    :param alert_type: 
    :type alert_type: str
    :param status: 
    :type status: str
    :param severity: 
    :type severity: str
    :param start_time: format with \&quot;2023-02-21-11-56-30\&quot;
    :type start_time: str
    :param end_time: format with \&quot;2023-02-21-11-56-30\&quot;
    :type end_time: str
    :param limit: 
    :type limit: int

    :rtype: Union[List[GetAlertMessageResponse], Tuple[List[GetAlertMessageResponse], int], Tuple[List[GetAlertMessageResponse], int, Dict[str, str]]
    """
    return 'do some magic!'


def list_alert_receivers():  # noqa: E501
    """list_alert_receivers

    List all alert receivers # noqa: E501


    :rtype: Union[List[GetAlertReceiverResponse], Tuple[List[GetAlertReceiverResponse], int], Tuple[List[GetAlertReceiverResponse], int, Dict[str, str]]
    """
    return 'do some magic!'


def list_alert_rules():  # noqa: E501
    """list_alert_rules

    List all alert rules # noqa: E501


    :rtype: Union[List[GetAlertRuleResponse], Tuple[List[GetAlertRuleResponse], int], Tuple[List[GetAlertRuleResponse], int, Dict[str, str]]
    """
    return 'do some magic!'


def update_alert_message_in_batch(update_alert_message_status_request=None):  # noqa: E501
    """update_alert_message_in_batch

    Update the message status in batch # noqa: E501

    :param update_alert_message_status_request: 
    :type update_alert_message_status_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        update_alert_message_status_request = UpdateAlertMessageStatusRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def update_alert_receiver_by_id(receiver_id, create_alert_receiver_request=None):  # noqa: E501
    """update_alert_receiver_by_id

    Update alert receiver by ID # noqa: E501

    :param receiver_id: 
    :type receiver_id: str
    :param create_alert_receiver_request: 
    :type create_alert_receiver_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        create_alert_receiver_request = CreateAlertReceiverRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def update_alert_rule_by_id(rule_id, create_alert_rule_request=None):  # noqa: E501
    """update_alert_rule_by_id

     # noqa: E501

    :param rule_id: 
    :type rule_id: str
    :param create_alert_rule_request: 
    :type create_alert_rule_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        create_alert_rule_request = CreateAlertRuleRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'
