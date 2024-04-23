import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gs_flex_coordinator.core.alert import alert_manager
from gs_flex_coordinator.core import handle_api_exception
from gs_flex_coordinator.models.alert_message import AlertMessage  # noqa: E501
from gs_flex_coordinator.models.alert_receiver import AlertReceiver  # noqa: E501
from gs_flex_coordinator.models.alert_rule import AlertRule  # noqa: E501
from gs_flex_coordinator.models.update_alert_messages_request import UpdateAlertMessagesRequest  # noqa: E501
from gs_flex_coordinator import util


@handle_api_exception()
def delete_alert_rule_by_name(rule_name):  # noqa: E501
    """delete_alert_rule

     # noqa: E501

    :param rule_name: 
    :type rule_name: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return alert_manager.delete_alert_rule_by_name(rule_name)


@handle_api_exception()
def delete_receiver_by_id(receiver_id):  # noqa: E501
    """delete_receiver_by_id

     # noqa: E501

    :param receiver_id: 
    :type receiver_id: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return alert_manager.delete_receiver_by_id(receiver_id)


@handle_api_exception()
def list_alert_messages(alert_type=None, status=None, severity=None, start_time=None, end_time=None):  # noqa: E501
    """list_alert_messages

     # noqa: E501

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

    :rtype: Union[List[AlertMessage], Tuple[List[AlertMessage], int], Tuple[List[AlertMessage], int, Dict[str, str]]
    """
    return alert_manager.list_alert_messages(alert_type, status, severity, start_time, end_time)


@handle_api_exception()
def list_alert_rules():  # noqa: E501
    """list_alert_rules

     # noqa: E501


    :rtype: Union[List[AlertRule], Tuple[List[AlertRule], int], Tuple[List[AlertRule], int, Dict[str, str]]
    """
    return alert_manager.list_alert_rules()


@handle_api_exception()
def list_receivers():  # noqa: E501
    """list_receivers

     # noqa: E501


    :rtype: Union[List[AlertReceiver], Tuple[List[AlertReceiver], int], Tuple[List[AlertReceiver], int, Dict[str, str]]
    """
    return alert_manager.list_receivers()


@handle_api_exception()
def register_receiver(alert_receiver):  # noqa: E501
    """register_receiver

     # noqa: E501

    :param alert_receiver: 
    :type alert_receiver: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        alert_receiver = AlertReceiver.from_dict(connexion.request.get_json())  # noqa: E501
    return alert_manager.register_receiver(alert_receiver)
    return 'do some magic!'


@handle_api_exception()
def update_alert_messages(update_alert_messages_request=None):  # noqa: E501
    """update_alert_messages

    Update alert messages in batch # noqa: E501

    :param update_alert_messages_request: 
    :type update_alert_messages_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        update_alert_messages_request = UpdateAlertMessagesRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return alert_manager.update_alert_messages(
        update_alert_messages_request.messages,
        update_alert_messages_request.batch_status,
        update_alert_messages_request.batch_delete
    )


@handle_api_exception()
def update_alert_rule_by_name(rule_name, alert_rule=None):  # noqa: E501
    """update_alert_rule_by_name

     # noqa: E501

    :param rule_name: 
    :type rule_name: str
    :param alert_rule: 
    :type alert_rule: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        alert_rule = AlertRule.from_dict(connexion.request.get_json())  # noqa: E501
    return alert_manager.update_alert_rule_by_name(rule_name, alert_rule)


@handle_api_exception()
def update_receiver_by_id(receiver_id, alert_receiver=None):  # noqa: E501
    """update_receiver_by_id

     # noqa: E501

    :param receiver_id: 
    :type receiver_id: str
    :param alert_receiver: 
    :type alert_receiver: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        alert_receiver = AlertReceiver.from_dict(connexion.request.get_json())  # noqa: E501
    return alert_manager.update_receiver_by_id(receiver_id, alert_receiver)
