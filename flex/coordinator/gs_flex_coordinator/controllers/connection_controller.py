import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gs_flex_coordinator.core.config import SOLUTION
from gs_flex_coordinator.models.connection import Connection  # noqa: E501
from gs_flex_coordinator.models.connection_status import ConnectionStatus  # noqa: E501
from gs_flex_coordinator import util


def close():  # noqa: E501
    """close

    Close the connection with coordinator # noqa: E501


    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return "Disconnect with coordinator successfully."


def connect(connection):  # noqa: E501
    """connect

    Connect to coordinator service # noqa: E501

    :param connection: 
    :type connection: dict | bytes

    :rtype: Union[ConnectionStatus, Tuple[ConnectionStatus, int], Tuple[ConnectionStatus, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        connection = Connection.from_dict(connexion.request.get_json())  # noqa: E501
    connection_status = ConnectionStatus.from_dict({
        "status": "CONNECTED",
        "solution": SOLUTION
    })
    return connection_status
