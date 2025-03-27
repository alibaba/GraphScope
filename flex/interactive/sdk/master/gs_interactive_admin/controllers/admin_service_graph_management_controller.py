import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gs_interactive_admin.models.api_response_with_code import (
    APIResponseWithCode,
)  # noqa: E501
from gs_interactive_admin.models.create_edge_type import CreateEdgeType  # noqa: E501
from gs_interactive_admin.models.create_graph_request import (
    CreateGraphRequest,
)  # noqa: E501
from gs_interactive_admin.models.create_graph_response import (
    CreateGraphResponse,
)  # noqa: E501
from gs_interactive_admin.models.create_vertex_type import (
    CreateVertexType,
)  # noqa: E501
from gs_interactive_admin.models.get_graph_response import (
    GetGraphResponse,
)  # noqa: E501
from gs_interactive_admin.models.get_graph_schema_response import (
    GetGraphSchemaResponse,
)  # noqa: E501
from gs_interactive_admin.models.get_graph_statistics_response import (
    GetGraphStatisticsResponse,
)  # noqa: E501
from gs_interactive_admin.models.job_response import JobResponse  # noqa: E501
from gs_interactive_admin.models.schema_mapping import SchemaMapping  # noqa: E501
from gs_interactive_admin.models.snapshot_status import SnapshotStatus  # noqa: E501
from gs_interactive_admin import util
from gs_interactive_admin.core.metadata.metadata_store import get_metadata_store
from gs_interactive_admin.core.job.job_manager import get_job_manager
from gs_interactive_admin.core.service.service_manager import get_service_manager


def create_dataloading_job(graph_id, schema_mapping):  # noqa: E501
    """create_dataloading_job
    TODO: currently we launch the job in master, we should launch the job in a temporary pod in the future.

    Create a dataloading job # noqa: E501

    :param graph_id: The id of graph to do bulk loading.
    :type graph_id: str
    :param schema_mapping:
    :type schema_mapping: dict | bytes

    :rtype: Union[JobResponse, Tuple[JobResponse, int], Tuple[JobResponse, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        schema_mapping = SchemaMapping.from_dict(
            connexion.request.get_json()
        )  # noqa: E501
        job_id = get_job_manager().create_dataloading_job(
            graph_id=graph_id, schema_mapping=schema_mapping.to_dict()
        )
        return JobResponse(job_id=job_id)
    else:
        raise RuntimeError("Invalid request")


def create_graph(create_graph_request):  # noqa: E501
    """create_graph

    Create a new graph # noqa: E501

    :param create_graph_request:
    :type create_graph_request: dict | bytes

    :rtype: Union[CreateGraphResponse, Tuple[CreateGraphResponse, int], Tuple[CreateGraphResponse, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        create_graph_request = CreateGraphRequest.from_dict(
            connexion.request.get_json()
        )  # noqa: E501
        graph_id = get_metadata_store().create_graph_meta(
            create_graph_request.to_dict()
        )
        return CreateGraphResponse(graph_id=graph_id)
    else:
        raise RuntimeError("Invalid request")


def delete_graph(graph_id):  # noqa: E501
    """delete_graph

    Delete a graph by id # noqa: E501
    TODO: Should we stop the service before we delete the graph?

    :param graph_id: The id of graph to delete
    :type graph_id: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    # Before we delete graph, we need to make sure the service on the graph has been stopped.
    if get_service_manager().is_graph_running(graph_id):
        # bad request
        return APIResponseWithCode(
            code=400, message=f"The service on the graph {graph_id} has not been stopped"
        )
    return get_metadata_store().delete_graph_meta(graph_id)


def get_graph(graph_id):  # noqa: E501
    """get_graph

    Get a graph by name # noqa: E501

    :param graph_id: The id of graph to get
    :type graph_id: str

    :rtype: Union[GetGraphResponse, Tuple[GetGraphResponse, int], Tuple[GetGraphResponse, int, Dict[str, str]]
    """
    return get_metadata_store().get_graph_meta(graph_id)


def get_graph_statistic(graph_id):  # noqa: E501
    """get_graph_statistic

    Get the statics info of a graph, including number of vertices for each label, number of edges for each label. # noqa: E501

    :param graph_id: The id of graph to get statistics
    :type graph_id: str

    :rtype: Union[GetGraphStatisticsResponse, Tuple[GetGraphStatisticsResponse, int], Tuple[GetGraphStatisticsResponse, int, Dict[str, str]]
    """
    raise get_metadata_store().get_graph_statistics(graph_id)


def get_schema(graph_id):  # noqa: E501
    """get_schema

    Get schema by graph id # noqa: E501

    :param graph_id: The id of graph to get schema
    :type graph_id: str

    :rtype: Union[GetGraphSchemaResponse, Tuple[GetGraphSchemaResponse, int], Tuple[GetGraphSchemaResponse, int, Dict[str, str]]
    """
    return get_metadata_store().get_graph_schema(graph_id)


def list_graphs():  # noqa: E501
    """list_graphs

    List all graphs # noqa: E501


    :rtype: Union[List[GetGraphResponse], Tuple[List[GetGraphResponse], int], Tuple[List[GetGraphResponse], int, Dict[str, str]]
    """
    return dict(get_metadata_store().get_all_graph_meta())


################################################################
def create_edge_type(graph_id, create_edge_type=None):  # noqa: E501
    """create_edge_type

    Create a edge type # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param create_edge_type:
    :type create_edge_type: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    raise RuntimeError("Not supported")


def create_vertex_type(graph_id, create_vertex_type):  # noqa: E501
    """create_vertex_type

    Create a vertex type # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param create_vertex_type:
    :type create_vertex_type: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    raise RuntimeError("Not supported")


def update_vertex_type(graph_id, create_vertex_type):  # noqa: E501
    """update_vertex_type

    Update a vertex type to add more properties # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param create_vertex_type:
    :type create_vertex_type: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    raise RuntimeError("Not supported")


def update_edge_type(graph_id, create_edge_type):  # noqa: E501
    """update_edge_type

    Update an edge type to add more properties # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param create_edge_type:
    :type create_edge_type: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    raise RuntimeError("Not supported")


def delete_edge_type(
    graph_id, type_name, source_vertex_type, destination_vertex_type
):  # noqa: E501
    """delete_edge_type

    Delete an edge type by name # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param type_name:
    :type type_name: str
    :param source_vertex_type:
    :type source_vertex_type: str
    :param destination_vertex_type:
    :type destination_vertex_type: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    raise RuntimeError("Not supported")


def get_snapshot_status(graph_id, snapshot_id):  # noqa: E501
    """get_snapshot_status

    Get the status of a snapshot by id # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param snapshot_id:
    :type snapshot_id: int

    :rtype: Union[SnapshotStatus, Tuple[SnapshotStatus, int], Tuple[SnapshotStatus, int, Dict[str, str]]
    """
    raise RuntimeError("Not supported")


def delete_vertex_type(graph_id, type_name):  # noqa: E501
    """delete_vertex_type

    Delete a vertex type by name # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param type_name:
    :type type_name: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    raise RuntimeError("Not supported")
