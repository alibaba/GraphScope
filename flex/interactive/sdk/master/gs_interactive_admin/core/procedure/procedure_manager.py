from abc import ABCMeta
from abc import abstractmethod

import logging
import os

from gs_interactive_admin.core.config import Config, CODE_GEN_BIN,CODE_GEN_TMP_DIR
from gs_interactive_admin.util import dump_file,check_field_in_dict
from gs_interactive_admin.core.metadata.metadata_store import get_metadata_store
from gs_interactive_admin.core.metadata.metadata_store import IMetadataStore
from gs_interactive_admin.models.create_procedure_request import CreateProcedureRequest
from gs_interactive_admin.models.get_procedure_response import GetProcedureResponse
from gs_interactive_admin.models.update_procedure_request import UpdateProcedureRequest
from gs_interactive_admin.util import get_current_time_stamp_ms
import subprocess
import yaml

logger = logging.getLogger("interactive")


class ProcedureManager(metaclass=ABCMeta):
    """The interface of procedure manager.

    Args:
        metaclass (_type_, optional): _description_. Defaults to ABCMeta.
    """
    @abstractmethod
    def open(self):
        """Open the procedure manager."""
        pass
    
    @abstractmethod
    def close(self):
        """Close the procedure manager."""
        pass
    
    @abstractmethod
    def create_procedure(self, graph_id, create_procedure_request):
        """Create a procedure on a graph.

        Args:
            graph_id (str): The id of the graph.
            create_procedure_request (CreateProcedureRequest): The request to create a procedure.

        Returns:
            CreateProcedureResponse: The response of the creation.
        """
        
    
    @abstractmethod
    def delete_procedure(self, graph_id, procedure_id):
        """Delete a procedure on a graph by id.

        Args:
            graph_id (str): The id of the graph.
            procedure_id (str): The id of the procedure.

        Returns:
            str: The result of the deletion.
        """
        pass
    
    @abstractmethod
    def get_procedure(self, graph_id, procedure_id):
        """Get a procedure by id.

        Args:
            graph_id (str): The id of the graph.
            procedure_id (str): The id of the procedure.

        Returns:
            GetProcedureResponse: The response of the procedure.
        """
        pass
    
    @abstractmethod
    def list_procedures(self, graph_id):
        """List all procedures on a graph.

        Args:
            graph_id (str): The id of the graph.

        Returns:
            List[GetProcedureResponse]: The response of all procedures.
        """
        pass
    
    @abstractmethod
    def update_procedure(self, graph_id, procedure_id, update_procedure_request):
        """Update a procedure on a graph.

        Args:
            graph_id (str): The id of the graph.
            procedure_id (str): The id of the procedure.
            update_procedure_request (UpdateProcedureRequest): The request to update a procedure.

        Returns:
            UpdateProcedureResponse: The response of the update.
        """
        pass
    
    
class DefaultProcedureManager(ProcedureManager):
    """The default implementation of procedure manager.

    Args:
        ProcedureManager (_type_): The interface of procedure manager.
    """
    def __init__(self, metadata_manager : IMetadataStore, config_file_path : str):
        self._metadata_manager = metadata_manager
        self._config_file_path = config_file_path
        self._procedure_builder = None
        self._builtin_proc_names = ["count_vertices", "pagerank", "k_neighbors", "shortest_path_among_three"]
        
    def open(self):
        pass
    
    def close(self):
        pass
    
    def create_procedure(self, graph_id : str, create_procedure_request : CreateProcedureRequest):
        if not graph_id or graph_id == "":
            raise RuntimeError("The graph id is None.")
        if not create_procedure_request:
            raise RuntimeError("The create procedure request is None.")
        if self._metadata_manager.get_graph_meta(graph_id) is None:
            raise RuntimeError(f"The graph {graph_id} does not exist.")
        if self._metadata_manager.get_plugin_meta(graph_id, create_procedure_request.name) is not None:
            raise RuntimeError(f"The procedure {create_procedure_request.name} already exists.")
        logger.info(f"Creating procedure {create_procedure_request.name} on graph {graph_id}")
        proc_name = create_procedure_request.name
        if proc_name in self._builtin_proc_names:
            raise RuntimeError(f"The procedure name {proc_name} is reserved, please use another name.")
        request_dict = create_procedure_request.to_dict()
        request_dict["id"] = request_dict["name"]
        request_dict["bound_graph"] = graph_id
        request_dict["creation_time"] = get_current_time_stamp_ms()
        request_dict["update_time"] = get_current_time_stamp_ms()
        request_dict["enable"] = True
        
        logger.info("Creating procedure with request: %s", request_dict)
        plugin_key = self._metadata_manager.create_plugin_meta(graph_id, str(request_dict))
        logger.info("Created plugin meta: %s", plugin_key)
        
        if not self._generate_procedure(graph_id, request_dict, plugin_key, self._config_file_path):
            logger.error(f"Failed to generate procedure {proc_name}")
            self._metadata_manager.delete_plugin_meta(graph_id, proc_name)
            raise RuntimeError(f"Failed to generate procedure {proc_name}")
        logger.info(f"Successfully created procedure {proc_name}")
        return plugin_key
        
    
    def delete_procedure(self, graph_id, procedure_id):
        """_summary_

        Args:
            graph_id (_type_): _description_
            procedure_id (_type_): _description_
        """
        if not graph_id or graph_id == "":
            raise RuntimeError("The graph id is None.")
        if not procedure_id or procedure_id == "":
            raise RuntimeError("The procedure id is None.")
        if self._metadata_manager.get_graph_meta(graph_id) is None:
            raise RuntimeError(f"The graph {graph_id} does not exist.")
        if self._metadata_manager.get_plugin_meta(graph_id, procedure_id) is None:
            raise RuntimeError(f"The procedure {procedure_id} does not exist.")
        logger.info(f"Deleting procedure {procedure_id} on graph {graph_id}")

        # delete the procedure from meta store
        if not self._metadata_manager.delete_plugin_meta(graph_id, procedure_id):
            raise RuntimeError(f"Failed to delete procedure {procedure_id}")
        
        # delete the procedure from remote storage
        if not self._delete_procedure_from_remote_storage(graph_id, procedure_id):
            raise RuntimeError(f"Failed to delete procedure {procedure_id} from remote storage")
        logger.info(f"Successfully deleted procedure {procedure_id}")
        return f"Successfully deleted procedure {procedure_id}"  
    
    def get_procedure(self, graph_id, procedure_id):
        if not graph_id or graph_id == "":
            raise RuntimeError("The graph id is None.")
        if not procedure_id or procedure_id == "":
            raise RuntimeError("The procedure id is None.")
        if self._metadata_manager.get_graph_meta(graph_id) is None:
            raise RuntimeError(f"The graph {graph_id} does not exist.")
        logger.info(f"Getting procedure {procedure_id} on graph {graph_id}")
        plugin_meta = self._metadata_manager.get_plugin_meta(graph_id, procedure_id)
        if plugin_meta is None:
            raise RuntimeError(f"Cannot find the procedure {procedure_id}")
        # parse the plugin_meta from str to dict
        plugin_meta = yaml.safe_load(plugin_meta)
        logger.info(f"Got plugin meta: {plugin_meta}")
        return GetProcedureResponse.from_dict(plugin_meta)
    
    def list_procedures(self, graph_id):
        if not graph_id or graph_id == "":
            raise RuntimeError("The graph id is None.")
        if self._metadata_manager.get_graph_meta(graph_id) is None:
            raise RuntimeError(f"The graph {graph_id} does not exist.")
        logger.info(f"Listing procedures on graph {graph_id}")
        plugin_metas = self._metadata_manager.get_all_plugin_meta(graph_id)
        if plugin_metas is None:
            raise RuntimeError(f"Cannot find the procedures on graph {graph_id}")
        results = []
        for plugin_meta in plugin_metas:
            results.append(GetProcedureResponse.from_dict(yaml.safe_load(plugin_meta)))
        return
    
    def update_procedure(self, graph_id, procedure_id, update_procedure_request):
        if not graph_id or graph_id == "":
            raise RuntimeError("The graph id is None.")
        if not procedure_id or procedure_id == "":
            raise RuntimeError("The procedure id is None.")
        if not update_procedure_request:
            raise RuntimeError("The update procedure request is None.")
        if self._metadata_manager.get_graph_meta(graph_id) is None:
            raise RuntimeError(f"The graph {graph_id} does not exist.")
        if self._metadata_manager.get_plugin_meta(graph_id, procedure_id) is None:
            raise RuntimeError(f"The procedure {procedure_id} does not exist.")
        logger.info(f"Updating procedure {procedure_id} on graph {graph_id}")
        new_description = update_procedure_request.description
        old_plugin_meta = self._metadata_manager.get_plugin_meta(graph_id, procedure_id)
        if old_plugin_meta is None:
            raise RuntimeError(f"Cannot find the procedure {procedure_id}")
        old_plugin_meta = yaml.safe_load(old_plugin_meta)
        logger.info(f"Got old plugin meta: {old_plugin_meta}")
        old_plugin_meta["description"] = new_description
        self._metadata_manager.update_plugin_meta(graph_id, procedure_id, str(old_plugin_meta))
        logger.info(f"Updated plugin meta: {old_plugin_meta}")
        return f"Successfully updated procedure {procedure_id}"
        
    
    def _generate_procedure(self, graph_id, request_dict, plugin_key, config_file_path):
        # Check whether the request is valid
        self._check_request(request_dict)
        builder_path = self._get_procedure_builder_path()
        logger.info(f"Generating procedure with builder path: {builder_path}")
        query_str = request_dict["query"]
        if query_str is None or query_str == "":
            raise RuntimeError("The query is None or empty.")
        if "description" not in request_dict:
            request_dict["description"] = "A procedure generated by FLEX"
        query_file_path = self._dump_query_to_file(query_str, request_dict["type"], plugin_key)
        desc_file_path = f"{CODE_GEN_TMP_DIR}/{plugin_key}.desc"
        output_dir=f"{CODE_GEN_TMP_DIR}/{plugin_key}"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        dump_file(request_dict['description'], desc_file_path)
        command = [
            CODE_GEN_BIN,
            "-e=hqps",
            f"-i={query_file_path}",
            f"-o={output_dir}",
            f"--procedure_name={request_dict['name']}",
            f"-w={CODE_GEN_TMP_DIR}",
            f"--ir_conf={config_file_path}",
            f"--graph_schema_path={self._dump_schema_to_file(graph_id, plugin_key)}",
        ]
        if "description" in request_dict and request_dict["description"] is not None:
            command.append(f"--procedure_desc=={desc_file_path}")
            
        logger.info(f"Generating procedure with command: {command}")
        log_file = f"{CODE_GEN_TMP_DIR}/{plugin_key}.log"
        with open(log_file, "w") as log:
            process = subprocess.Popen(command, stdout = log, stderr = log)
            logger.info(f"Started process {process.pid}")
            process.wait()
            logger.info(f"Finished process {process.pid} with code {process.returncode}")
            if process.returncode != 0:
                logger.error(f"Failed to generate procedure {request_dict['name']}")
                return False
        return True
            
        
        
        
    def _dump_schema_to_file(self, graph_id, plugin_key):
        schema_file = f"{CODE_GEN_TMP_DIR}/{plugin_key}.schema"
        schema = self._metadata_manager.get_graph_schema(graph_id)
        if schema is None:
            raise RuntimeError(f"Cannot find the schema of graph {graph_id}")
        return dump_file(schema, schema_file)
        
    def _dump_query_to_file(self, query_str, query_type, plugin_key):
        query_file = f"{CODE_GEN_TMP_DIR}/{plugin_key}"
        if query_type.lower() == "cypher":
            query_file += ".cypher"
        elif query_type.lower() == "cpp" or query_type.lower() == "cc":
            query_file += ".cc"
        else:
            raise RuntimeError(f"Invalid query type: {query_type}")
        return dump_file(query_str, query_file)
    
    def _get_procedure_builder_path(self):
        """Get the path to CODE_GEN_BIN."""
        # Try from FLEX_HOME
        if os.environ.get("FLEX_HOME"):
            bin_path =  os.path.join(os.environ.get("FLEX_HOME"), "bin", CODE_GEN_BIN)
            if os.path.exists(bin_path):
                return bin_path
        # Try from /opt/flex/bin
        bin_path = os.path.join("/opt/flex/bin", CODE_GEN_BIN)
        if os.path.exists(bin_path):
            return bin_path
        # Try to find via the relative path
        bin_path = os.path.join(os.path.dirname(__file__), "../../../../../../bin", CODE_GEN_BIN)
        if os.path.exists(bin_path):
            return bin_path
        raise RuntimeError(f"Cannot find the code gen bin: {CODE_GEN_BIN}")
    
    def _check_request(self, request_dict):
        """check create procedure request.

        Args:
            request_dict (_type_): _description_

        Returns:
            _type_: _description_
        """
        check_field_in_dict(request_dict, "bound_graph")
        check_field_in_dict(request_dict, "name")
        check_field_in_dict(request_dict, "description")
        check_field_in_dict(request_dict, "enable")
        check_field_in_dict(request_dict, "query")
        check_field_in_dict(request_dict, "type")
        _type = request_dict["type"]
        if _type not in ["cypher", "CYPHER", "CPP", "cpp"]:
            raise RuntimeError(f"Invalid procedure type: {_type}")    

        
        
    
procedure_manager = None

def get_procedure_manager() -> ProcedureManager:
    global procedure_manager
    return procedure_manager
    
def init_procedure_manager(config: Config, config_file_path: str):
    global procedure_manager
    procedure_manager = DefaultProcedureManager(get_metadata_store(), config_file_path)