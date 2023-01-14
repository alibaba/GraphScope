import datetime
import json
import logging
import os
import pickle
import random
import time
import zipfile
from concurrent import futures
from io import BytesIO

import grpc
from graphscope.framework import utils
from graphscope.framework.dag_utils import create_graph
from graphscope.framework.dag_utils import create_loader
from graphscope.framework.errors import AnalyticalEngineInternalError
from graphscope.framework.graph_utils import normalize_parameter_edges
from graphscope.framework.graph_utils import normalize_parameter_vertices
from graphscope.framework.loader import Loader
from graphscope.framework.utils import find_java
from graphscope.framework.utils import get_tempdir
from graphscope.framework.utils import normalize_data_type_str
from graphscope.proto import attr_value_pb2
from graphscope.proto import engine_service_pb2_grpc
from graphscope.proto import graph_def_pb2
from graphscope.proto import message_pb2
from graphscope.proto import op_def_pb2
from graphscope.proto import types_pb2
from graphscope.proto.error_codes_pb2 import OK

from gscoordinator.monitor import Monitor
from gscoordinator.object_manager import GraphMeta
from gscoordinator.object_manager import GremlinResultSet
from gscoordinator.object_manager import LibMeta
from gscoordinator.utils import ANALYTICAL_BUILTIN_SPACE
from gscoordinator.utils import ANALYTICAL_ENGINE_JAVA_INIT_CLASS_PATH
from gscoordinator.utils import ANALYTICAL_ENGINE_JAVA_JVM_OPTS
from gscoordinator.utils import GS_GRPC_MAX_MESSAGE_LENGTH
from gscoordinator.utils import INTERACTIVE_ENGINE_THREADS_PER_WORKER
from gscoordinator.utils import RESOURCE_DIR_NAME
from gscoordinator.utils import WORKSPACE
from gscoordinator.utils import compile_app
from gscoordinator.utils import compile_graph_frame
from gscoordinator.utils import create_single_op_dag
from gscoordinator.utils import dump_string
from gscoordinator.utils import get_app_sha256
from gscoordinator.utils import get_graph_sha256
from gscoordinator.utils import get_lib_path
from gscoordinator.utils import op_pre_process
from gscoordinator.utils import to_interactive_engine_schema

logger = logging.getLogger("graphscope")


class OperationExecutor:
    def __init__(self, session_id: str, launcher, object_manager):
        self._session_id = session_id
        self._launcher = launcher

        self._object_manager = object_manager

        self._key_to_op = {}
        # dict of op_def_pb2.OpResult
        self._op_result_pool = {}

        # Analytical engine attributes
        # ============================
        self._analytical_grpc_stub = None
        # java class path should contain
        #   1) java runtime path
        #   2) uploaded resources, the recent uploaded resource will be placed first.
        self._java_class_path = ANALYTICAL_ENGINE_JAVA_INIT_CLASS_PATH
        self._jvm_opts = ANALYTICAL_ENGINE_JAVA_JVM_OPTS
        # runtime workspace, consisting of some libraries, logs, etc.
        self._builtin_workspace = os.path.join(WORKSPACE, "builtin")
        # udf app workspace and resource directory should be bound to a specific session when client connect.
        self._udf_app_workspace = os.path.join(
            WORKSPACE, launcher.instance_id, session_id
        )
        self._resource_dir = os.path.join(
            WORKSPACE, launcher.instance_id, session_id, RESOURCE_DIR_NAME
        )

    def run_step(self, dag_def, dag_bodies):
        def _generate_runstep_request(session_id, dag_def, dag_bodies):
            runstep_requests = [
                message_pb2.RunStepRequest(
                    head=message_pb2.RunStepRequestHead(
                        session_id=session_id, dag_def=dag_def
                    )
                )
            ]
            # head
            runstep_requests.extend(dag_bodies)
            for item in runstep_requests:
                yield item

        requests = _generate_runstep_request(self._session_id, dag_def, dag_bodies)
        # response
        response_head, response_bodies = None, []
        try:
            responses = self.analytical_grpc_stub.RunStep(requests)
            for response in responses:
                if response.HasField("head"):
                    response_head = response
                else:
                    response_bodies.append(response)
            return response_head, response_bodies
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.INTERNAL:
                # TODO: make the stacktrace separated from normal error messages
                # Too verbose.
                if len(e.details()) > 3072:  # 3k bytes
                    msg = f"{e.details()[:30]} ... [truncated]"
                else:
                    msg = e.details()
                raise AnalyticalEngineInternalError(msg)
            else:
                raise

    def pre_process(self, dag_def, dag_bodies, loader_op_bodies):
        for op in dag_def.op:
            self._key_to_op[op.key] = op
            op_pre_process(
                op,
                self._op_result_pool,
                self._key_to_op,
                engine_hosts=self._launcher.hosts,
                engine_java_class_path=self._java_class_path,  # may be needed in CREATE_GRAPH or RUN_APP
                engine_jvm_opts=self._jvm_opts,
            )

            # Handle op that depends on loader (data source)
            if op.op == types_pb2.CREATE_GRAPH or op.op == types_pb2.ADD_LABELS:
                for key_of_parent_op in op.parents:
                    parent_op = self._key_to_op[key_of_parent_op]
                    if parent_op.op == types_pb2.DATA_SOURCE:
                        # handle bodies of loader op
                        if parent_op.key in loader_op_bodies:
                            dag_bodies.extend(loader_op_bodies[parent_op.key])

            # Compile app or not.
            if op.op == types_pb2.BIND_APP:
                op, _, _ = self._maybe_compile_app(op)

            # Compile graph or not
            # arrow property graph and project graph need to compile
            # If engine crashed, we will get a SocketClosed grpc Exception.
            # In that case, we should notify client the engine is dead.
            if (
                (
                    op.op == types_pb2.CREATE_GRAPH
                    and op.attr[types_pb2.GRAPH_TYPE].i == graph_def_pb2.ARROW_PROPERTY
                )
                or op.op == types_pb2.TRANSFORM_GRAPH
                or op.op == types_pb2.PROJECT_TO_SIMPLE
                or op.op == types_pb2.ADD_LABELS
            ):
                op = self._maybe_register_graph(op)
        return dag_def, dag_bodies

    @Monitor.runOnAnalyticalEngine
    def run_on_analytical_engine(
        self, dag_def, dag_bodies, loader_op_bodies
    ):  # noqa: C901
        # preprocess of op before run on analytical engine
        dag_def, dag_bodies = self.pre_process(dag_def, dag_bodies, loader_op_bodies)
        # generate runstep requests, and run on analytical engine
        response_head, response_bodies = self.run_step(dag_def, dag_bodies)
        response_head, response_bodies = self.post_process(
            response_head, response_bodies
        )
        return response_head, response_bodies

    def post_process(self, response_head, response_bodies):
        # handle result from response stream
        if response_head is None:
            raise AnalyticalEngineInternalError(
                "Missing head from the response stream."
            )
        for op_result in response_head.head.results:
            # record result in coordinator, which doesn't contain large data
            self._op_result_pool[op_result.key] = op_result
            # get the op corresponding to the result
            op = self._key_to_op[op_result.key]
            # register graph and dump graph schema
            if op.op in (
                types_pb2.CREATE_GRAPH,
                types_pb2.PROJECT_GRAPH,
                types_pb2.PROJECT_TO_SIMPLE,
                types_pb2.TRANSFORM_GRAPH,
                types_pb2.ADD_LABELS,
                types_pb2.ADD_COLUMN,
            ):
                schema_path = os.path.join(
                    get_tempdir(), op_result.graph_def.key + ".json"
                )
                vy_info = graph_def_pb2.VineyardInfoPb()
                op_result.graph_def.extension.Unpack(vy_info)
                self._object_manager.put(
                    op_result.graph_def.key,
                    GraphMeta(
                        op_result.graph_def.key,
                        vy_info.vineyard_id,
                        op_result.graph_def,
                        schema_path,
                    ),
                )
                if op_result.graph_def.graph_type == graph_def_pb2.ARROW_PROPERTY:
                    dump_string(
                        to_interactive_engine_schema(vy_info.property_schema_json),
                        schema_path,
                    )
                    vy_info.schema_path = schema_path
                    op_result.graph_def.extension.Pack(vy_info)
            # register app
            elif op.op == types_pb2.BIND_APP:
                _, app_sig, app_lib_path = self._maybe_compile_app(op)
                self._object_manager.put(
                    app_sig,
                    LibMeta(
                        op_result.result.decode("utf-8", errors="ignore"),
                        "app",
                        app_lib_path,
                    ),
                )
            # unregister graph
            elif op.op == types_pb2.UNLOAD_GRAPH:
                self._object_manager.pop(op.attr[types_pb2.GRAPH_NAME].s.decode())
            # unregister app
            elif op.op == types_pb2.UNLOAD_APP:
                self._object_manager.pop(op.attr[types_pb2.APP_NAME].s.decode())
        return response_head, response_bodies

    # Analytical engine related operations
    # ====================================
    def _maybe_compile_app(self, op):
        app_sig = get_app_sha256(op.attr, self._java_class_path)
        # try to get compiled file from GRAPHSCOPE_HOME/precompiled
        app_lib_path = get_lib_path(
            os.path.join(ANALYTICAL_BUILTIN_SPACE, app_sig), app_sig
        )
        if not os.path.isfile(app_lib_path):
            algo_name = op.attr[types_pb2.APP_ALGO].s.decode("utf-8", errors="ignore")
            if (
                types_pb2.GAR in op.attr
                or algo_name.startswith("giraph:")
                or algo_name.startswith("java_pie:")
            ):
                space = self._udf_app_workspace
            else:
                space = self._builtin_workspace
            # try to get compiled file from workspace
            app_lib_path = get_lib_path(os.path.join(space, app_sig), app_sig)
            if not os.path.isfile(app_lib_path):
                # compile and distribute
                compiled_path = self._compile_lib_and_distribute(
                    compile_app,
                    app_sig,
                    op,
                    self._java_class_path,
                )
                if app_lib_path != compiled_path:
                    msg = f"Computed app library path != compiled path, {app_lib_path} versus {compiled_path}"
                    raise RuntimeError(msg)
        op.attr[types_pb2.APP_LIBRARY_PATH].CopyFrom(
            attr_value_pb2.AttrValue(s=app_lib_path.encode("utf-8", errors="ignore"))
        )
        return op, app_sig, app_lib_path

    def _maybe_register_graph(self, op):
        graph_sig = get_graph_sha256(op.attr)
        # try to get compiled file from GRAPHSCOPE_HOME/precompiled/builtin
        graph_lib_path = get_lib_path(
            os.path.join(ANALYTICAL_BUILTIN_SPACE, graph_sig), graph_sig
        )
        if not os.path.isfile(graph_lib_path):
            space = self._builtin_workspace
            # try to get compiled file from workspace
            graph_lib_path = get_lib_path(os.path.join(space, graph_sig), graph_sig)
            if not os.path.isfile(graph_lib_path):
                # compile and distribute
                compiled_path = self._compile_lib_and_distribute(
                    compile_graph_frame,
                    graph_sig,
                    op,
                )
                if graph_lib_path != compiled_path:
                    raise RuntimeError(
                        f"Computed graph library path not equal to compiled path, {graph_lib_path} versus {compiled_path}"
                    )
        if graph_sig not in self._object_manager:
            dag_def = create_single_op_dag(
                types_pb2.REGISTER_GRAPH_TYPE,
                config={
                    types_pb2.GRAPH_LIBRARY_PATH: attr_value_pb2.AttrValue(
                        s=graph_lib_path.encode("utf-8", errors="ignore")
                    ),
                    types_pb2.TYPE_SIGNATURE: attr_value_pb2.AttrValue(
                        s=graph_sig.encode("utf-8", errors="ignore")
                    ),
                    types_pb2.GRAPH_TYPE: attr_value_pb2.AttrValue(
                        i=op.attr[types_pb2.GRAPH_TYPE].i
                    ),
                },
            )
            try:
                response_head, _ = self.run_on_analytical_engine(dag_def, [], {})
            except grpc.RpcError as e:
                logger.error(
                    "Register graph failed, code: %s, details: %s",
                    e.code().name,
                    e.details(),
                )
                if e.code() == grpc.StatusCode.INTERNAL:
                    raise AnalyticalEngineInternalError(e.details())
                else:
                    raise
            self._object_manager.put(
                graph_sig,
                LibMeta(
                    response_head.head.results[0].result,
                    "graph_frame",
                    graph_lib_path,
                ),
            )
        op.attr[types_pb2.TYPE_SIGNATURE].CopyFrom(
            attr_value_pb2.AttrValue(s=graph_sig.encode("utf-8", errors="ignore"))
        )
        return op

    def _create_analytical_grpc_stub(self):
        options = [
            ("grpc.max_send_message_length", GS_GRPC_MAX_MESSAGE_LENGTH),
            ("grpc.max_receive_message_length", GS_GRPC_MAX_MESSAGE_LENGTH),
            ("grpc.max_metadata_size", GS_GRPC_MAX_MESSAGE_LENGTH),
        ]
        # Check connectivity, otherwise the stub is useless
        retry = 0
        while retry < 20:
            try:
                channel = grpc.insecure_channel(
                    self._launcher.analytical_engine_endpoint, options=options
                )
                stub = engine_service_pb2_grpc.EngineServiceStub(channel)
                stub.HeartBeat(message_pb2.HeartBeatRequest())
                return stub
            except grpc.RpcError as e:
                logger.warning(
                    "Connecting to analytical engine... retrying %d time", retry
                )
                logger.warning("Error code: %s, details %s", e.code(), e.details())
                retry += 1
                time.sleep(3)
        raise RuntimeError(
            "Failed to connect to engine in 60s, deployment may failed. Please check coordinator log for details"
        )

    @property
    def analytical_grpc_stub(self):
        if self._launcher.analytical_engine_endpoint is None:
            raise RuntimeError("Analytical engine endpoint not set.")
        if self._analytical_grpc_stub is None:
            self._analytical_grpc_stub = self._create_analytical_grpc_stub()
        return self._analytical_grpc_stub

    def get_analytical_engine_config(self) -> {}:
        dag_def = create_single_op_dag(types_pb2.GET_ENGINE_CONFIG)
        response_head, _ = self.run_on_analytical_engine(dag_def, [], {})
        config = json.loads(
            response_head.head.results[0].result.decode("utf-8", errors="ignore")
        )
        config["engine_hosts"] = self._launcher.hosts
        # Disable ENABLE_JAVA_SDK when java is not installed on coordinator
        if config["enable_java_sdk"] == "ON":
            try:
                find_java()
            except RuntimeError:
                logger.warning(
                    "Disable java sdk support since java is not installed on coordinator"
                )
                config["enable_java_sdk"] = "OFF"
        return config

    def _compile_lib_and_distribute(self, compile_func, lib_name, op, *args, **kwargs):
        algo_name = op.attr[types_pb2.APP_ALGO].s.decode("utf-8", errors="ignore")
        if (
            types_pb2.GAR in op.attr
            or algo_name.startswith("giraph:")
            or algo_name.startswith("java_pie:")
        ):
            space = self._udf_app_workspace
        else:
            space = self._builtin_workspace
        lib_path, java_jar_path, java_ffi_path, app_type = compile_func(
            space,
            lib_name,
            op.attr,
            self.get_analytical_engine_config(),
            self._launcher,
            *args,
            **kwargs,
        )
        # for java app compilation, we need to distribute the jar and ffi generated
        if app_type == "java_pie":
            self._launcher.distribute_file(java_jar_path)
            self._launcher.distribute_file(java_ffi_path)
        self._launcher.distribute_file(lib_path)
        return lib_path

    def heart_beat(self, request):
        return self.analytical_grpc_stub.HeartBeat(request)

    def add_lib(self, request):
        os.makedirs(self._resource_dir, exist_ok=True)
        fp = BytesIO(request.gar)
        with zipfile.ZipFile(fp, "r") as zip_ref:
            zip_ref.extractall(self._resource_dir)
            logger.info(
                "Coordinator received add lib request with file: %s", zip_ref.namelist()
            )
            if len(zip_ref.namelist()) != 1:
                raise RuntimeError("Expect only one resource in one gar")
            filename = zip_ref.namelist()[0]
        filename = os.path.join(self._resource_dir, filename)
        self._launcher.distribute_file(filename)
        logger.info("Successfully distributed %s", filename)
        if filename.endswith(".jar"):
            logger.info("adding lib to java class path since it ends with .jar")
            self._java_class_path = filename + ":" + self._java_class_path
            logger.info("current java class path: %s", self._java_class_path)

    # Interactive engine related operations
    # =====================================
    @Monitor.runOnInteractiveEngine
    def run_on_interactive_engine(self, dag_def: op_def_pb2.DagDef):
        response_head = message_pb2.RunStepResponseHead()
        for op in dag_def.op:
            self._key_to_op[op.key] = op
            op_pre_process(op, self._op_result_pool, self._key_to_op)
            if op.op == types_pb2.GREMLIN_QUERY:
                op_result = self._execute_gremlin_query(op)
            elif op.op == types_pb2.FETCH_GREMLIN_RESULT:
                op_result = self._fetch_gremlin_result(op)
            elif op.op == types_pb2.SUBGRAPH:
                op_result = self._gremlin_to_subgraph(op)
            else:
                raise RuntimeError("Unsupported op type: " + str(op.op))
            response_head.results.append(op_result)
            # record op result
            self._op_result_pool[op.key] = op_result
        return message_pb2.RunStepResponse(head=response_head), []

    def _execute_gremlin_query(self, op: op_def_pb2.OpDef):
        logger.info("execute gremlin query")
        message = op.attr[types_pb2.GIE_GREMLIN_QUERY_MESSAGE].s.decode()
        request_options = None
        if types_pb2.GIE_GREMLIN_REQUEST_OPTIONS in op.attr:
            request_options = json.loads(
                op.attr[types_pb2.GIE_GREMLIN_REQUEST_OPTIONS].s.decode()
            )
        object_id = op.attr[types_pb2.VINEYARD_ID].i
        gremlin_client = self._object_manager.get(object_id)
        rlt = gremlin_client.submit(message, request_options=request_options)
        logger.info("put %s, client %s", op.key, gremlin_client)
        self._object_manager.put(op.key, GremlinResultSet(op.key, rlt))
        return op_def_pb2.OpResult(code=OK, key=op.key)

    def _fetch_gremlin_result(self, op: op_def_pb2.OpDef):
        fetch_result_type = op.attr[types_pb2.GIE_GREMLIN_FETCH_RESULT_TYPE].s.decode()
        key_of_parent_op = op.parents[0]
        result_set = self._object_manager.get(key_of_parent_op).result_set
        if fetch_result_type == "one":
            rlt = result_set.one()
        elif fetch_result_type == "all":
            rlt = result_set.all().result()
        else:
            raise RuntimeError("Not supported fetch result type: " + fetch_result_type)
        # Large data should be fetched use gremlin pagination
        # meta = op_def_pb2.OpResult.Meta(has_large_result=True)
        return op_def_pb2.OpResult(
            code=OK,
            key=op.key,
            result=pickle.dumps(rlt),
        )

    def _gremlin_to_subgraph(self, op: op_def_pb2.OpDef):
        gremlin_script = op.attr[types_pb2.GIE_GREMLIN_QUERY_MESSAGE].s.decode()
        oid_type = op.attr[types_pb2.OID_TYPE].s.decode()
        request_options = None
        if types_pb2.GIE_GREMLIN_REQUEST_OPTIONS in op.attr:
            request_options = json.loads(
                op.attr[types_pb2.GIE_GREMLIN_REQUEST_OPTIONS].s.decode()
            )
        object_id = op.attr[types_pb2.VINEYARD_ID].i
        gremlin_client = self._object_manager.get(object_id)

        def create_global_graph_builder(
            graph_name, num_workers, threads_per_executor, vineyard_rpc_endpoint
        ):
            import vineyard

            vineyard_client = vineyard.connect(*vineyard_rpc_endpoint.split(":"))

            instances = [key for key in vineyard_client.meta]

            # duplicate each instances for each thread per worker.
            chunk_instances = [
                key for key in instances for _ in range(threads_per_executor)
            ]

            # build the vineyard::GlobalPGStream
            metadata = vineyard.ObjectMeta()
            metadata.set_global(True)
            metadata["typename"] = "vineyard::htap::GlobalPGStream"
            metadata["local_stream_chunks"] = threads_per_executor
            metadata["total_stream_chunks"] = len(chunk_instances)

            # build the parallel stream for edge
            edge_metadata = vineyard.ObjectMeta()
            edge_metadata.set_global(True)
            edge_metadata["typename"] = "vineyard::ParallelStream"
            edge_metadata["__streams_-size"] = len(chunk_instances)

            # build the parallel stream for vertex
            vertex_metadata = vineyard.ObjectMeta()
            vertex_metadata.set_global(True)
            vertex_metadata["typename"] = "vineyard::ParallelStream"
            vertex_metadata["__streams_-size"] = len(chunk_instances)

            # NB: we don't respect `num_workers`, instead, we create a substream
            # on each vineyard instance.
            #
            # Such a choice is to handle cases where that etcd instance still contains
            # information about dead instances.
            #
            # It should be ok, as each engine work will get its own local stream. But,
            # generally it should be equal to `num_workers`.
            for worker, instance_id in enumerate(chunk_instances):
                edge_stream = vineyard.ObjectMeta()
                edge_stream["typename"] = "vineyard::RecordBatchStream"
                edge_stream["nbytes"] = 0
                edge_stream["params_"] = json.dumps(
                    {
                        "graph_name": graph_name,
                        "kind": "edge",
                    }
                )
                edge = vineyard_client.create_metadata(edge_stream, instance_id)
                vineyard_client.persist(edge.id)
                edge_metadata.add_member("__streams_-%d" % worker, edge)

                vertex_stream = vineyard.ObjectMeta()
                vertex_stream["typename"] = "vineyard::RecordBatchStream"
                vertex_stream["nbytes"] = 0
                vertex_stream["params_"] = json.dumps(
                    {
                        "graph_name": graph_name,
                        "kind": "vertex",
                    }
                )
                vertex = vineyard_client.create_metadata(vertex_stream, instance_id)
                vineyard_client.persist(vertex.id)
                vertex_metadata.add_member("__streams_-%d" % worker, vertex)

                chunk_stream = vineyard.ObjectMeta()
                chunk_stream["typename"] = "vineyard::htap::PropertyGraphOutStream"
                chunk_stream["graph_name"] = graph_name
                chunk_stream["graph_schema"] = "{}"
                chunk_stream["nbytes"] = 0
                chunk_stream["stream_index"] = worker
                chunk_stream.add_member("edge_stream", edge)
                chunk_stream.add_member("vertex_stream", vertex)
                chunk = vineyard_client.create_metadata(chunk_stream, instance_id)
                vineyard_client.persist(chunk.id)
                metadata.add_member("stream_chunk_%d" % worker, chunk)

            # build the vineyard::GlobalPGStream
            graph = vineyard_client.create_metadata(metadata)
            vineyard_client.persist(graph.id)
            vineyard_client.put_name(graph.id, graph_name)

            # build the parallel stream for edge
            edge = vineyard_client.create_metadata(edge_metadata)
            vineyard_client.persist(edge.id)
            vineyard_client.put_name(edge.id, "__%s_edge_stream" % graph_name)

            # build the parallel stream for vertex
            vertex = vineyard_client.create_metadata(vertex_metadata)
            vineyard_client.persist(vertex.id)
            vineyard_client.put_name(vertex.id, "__%s_vertex_stream" % graph_name)

            return repr(graph.id), repr(edge.id), repr(vertex.id)

        def load_subgraph(
            graph_name,
            total_builder_chunks,
            oid_type,
            edge_stream_id,
            vertex_stream_id,
            vineyard_rpc_endpoint,
        ):
            import vineyard

            # wait all flags been created, see also
            #
            # `PropertyGraphOutStream::Initialize(Schema schema)`
            vineyard_client = vineyard.connect(*vineyard_rpc_endpoint.split(":"))

            # wait for all stream been created by GAIA executor in FFI
            for worker in range(total_builder_chunks):
                name = "__%s_%d_streamed" % (graph_name, worker)
                vineyard_client.get_name(name, wait=True)

            vertices = [Loader(vineyard.ObjectID(vertex_stream_id))]
            edges = [Loader(vineyard.ObjectID(edge_stream_id))]
            oid_type = normalize_data_type_str(oid_type)
            v_labels = normalize_parameter_vertices(vertices, oid_type)
            e_labels = normalize_parameter_edges(edges, oid_type)
            loader_op = create_loader(v_labels + e_labels)
            config = {
                types_pb2.DIRECTED: utils.b_to_attr(True),
                types_pb2.OID_TYPE: utils.s_to_attr(oid_type),
                types_pb2.GENERATE_EID: utils.b_to_attr(False),
                # otherwise the new graph cannot be used for GIE
                types_pb2.RETAIN_OID: utils.b_to_attr(True),
                types_pb2.VID_TYPE: utils.s_to_attr("uint64_t"),
                types_pb2.IS_FROM_VINEYARD_ID: utils.b_to_attr(False),
            }
            new_op = create_graph(
                self._session_id,
                graph_def_pb2.ARROW_PROPERTY,
                inputs=[loader_op],
                attrs=config,
            )
            # spawn a vineyard stream loader on coordinator
            loader_op_def = loader_op.as_op_def()
            coordinator_dag = op_def_pb2.DagDef()
            coordinator_dag.op.extend([loader_op_def])
            # set the same key from subgraph to new op
            new_op_def = new_op.as_op_def()
            new_op_def.key = op.key
            dag = op_def_pb2.DagDef()
            dag.op.extend([new_op_def])
            self.run_on_coordinator(coordinator_dag, [], {})
            response_head, _ = self.run_on_analytical_engine(dag, [], {})
            logger.info("subgraph has been loaded")
            return response_head.head.results[-1]

        # generate a random graph name
        now_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        random_num = random.randint(0, 10000000)
        graph_name = "subgraph-%s-%s" % (str(now_time), str(random_num))

        threads_per_worker = int(
            os.environ.get("THREADS_PER_WORKER", INTERACTIVE_ENGINE_THREADS_PER_WORKER)
        )

        if self._launcher.type() == types_pb2.HOSTS:
            # only 1 GIE executor on local cluster
            executor_workers_num = 1
            threads_per_executor = self._launcher.num_workers * threads_per_worker
            engine_config = self.get_analytical_engine_config()
            vineyard_rpc_endpoint = engine_config["vineyard_rpc_endpoint"]
        else:
            executor_workers_num = self._launcher.num_workers
            threads_per_executor = threads_per_worker
            vineyard_rpc_endpoint = self._launcher.vineyard_internal_endpoint
        total_builder_chunks = executor_workers_num * threads_per_executor

        (
            _graph_builder_id,
            edge_stream_id,
            vertex_stream_id,
        ) = create_global_graph_builder(
            graph_name,
            executor_workers_num,
            threads_per_executor,
            vineyard_rpc_endpoint,
        )
        # start a thread to launch the graph
        pool = futures.ThreadPoolExecutor()
        subgraph_task = pool.submit(
            load_subgraph,
            graph_name,
            total_builder_chunks,
            oid_type,
            edge_stream_id,
            vertex_stream_id,
            vineyard_rpc_endpoint,
        )

        # add subgraph vertices and edges
        subgraph_script = "{0}.subgraph('{1}')".format(
            gremlin_script,
            graph_name,
        )
        gremlin_client.submit(
            subgraph_script, request_options=request_options
        ).all().result()

        return subgraph_task.result()

    # Learning engine related operations
    # ==================================
    def run_on_learning_engine(self, dag_def: op_def_pb2.DagDef):
        raise NotImplementedError("Learning engine is not implemented yet")

    # Coordinator related operations
    # ==============================
    def run_on_coordinator(self, dag_def, dag_bodies, loader_op_bodies):
        response_head = message_pb2.RunStepResponseHead()
        for op in dag_def.op:
            self._key_to_op[op.key] = op
            op_pre_process(op, self._op_result_pool, self._key_to_op)
            if op.op == types_pb2.DATA_SOURCE:
                op_result = self._process_data_source(op, dag_bodies, loader_op_bodies)
            elif op.op == types_pb2.DATA_SINK:
                op_result = self._process_data_sink(op)
            else:
                raise RuntimeError("Unsupported op type: " + str(op.op))
            response_head.results.append(op_result)
            self._op_result_pool[op.key] = op_result
        return message_pb2.RunStepResponse(head=response_head), []

    def _process_data_sink(self, op: op_def_pb2.OpDef):
        import vineyard
        import vineyard.io

        storage_options = json.loads(op.attr[types_pb2.STORAGE_OPTIONS].s.decode())
        write_options = json.loads(op.attr[types_pb2.WRITE_OPTIONS].s.decode())
        fd = op.attr[types_pb2.FD].s.decode()
        df = op.attr[types_pb2.VINEYARD_ID].s.decode()
        engine_config = self.get_analytical_engine_config()
        if self._launcher.type() == types_pb2.HOSTS:
            vineyard_endpoint = engine_config["vineyard_rpc_endpoint"]
        else:
            vineyard_endpoint = self._launcher.vineyard_internal_endpoint
        vineyard_ipc_socket = engine_config["vineyard_socket"]
        deployment, hosts = self._launcher.get_vineyard_stream_info()
        dfstream = vineyard.io.open(
            "vineyard://" + str(df),
            mode="r",
            vineyard_ipc_socket=vineyard_ipc_socket,
            vineyard_endpoint=vineyard_endpoint,
            deployment=deployment,
            hosts=hosts,
        )
        vineyard.io.open(
            fd,
            dfstream,
            mode="w",
            vineyard_ipc_socket=vineyard_ipc_socket,
            vineyard_endpoint=vineyard_endpoint,
            storage_options=storage_options,
            write_options=write_options,
            deployment=deployment,
            hosts=hosts,
        )
        return op_def_pb2.OpResult(code=OK, key=op.key)

    def _process_data_source(
        self, op: op_def_pb2.OpDef, dag_bodies, loader_op_bodies: dict
    ):
        def _spawn_vineyard_io_stream(
            source,
            storage_options,
            read_options,
            vineyard_endpoint,
            vineyard_ipc_socket,
        ):
            import vineyard
            import vineyard.io

            deployment, hosts = self._launcher.get_vineyard_stream_info()
            num_workers = self._launcher.num_workers
            stream_id = repr(
                vineyard.io.open(
                    source,
                    mode="r",
                    vineyard_endpoint=vineyard_endpoint,
                    vineyard_ipc_socket=vineyard_ipc_socket,
                    hosts=hosts,
                    num_workers=num_workers,
                    deployment=deployment,
                    read_options=read_options,
                    storage_options=storage_options,
                )
            )
            return "vineyard", stream_id

        def _process_loader_func(loader, vineyard_endpoint, vineyard_ipc_socket):
            # loader is type of attr_value_pb2.Chunk
            protocol = loader.attr[types_pb2.PROTOCOL].s.decode()
            source = loader.attr[types_pb2.SOURCE].s.decode()
            try:
                storage_options = json.loads(
                    loader.attr[types_pb2.STORAGE_OPTIONS].s.decode()
                )
                read_options = json.loads(
                    loader.attr[types_pb2.READ_OPTIONS].s.decode()
                )
            except:  # noqa: E722, pylint: disable=bare-except
                storage_options = {}
                read_options = {}
            filetype = storage_options.get("filetype", None)
            if filetype is None:
                filetype = read_options.get("filetype", None)
            filetype = str(filetype).upper()
            if (
                protocol in ("hdfs", "hive", "oss", "s3")
                or protocol == "file"
                and (
                    source.endswith(".orc")
                    or source.endswith(".parquet")
                    or source.endswith(".pq")
                )
                or filetype in ["ORC", "PARQUET"]
            ):
                new_protocol, new_source = _spawn_vineyard_io_stream(
                    source,
                    storage_options,
                    read_options,
                    vineyard_endpoint,
                    vineyard_ipc_socket,
                )
                logger.debug(
                    "new_protocol = %s, new_source = %s", new_protocol, new_source
                )
                loader.attr[types_pb2.PROTOCOL].CopyFrom(utils.s_to_attr(new_protocol))
                loader.attr[types_pb2.SOURCE].CopyFrom(utils.s_to_attr(new_source))

        engine_config = self.get_analytical_engine_config()
        if self._launcher.type() == types_pb2.HOSTS:
            vineyard_endpoint = engine_config["vineyard_rpc_endpoint"]
        else:
            vineyard_endpoint = self._launcher.vineyard_internal_endpoint
        vineyard_ipc_socket = engine_config["vineyard_socket"]

        for loader in op.large_attr.chunk_meta_list.items:
            # handle vertex or edge loader
            if loader.attr[types_pb2.CHUNK_TYPE].s.decode() == "loader":
                # set op bodies, this is for loading graph from numpy/pandas
                op_bodies = []
                for bodies in dag_bodies:
                    if bodies.body.op_key == op.key:
                        op_bodies.append(bodies)
                loader_op_bodies[op.key] = op_bodies
                try:
                    _process_loader_func(loader, vineyard_endpoint, vineyard_ipc_socket)
                except:  # noqa: E722
                    logger.exception(
                        "Failed to process loader function for %s:%s",
                        loader.attr[types_pb2.PROTOCOL].s.decode(),
                        loader.attr[types_pb2.SOURCE].s.decode(),
                    )
                    raise

        return op_def_pb2.OpResult(code=OK, key=op.key)
