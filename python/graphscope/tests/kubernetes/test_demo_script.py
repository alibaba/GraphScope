#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import os
import random
import string
import tempfile
import time

import numpy as np
import pytest
import vineyard
from kubernetes import client
from kubernetes import config
from kubernetes.client.rest import ApiException

import graphscope
from graphscope import Graph
from graphscope.config import GSConfig as gs_config
from graphscope.dataset import load_ldbc
from graphscope.dataset import load_modern_graph
from graphscope.framework.loader import Loader

graphscope.set_option(show_log=True)
graphscope.set_option(log_level="DEBUG")
logger = logging.getLogger("graphscope")


def get_k8s_volumes():
    k8s_volumes = {
        "data": {
            "type": "hostPath",
            "field": {"path": os.environ["GS_TEST_DIR"], "type": "Directory"},
            "mounts": {"mountPath": "/testingdata"},
        }
    }
    return k8s_volumes


def get_gs_registry_on_ci_env():
    if "GS_REGISTRY" in os.environ:
        return os.environ["GS_REGISTRY"]
    return gs_config.k8s_image_registry


def get_gs_tag_on_ci_env():
    if "GS_TAG" in os.environ:
        return os.environ["GS_TAG"]
    return gs_config.k8s_image_tag


# get the num of engine pod which contains the specific name
def get_engine_pod_num(name, namespace):
    from kubernetes import client
    from kubernetes import config
    from kubernetes.client.rest import ApiException

    config.load_kube_config()
    v1 = client.CoreV1Api()
    num = 0
    try:
        pod_lists = v1.list_namespaced_pod(namespace=namespace)
        for i in pod_lists.items:
            if name in i.metadata.name and i.status.phase == "Running":
                num += 1
    except ApiException as e:
        if e.status == 404:
            return num
        else:
            raise e
    return num


def wait_for_pod_deletion(name, namespace, timeout=120):
    from kubernetes import client
    from kubernetes import config
    from kubernetes.client.rest import ApiException

    config.load_kube_config()
    v1 = client.CoreV1Api()

    # Wait for the pod to be deleted
    start_time = time.time()
    pod = ""
    pod_lists = v1.list_namespaced_pod(namespace=namespace)
    for i in pod_lists.items:
        if name in i.metadata.name:
            pod = i.metadata.name
    logger.info("Waiting for pod %s to be deleted", pod)
    while time.time() - start_time < timeout:
        try:
            _ = v1.read_namespaced_pod(pod, namespace)
        except ApiException:
            # The pod has been deleted
            logger.info("Pod %s has been deleted", pod)
            return

        time.sleep(1)
    raise Exception("Pod not deleted after {} seconds".format(timeout))


@pytest.fixture
def gs_session():
    sess = graphscope.session(
        num_workers=1,
        k8s_image_registry=get_gs_registry_on_ci_env(),
        k8s_image_tag=get_gs_tag_on_ci_env(),
        k8s_coordinator_cpu=2,
        k8s_coordinator_mem="4Gi",
        k8s_vineyard_cpu=2,
        k8s_vineyard_mem="512Mi",
        k8s_engine_cpu=2,
        k8s_engine_mem="4Gi",
        vineyard_shared_mem="4Gi",
        k8s_volumes=get_k8s_volumes(),
    )
    yield sess
    sess.close()


@pytest.fixture
def gs_session_distributed():
    sess = graphscope.session(
        num_workers=2,
        k8s_image_registry=get_gs_registry_on_ci_env(),
        k8s_image_tag=get_gs_tag_on_ci_env(),
        k8s_coordinator_cpu=2,
        k8s_coordinator_mem="4Gi",
        k8s_vineyard_cpu=2,
        k8s_vineyard_mem="1Gi",
        k8s_engine_cpu=2,
        k8s_engine_mem="4Gi",
        vineyard_shared_mem="4Gi",
        k8s_volumes=get_k8s_volumes(),
    )
    yield sess
    sess.close()


@pytest.fixture
def gs_session_with_lazy_mode():
    sess = graphscope.session(
        num_workers=1,
        k8s_image_registry=get_gs_registry_on_ci_env(),
        k8s_image_tag=get_gs_tag_on_ci_env(),
        k8s_coordinator_cpu=2,
        k8s_coordinator_mem="4Gi",
        k8s_vineyard_cpu=2,
        k8s_vineyard_mem="512Mi",
        k8s_engine_cpu=2,
        k8s_engine_mem="4Gi",
        vineyard_shared_mem="4Gi",
        k8s_vineyard_deployment="vineyardd-sample",
        k8s_namespace="graphscope-system",
        k8s_volumes=get_k8s_volumes(),
        k8s_deploy_mode="lazy",
    )
    yield sess
    sess.close()


@pytest.fixture
def create_vineyard_deployment_on_single_node():
    import vineyard


@pytest.fixture
def create_vineyard_deployment_on_single_node():
    # create vineyard deployment on single node
    # set the replicas of vineyard and etcd to 1 as there is only one node in the cluster
    vineyard.deploy.vineyardctl.deploy.vineyard_deployment(
        replicas=1,
        etcd_replicas=1,
        namespace="graphscope-system",
        create_namespace=True,
    )


@pytest.fixture
def gs_session_with_vineyard_deployment(create_vineyard_deployment_on_single_node):
    sess = graphscope.session(
        num_workers=2,
        k8s_namespace="graphscope-system",
        k8s_image_registry=get_gs_registry_on_ci_env(),
        k8s_image_tag=get_gs_tag_on_ci_env(),
        k8s_coordinator_cpu=2,
        k8s_coordinator_mem="4Gi",
        k8s_vineyard_cpu=2,
        k8s_vineyard_mem="1Gi",
        k8s_engine_cpu=2,
        k8s_engine_mem="4Gi",
        vineyard_shared_mem="4Gi",
        k8s_vineyard_deployment="vineyardd-sample",
        k8s_volumes=get_k8s_volumes(),
    )
    yield sess
    sess.close()


@pytest.fixture
def create_vineyard_deployment_on_multiple_nodes():
    import vineyard

    # create vineyard deployment on multiple nodes
    # set the replicas of vineyard and etcd to 2 as there are 2 nodes in the kubernetes cluster
    vineyard.deploy.vineyardctl.deploy.vineyard_deployment(
        replicas=2,
        etcd_replicas=2,
        namespace="graphscope-system",
        create_namespace=True,
    )


@pytest.fixture
def gs_session_distributed_with_vineyard_deployment(
    create_vineyard_deployment_on_multiple_nodes,
):
    sess = graphscope.session(
        num_workers=2,
        k8s_namespace="graphscope-system",
        k8s_image_registry=get_gs_registry_on_ci_env(),
        k8s_image_tag=get_gs_tag_on_ci_env(),
        k8s_coordinator_cpu=2,
        k8s_coordinator_mem="4Gi",
        k8s_vineyard_cpu=2,
        k8s_vineyard_mem="1Gi",
        k8s_engine_cpu=2,
        k8s_engine_mem="4Gi",
        vineyard_shared_mem="4Gi",
        k8s_vineyard_deployment="vineyardd-sample",
        k8s_volumes=get_k8s_volumes(),
    )
    yield sess
    sess.close()


@pytest.fixture
def data_dir():
    return "/testingdata/ldbc_sample"


@pytest.fixture
def modern_graph_data_dir():
    return "/testingdata/modern_graph"


@pytest.fixture
def p2p_property_dir():
    return "/testingdata/property"


@pytest.mark.skipif("HDFS_HOST" not in os.environ, reason="HDFS not specified")
def test_demo_on_hdfs(gs_session_distributed):
    graph = gs_session_distributed.g()
    graph = graph.add_vertices(
        Loader(
            os.environ["HDFS_TEST_DIR"] + "/person_0_0.csv",
            host=os.environ["HDFS_HOST"],
            port=9000,
            delimiter="|",
        ),
        "person",
        [
            "firstName",
            "lastName",
            "gender",
            "birthday",
            # "creationDate",
            "locationIP",
            "browserUsed",
        ],
        "id",
    )
    graph = graph.add_edges(
        Loader(
            os.environ["HDFS_TEST_DIR"] + "/person_knows_person_0_0.csv",
            host=os.environ["HDFS_HOST"],
            port=9000,
            delimiter="|",
        ),
        "knows",
        [],
        src_label="person",
        dst_label="person",
    )

    # Interactive engine
    interactive = gs_session_distributed.gremlin(graph)
    sub_graph = interactive.subgraph(  # noqa: F841
        'g.V().hasLabel("person").outE("knows")'
    )

    # Analytical engine
    # project the projected graph to simple graph.
    simple_g = sub_graph.project(vertices={"person": []}, edges={"knows": []})

    pr_result = graphscope.pagerank(simple_g, delta=0.8)

    # output to hdfs
    pr_result.output(
        os.environ["HDFS_TEST_DIR"] + "/res.csv",
        selector={"id": "v.id", "rank": "r"},
        host=os.environ["HDFS_HOST"],
        port=9000,
    )


@pytest.mark.skip(reason="(caoye)skip for testing")
def test_vineyard_deployment_on_single_node(
    gs_session_with_vineyard_deployment, data_dir, modern_graph_data_dir
):
    test_demo_distribute(
        gs_session_with_vineyard_deployment, data_dir, modern_graph_data_dir
    )


@pytest.mark.skip(reason="(caoye)skip for testing")
def test_vineyard_deployment_on_multiple_nodes(
    gs_session_distributed_with_vineyard_deployment, data_dir, modern_graph_data_dir
):
    test_demo_distribute(
        gs_session_distributed_with_vineyard_deployment, data_dir, modern_graph_data_dir
    )


def test_demo_distribute(gs_session_distributed, data_dir, modern_graph_data_dir):
    graph = load_ldbc(gs_session_distributed, data_dir)

    # Interactive engine
    interactive = gs_session_distributed.gremlin(graph)
    sub_graph = interactive.subgraph(  # noqa: F841
        'g.V().hasLabel("person").outE("knows")'
    )
    person_count = (
        interactive.execute(
            'g.V().hasLabel("person").outE("knows").bothV().dedup().count()'
        )
        .all()
        .result()[0]
    )
    knows_count = (
        interactive.execute('g.V().hasLabel("person").outE("knows").count()')
        .all()
        .result()[0]
    )
    interactive2 = gs_session_distributed.gremlin(sub_graph)
    sub_person_count = interactive2.execute("g.V().count()").all().result()[0]
    sub_knows_count = interactive2.execute("g.E().count()").all().result()[0]
    assert person_count == sub_person_count
    assert knows_count == sub_knows_count

    # Analytical engine
    # project the projected graph to simple graph.
    simple_g = sub_graph.project(vertices={"person": []}, edges={"knows": []})

    pr_result = graphscope.pagerank(simple_g, delta=0.8)
    tc_result = graphscope.triangles(simple_g)

    # add the PageRank and triangle-counting results as new columns to the property graph
    sub_graph.add_column(pr_result, {"Ranking": "r"})
    sub_graph.add_column(tc_result, {"TC": "r"})

    # test subgraph on modern graph
    mgraph = load_modern_graph(gs_session_distributed, modern_graph_data_dir)

    # Interactive engine
    minteractive = gs_session_distributed.gremlin(mgraph)
    msub_graph = minteractive.subgraph(  # noqa: F841
        'g.V().hasLabel("person").outE("knows")'
    )
    person_count = (
        minteractive.execute(
            'g.V().hasLabel("person").outE("knows").bothV().dedup().count()'
        )
        .all()
        .result()[0]
    )
    msub_interactive = gs_session_distributed.gremlin(msub_graph)
    sub_person_count = msub_interactive.execute("g.V().count()").all().result()[0]
    assert person_count == sub_person_count

    # GNN engine


def test_demo_with_lazy_mode(
    gs_session_with_lazy_mode, data_dir, modern_graph_data_dir
):
    graph = load_ldbc(gs_session_with_lazy_mode, data_dir)

    interactive_name = "interactive-" + str(graph.vineyard_id)
    namespace = gs_session_with_lazy_mode.info["namespace"]
    assert get_engine_pod_num(interactive_name, namespace) == 0
    # Interactive engine
    interactive = gs_session_with_lazy_mode.gremlin(graph)
    sub_graph = interactive.subgraph(  # noqa: F841
        'g.V().hasLabel("person").outE("knows")'
    )
    person_count = (
        interactive.execute(
            'g.V().hasLabel("person").outE("knows").bothV().dedup().count()'
        )
        .all()
        .result()[0]
    )
    knows_count = (
        interactive.execute('g.V().hasLabel("person").outE("knows").count()')
        .all()
        .result()[0]
    )
    assert get_engine_pod_num(interactive_name, namespace) == 1

    interactive.close()
    # wait for engine pod to be deleted
    wait_for_pod_deletion(interactive_name, namespace)
    assert get_engine_pod_num(interactive_name, namespace) == 0

    interactive2_name = "interactive-" + str(sub_graph.vineyard_id)
    assert get_engine_pod_num(interactive2_name, namespace) == 0

    interactive2 = gs_session_with_lazy_mode.gremlin(sub_graph)
    assert get_engine_pod_num(interactive2_name, namespace) == 1

    sub_person_count = interactive2.execute("g.V().count()").all().result()[0]
    sub_knows_count = interactive2.execute("g.E().count()").all().result()[0]
    assert person_count == sub_person_count
    assert knows_count == sub_knows_count

    interactive2.close()
    # wait for engine pod to be deleted
    wait_for_pod_deletion(interactive2_name, namespace)
    assert get_engine_pod_num(interactive2_name, namespace) == 0

    # Analytical engine
    # project the projected graph to simple graph.
    simple_g = sub_graph.project(vertices={"person": []}, edges={"knows": []})

    pr_result = graphscope.pagerank(simple_g, delta=0.8)
    tc_result = graphscope.triangles(simple_g)

    # add the PageRank and triangle-counting results as new columns to the property graph
    sub_graph.add_column(pr_result, {"Ranking": "r"})
    sub_graph.add_column(tc_result, {"TC": "r"})

    # test subgraph on modern graph
    mgraph = load_modern_graph(gs_session_with_lazy_mode, modern_graph_data_dir)

    minteractive_name = "interactive-" + str(mgraph.vineyard_id)
    assert get_engine_pod_num(minteractive_name, namespace) == 0
    # Interactive engine
    minteractive = gs_session_with_lazy_mode.gremlin(mgraph)
    assert get_engine_pod_num(minteractive_name, namespace) == 1
    msub_graph = minteractive.subgraph(  # noqa: F841
        'g.V().hasLabel("person").outE("knows")'
    )
    person_count = (
        minteractive.execute(
            'g.V().hasLabel("person").outE("knows").bothV().dedup().count()'
        )
        .all()
        .result()[0]
    )

    minteractive.close()
    # wait for engine pod to be deleted
    wait_for_pod_deletion(minteractive_name, namespace)
    assert get_engine_pod_num(minteractive_name, namespace) == 0
    msub_interactive = gs_session_with_lazy_mode.gremlin(msub_graph)
    sub_person_count = msub_interactive.execute("g.V().count()").all().result()[0]
    assert person_count == sub_person_count

    # GNN engine


def test_multiple_session():
    namespace = "gs-multi-" + "".join(
        [random.choice(string.ascii_lowercase) for _ in range(6)]
    )

    sess = graphscope.session(
        num_workers=1,
        k8s_image_registry=get_gs_registry_on_ci_env(),
        k8s_image_tag=get_gs_tag_on_ci_env(),
        k8s_volumes=get_k8s_volumes(),
    )
    info = sess.info
    assert info["status"] == "active"
    assert len(info["engine_hosts"].split(",")) == 1

    sess2 = graphscope.session(
        k8s_namespace=namespace,
        num_workers=2,
        k8s_image_registry=get_gs_registry_on_ci_env(),
        k8s_image_tag=get_gs_tag_on_ci_env(),
        k8s_volumes=get_k8s_volumes(),
    )

    info = sess2.info
    assert info["status"] == "active"
    assert len(info["engine_hosts"].split(",")) == 2

    sess2.close()
    sess.close()


def test_query_modern_graph(
    gs_session, modern_graph_data_dir, modern_scripts, modern_bytecode
):
    graph = load_modern_graph(gs_session, modern_graph_data_dir)
    interactive = gs_session.gremlin(graph)
    # query on modern graph
    for q in modern_scripts:
        result = interactive.execute(q).all().result()[0]
        assert result == 1
    # traversal on moder graph
    g = interactive.traversal_source()
    modern_bytecode(g)


def create_pv(name, path):
    config.load_kube_config()
    v1 = client.CoreV1Api()
    try:
        pv = {
            "apiVersion": "v1",
            "kind": "PersistentVolume",
            "metadata": {
                "name": name,
                "labels": {
                    "app.kubernetes.io/name": name,
                },
            },
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": "manual",
                "hostPath": {
                    "path": path,
                    "type": "DirectoryOrCreate",
                },
            },
        }

        v1.create_persistent_volume(body=pv)
    except ApiException as e:
        logger.error(
            "Exception when calling CoreV1Api->create_persistent_volume: %s\n" % e
        )


def create_pvc(name, namespace, pv_name):
    config.load_kube_config()
    v1 = client.CoreV1Api()
    try:
        pvc = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": name,
                "namespace": namespace,
            },
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": "1Gi"}},
                "storageClassName": "manual",
                "selector": {
                    "matchLabels": {
                        "app.kubernetes.io/name": pv_name,
                    }
                },
            },
        }

        v1.create_namespaced_persistent_volume_claim(namespace=namespace, body=pvc)
    except ApiException as e:
        logger.error(
            "Exception when calling CoreV1Api->create_persistent_volume_claim: %s\n" % e
        )


def create_namespace(name):
    config.load_kube_config()
    v1 = client.CoreV1Api()
    try:
        # Create a new namespace object
        ns = client.V1Namespace()
        ns.metadata = client.V1ObjectMeta(name=name)
        v1.create_namespace(ns)
    except ApiException as e:
        logger.error("Exception when calling CoreV1Api->create_namespace: %s\n" % e)


def test_store_and_restore_graphs(
    modern_graph_data_dir,
):
    namespace = "graphscope-system"
    create_namespace(namespace)
    old_sess = graphscope.session(
        num_workers=1,
        k8s_namespace="graphscope-system",
        k8s_image_registry=get_gs_registry_on_ci_env(),
        k8s_image_tag=get_gs_tag_on_ci_env(),
        k8s_coordinator_cpu=2,
        k8s_coordinator_mem="4Gi",
        k8s_vineyard_cpu=2,
        k8s_vineyard_mem="1Gi",
        k8s_engine_cpu=2,
        k8s_engine_mem="4Gi",
        vineyard_shared_mem="4Gi",
        k8s_vineyard_deployment="vineyardd-sample",
        k8s_volumes=get_k8s_volumes(),
    )
    graph = load_modern_graph(old_sess, "/testingdata/modern_graph")
    interactive = old_sess.gremlin(graph)
    graph_vineyard_id = vineyard.ObjectID(graph.vineyard_id)
    graph_person_count = (
        interactive.execute(
            'g.V().hasLabel("person").outE("knows").bothV().dedup().count()'
        )
        .all()
        .result()[0]
    )
    graph_knows_count = (
        interactive.execute('g.V().hasLabel("person").outE("knows").count()')
        .all()
        .result()[0]
    )

    sub_graph = interactive.subgraph(  # noqa: F841
        'g.V().hasLabel("person").outE("knows")'
    )
    sub_graph_person_count = (
        interactive.execute(
            'g.V().hasLabel("person").outE("knows").bothV().dedup().count()'
        )
        .all()
        .result()[0]
    )
    sub_graph_knows_count = (
        interactive.execute('g.V().hasLabel("person").outE("knows").count()')
        .all()
        .result()[0]
    )

    sub_graph_vineyard_id = vineyard.ObjectID(sub_graph.vineyard_id)
    # create the pv and pvc for backup
    pv_name = "test-pv"
    path = "/var/vineyard1/test"
    pvc_name = "test-pvc"
    create_pv(pv_name, path)
    create_pvc(pvc_name, namespace, pv_name)
    # store the specific graphs to pvc
    old_sess.store_to_pvc(
        graphIDs=[graph_vineyard_id, sub_graph_vineyard_id],
        path=path,
        pvc_name=pvc_name,
    )

    old_sess.close()
    # wait the session to be closed
    wait_for_pod_deletion("coordinator", namespace)
    wait_for_pod_deletion("gs-engine", namespace)

    # create a new session to recover the graph
    new_sess = graphscope.session(
        num_workers=1,
        k8s_namespace="graphscope-system",
        k8s_image_registry=get_gs_registry_on_ci_env(),
        k8s_image_tag=get_gs_tag_on_ci_env(),
        k8s_coordinator_cpu=2,
        k8s_coordinator_mem="4Gi",
        k8s_vineyard_cpu=2,
        k8s_vineyard_mem="1Gi",
        k8s_engine_cpu=2,
        k8s_engine_mem="4Gi",
        vineyard_shared_mem="4Gi",
        k8s_vineyard_deployment="vineyardd-sample",
        k8s_volumes=get_k8s_volumes(),
    )
    new_sess.restore_from_pvc(
        path=path,
        pvc_name=pvc_name,
    )

    new_graph = new_sess.g(vineyard.ObjectID(graph_vineyard_id))
    interactive = new_sess.gremlin(new_graph)
    new_graph_person_count = (
        interactive.execute(
            'g.V().hasLabel("person").outE("knows").bothV().dedup().count()'
        )
        .all()
        .result()[0]
    )
    new_graph_knows_count = (
        interactive.execute('g.V().hasLabel("person").outE("knows").count()')
        .all()
        .result()[0]
    )

    new_sub_graph = new_sess.g(vineyard.ObjectID(sub_graph_vineyard_id))
    interactive2 = new_sess.gremlin(new_sub_graph)
    new_sub_graph_person_count = interactive2.execute("g.V().count()").all().result()[0]
    new_sub_graph_knows_count = interactive2.execute("g.E().count()").all().result()[0]

    assert graph_person_count == new_graph_person_count
    assert graph_knows_count == new_graph_knows_count
    assert sub_graph_person_count == new_sub_graph_person_count
    assert sub_graph_knows_count == new_sub_graph_knows_count


def test_serialize_roundtrip(gs_session_distributed, p2p_property_dir):
    graph = gs_session_distributed.g(generate_eid=False, retain_oid=True)
    graph = graph.add_vertices(f"{p2p_property_dir}/p2p-31_property_v_0", "person")
    graph = graph.add_edges(
        f"{p2p_property_dir}/p2p-31_property_e_0",
        label="knows",
        src_label="person",
        dst_label="person",
    )

    serialization_path = os.path.join("/", tempfile.gettempprefix(), "serialize")
    graph.save_to(serialization_path)
    new_graph = Graph.load_from(serialization_path, gs_session_distributed)
    pg = new_graph.project(vertices={"person": []}, edges={"knows": ["dist"]})
    ctx = graphscope.sssp(pg, src=6)
    ret = (
        ctx.to_dataframe({"node": "v.id", "r": "r"}, vertex_range={"end": 6})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    expect = np.array(
        [[1.0, 260.0], [2.0, 229.0], [3.0, 310.0], [4.0, 256.0], [5.0, 303.0]]
    )
    assert np.all(ret == expect)


def test_local_vm_distribute(gs_session_distributed, p2p_property_dir):
    # Test for compiling of graph and app.
    graph = gs_session_distributed.g(directed=False, vertex_map="local")
    graph = graph.add_edges(
        f"{p2p_property_dir}/p2p-31_property_e_0",
        label="knows",
        src_label="person",
        dst_label="person",
    )
    graph = graph.project(vertices={"person": []}, edges={"knows": []})
    ctx = graphscope.wcc(graph)
    ret = (
        ctx.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=int)
    )
    wcc_result = np.loadtxt(f"{os.environ['GS_TEST_DIR']}/p2p-31-wcc_auto", dtype=int)
    # Test algorithm correctness
    assert np.all(ret == wcc_result)


def get_addr_on_ci_env():
    if "GS_ADDR" in os.environ:
        return os.environ["GS_ADDR"]
    else:
        raise RuntimeError("`GS_ADDR` doesn't existed in environ")


@pytest.mark.skipif("GS_ADDR" not in os.environ, reason="GS_ADDR not specified")
def test_helm_installation(data_dir, modern_graph_data_dir):
    addr = get_addr_on_ci_env()
    sess = graphscope.session(addr=addr)
    graph = load_ldbc(sess, data_dir)

    # Interactive engine
    interactive = sess.gremlin(graph)
    sub_graph = interactive.subgraph(  # noqa: F841
        'g.V().hasLabel("person").outE("knows")'
    )
    person_count = (
        interactive.execute(
            'g.V().hasLabel("person").outE("knows").bothV().dedup().count()'
        )
        .all()
        .result()[0]
    )
    knows_count = (
        interactive.execute('g.V().hasLabel("person").outE("knows").count()')
        .all()
        .result()[0]
    )
    interactive2 = sess.gremlin(sub_graph)
    sub_person_count = interactive2.execute("g.V().count()").all().result()[0]
    sub_knows_count = interactive2.execute("g.E().count()").all().result()[0]
    assert person_count == sub_person_count
    assert knows_count == sub_knows_count

    # Analytical engine
    # project the projected graph to simple graph.
    simple_g = sub_graph.project(vertices={"person": []}, edges={"knows": []})

    pr_result = graphscope.pagerank(simple_g, delta=0.8)

    # add the PageRank and triangle-counting results as new columns to the property graph
    sub_graph.add_column(pr_result, {"Ranking": "r"})


def test_modualize():
    sess = graphscope.session(
        num_workers=1,
        k8s_image_registry=get_gs_registry_on_ci_env(),
        k8s_image_tag=get_gs_tag_on_ci_env(),
        enabled_engines="interactive",
    )
    sess.close()
