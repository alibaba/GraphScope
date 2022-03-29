#!/usr/bin/env python3

import glob
import hashlib
import multiprocessing
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from string import Template

import yaml


def compute_sig(s):
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


NETWORKX = os.environ.get("NETWORKX", "ON")
try:
    import gscoordinator

    COORDINATOR_HOME = Path(gscoordinator.__file__).parent.parent.absolute()
except ModuleNotFoundError:
    COORDINATOR_HOME = Path(
        os.path.join(os.path.dirname(__file__), "..", "coordinator")
    )

TEMPLATE_DIR = COORDINATOR_HOME / "gscoordinator" / "template"
BUILTIN_APP_RESOURCE_PATH = (
    COORDINATOR_HOME / "gscoordinator" / "builtin" / "app" / "builtin_app.gar"
)
CMAKELISTS_TEMPLATE = TEMPLATE_DIR / "CMakeLists.template"
GRAPHSCOPE_HOME = (
    os.environ["GRAPHSCOPE_HOME"]
    if "GRAPHSCOPE_HOME" in os.environ
    else "/opt/graphscope"
)
WORKSPACE = Path(os.path.join("/", tempfile.gettempprefix(), "gs", "builtin"))


def cmake_and_make(cmake_commands):
    try:
        cmake_process = subprocess.run(
            cmake_commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True
        )
        make_process = subprocess.run(
            [shutil.which("make"), "-j4"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        shutil.rmtree("CMakeFiles")
    except subprocess.CalledProcessError as e:
        print(e.stderr)


def cmake_graph(graph_class):
    library_name = compute_sig(graph_class)
    library_dir = WORKSPACE / library_name
    library_dir.mkdir(exist_ok=True)
    os.chdir(library_dir)
    cmakelists_file = library_dir / "CMakeLists.txt"
    with open(CMAKELISTS_TEMPLATE, mode="r") as template:
        content = template.read()
        content = Template(content).safe_substitute(
            _analytical_engine_home=GRAPHSCOPE_HOME,
            _frame_name=library_name,
            _graph_type=graph_class,
        )
        with open(cmakelists_file, mode="w") as f:
            f.write(content)
    cmake_commands = ["cmake", ".", "-DNETWORKX=" + NETWORKX]
    if "ArrowFragment" in graph_class:
        cmake_commands.append("-DPROPERTY_GRAPH_FRAME=True")
    else:
        cmake_commands.append("-DPROJECT_FRAME=True")

    cmake_and_make(cmake_commands)
    print("Finished compiling", graph_class)


def cmake_app(app):
    algo, graph_class = app
    if "ArrowFragment" in graph_class:
        graph_header = "vineyard/graph/fragment/arrow_fragment.h"
    elif "ArrowProjectedFragment" in graph_class:
        graph_header = "core/fragment/arrow_projected_fragment.h"
    elif "ArrowFlattenedFragment" in graph_class:
        graph_header = "core/fragment/arrow_flattened_fragment.h"
    elif "DynamicProjectedFragment" in graph_class:
        graph_header = "core/fragment/dynamic_projected_fragment.h"
    else:
        raise ValueError("Not supported graph class %s" % graph_class)

    app_type, app_header, app_class = get_app_info(algo)
    assert app_type == "cpp_pie", "Only support cpp_pie currently."

    library_name = compute_sig(f"{app_type}.{app_class}.{graph_class}")
    library_dir = WORKSPACE / library_name
    library_dir.mkdir(exist_ok=True)
    os.chdir(library_dir)
    cmakelists_file = library_dir / "CMakeLists.txt"
    with open(CMAKELISTS_TEMPLATE, mode="r") as template:
        content = template.read()
        content = Template(content).safe_substitute(
            _analytical_engine_home=GRAPHSCOPE_HOME,
            _frame_name=library_name,
            _graph_type=graph_class,
            _graph_header=graph_header,
            _app_type=app_class,
            _app_header=app_header,
        )
        with open(cmakelists_file, mode="w") as f:
            f.write(content)
    cmake_commands = ["cmake", ".", "-DNETWORKX=" + NETWORKX]

    cmake_and_make(cmake_commands)
    print("Finished compiling", app_class, graph_class)


def get_app_info(algo: str):
    fp = BUILTIN_APP_RESOURCE_PATH  # default is builtin app resources.
    with zipfile.ZipFile(fp, "r") as zip_ref:
        with zip_ref.open(".gs_conf.yaml", "r") as f:
            config_yaml = yaml.safe_load(f)

    for app in config_yaml["app"]:
        if app["algo"] == algo:
            app_type = app["type"]  # cpp_pie or cython_pregel or cython_pie
            if app_type == "cpp_pie":
                return app_type, app["src"], f"{app['class_name']}<_GRAPH_TYPE>"

    raise KeyError("Algorithm %s does not exist in the gar resource." % algo)


def compile_graph():
    property_frame_template = "vineyard::ArrowFragment<{},{}>"
    projected_frame_template = "gs::ArrowProjectedFragment<{},{},{},{}>"
    flattened_frame_template = "gs::ArrowFlattenedFragment<{},{},{},{}>"
    dynamic_projected_frame_template = "gs::DynamicProjectedFragment<{},{}>"

    oid_types = ["int64_t", "std::string"]
    vid_types = ["uint64_t"]
    vdata_types = ["int64_t", "grape::EmptyType"]
    edata_types = ["grape::EmptyType", "int64_t", "double"]
    graph_classes = []

    for oid in oid_types:
        for vid in vid_types:
            graph_class = property_frame_template.format(oid, vid)
            graph_classes.append(graph_class)

    for oid in oid_types:
        for vid in vid_types:
            for vdata in vdata_types:
                for edata in edata_types:
                    graph_class = projected_frame_template.format(
                        oid, vid, vdata, edata
                    )
                    flattend_graph_class = flattened_frame_template.format(
                        oid, vid, vdata, edata
                    )
                    graph_classes.append(graph_class)
                    graph_classes.append(flattend_graph_class)

    for vdata in vdata_types:
        for edata in edata_types:
            graph_class = dynamic_projected_frame_template.format(vdata, edata)
            graph_classes.append(graph_class)

    with multiprocessing.Pool() as pool:
        pool.map(cmake_graph, graph_classes)


def compile_cpp_pie_app():
    targets = []
    # 1. arrow fragment
    property_template = "vineyard::ArrowFragment<{},{}>"
    lu = property_template.format("int64_t", "uint64_t")
    su = property_template.format("std::string", "uint64_t")
    # no builtin app can run on the arrow property graph

    # 2. projected arrow fragment
    project_template = "gs::ArrowProjectedFragment<{},{},{},{}>"
    psuee = project_template.format(
        "std::string", "uint64_t", "grape::EmptyType", "grape::EmptyType"
    )
    psuel = project_template.format(
        "std::string", "uint64_t", "grape::EmptyType", "int64_t"
    )
    psued = project_template.format(
        "std::string", "uint64_t", "grape::EmptyType", "double"
    )
    psull = project_template.format("std::string", "uint64_t", "int64_t", "int64_t")

    plull = project_template.format("int64_t", "uint64_t", "int64_t", "int64_t")
    pluee = project_template.format(
        "int64_t", "uint64_t", "grape::EmptyType", "grape::EmptyType"
    )
    pluel = project_template.format(
        "int64_t", "uint64_t", "grape::EmptyType", "int64_t"
    )
    plued = project_template.format("int64_t", "uint64_t", "grape::EmptyType", "double")
    targets.extend(
        [
            ("pagerank", psuee),
            ("pagerank", pluee),
            ("pagerank", plull),
            ("wcc", psuee),
            ("wcc", pluee),
            ("wcc", plull),
            ("sssp", psuel),
            ("sssp", psued),
            ("sssp", pluel),
            ("sssp", plued),
            ("sssp", plull),
            ("sssp", psull),
            ("cdlp", psuee),
            ("cdlp", pluee),
            ("cdlp", plull),
            ("bfs", psuee),
            ("bfs", pluee),
            ("bfs", plull),
            ("kcore", psuee),
            ("kcore", pluee),
            ("kshell", plull),
            ("kshell", pluee),
            ("kshell", psuee),
            ("hits", psuee),
            ("hits", pluee),
            ("hits", plull),
            ("triangles", psuee),
            ("triangles", pluee),
            ("triangles", plull),
            ("clustering", psuee),
            ("clustering", pluee),
            ("clustering", plull),
            ("degree_centrality", psuee),
            ("degree_centrality", pluee),
            ("degree_centrality", plull),
            ("eigenvector_centrality", plull),
            ("eigenvector_centrality", psued),
            ("eigenvector_centrality", psuel),
            ("eigenvector_centrality", plued),
            ("eigenvector_centrality", pluel),
            ("katz_centrality", psuee),
            ("katz_centrality", pluee),
            ("katz_centrality", plull),
            ("is_simple_path", plull),
            ("louvain", plull),
            ("sssp_has_path", plull),
        ]
    )

    # 3. flatten fragment
    flatten_template = "gs::ArrowFlattenedFragment<{},{},{},{}>"
    fsuee = flatten_template.format(
        "std::string", "uint64_t", "grape::EmptyType", "grape::EmptyType"
    )
    fsuel = flatten_template.format(
        "std::string", "uint64_t", "grape::EmptyType", "int64_t"
    )
    fsued = flatten_template.format(
        "std::string", "uint64_t", "grape::EmptyType", "double"
    )
    fluee = flatten_template.format(
        "int64_t", "uint64_t", "grape::EmptyType", "grape::EmptyType"
    )
    fluel = flatten_template.format(
        "int64_t", "uint64_t", "grape::EmptyType", "int64_t"
    )
    flued = flatten_template.format("int64_t", "uint64_t", "grape::EmptyType", "double")
    targets.extend(
        [
            ("pagerank", fsuee),
            ("pagerank", fluee),
            ("sssp", fsuel),
            ("sssp", fsued),
            ("sssp", fluel),
            ("sssp", flued),
            # ("cdlp", fsuee),
            # ("cdlp", fluee),
            ("kcore", fsuee),
            ("kcore", fluee),
            ("triangles", fsuee),
            ("triangles", fluee),
        ]
    )

    # 4. dynamic fragment
    dynamic_template = "gs::DynamicProjectedFragment<{},{}>"
    dee = dynamic_template.format("grape::EmptyType", "grape::EmptyType")
    ded = dynamic_template.format("grape::EmptyType", "double")
    dle = dynamic_template.format("int64_t", "grape::EmptyType")
    dde = dynamic_template.format("double", "grape::EmptyType")
    targets.extend(
        [
            ("wcc_projected", dee),
            ("sssp_projected", dee),
            ("sssp_projected", ded),
            ("sssp_path", dee),
            ("sssp_has_path", dee),
            ("sssp_average_length", dee),
            ("sssp_average_length", ded),
            ("hits", dee),
            ("degree_centrality", dee),
            ("eigenvector_centrality", dee),
            ("eigenvector_centrality", ded),
            ("katz_centrality", dee),
            ("katz_centrality", ded),
            ("bfs_generic", dee),
            ("kcore", dee),
            ("lcc", dee),
            ("clustering", dee),
            ("triangles", dee),
            ("transitivity", dee),
            ("avg_clustering", dee),
            ("pagerank_nx", dee),
            ("pagerank_nx", ded),
            ("degree_assortativity_coefficient", dee),
            ("node_boundary", dee),
            ("edge_boundary", dee),
            ("average_degree_connectivity", dee),
            ("average_degree_connectivity", ded),
            ("attribute_assortativity_coefficient", dle),
            ("attribute_assortativity_coefficient", dde),
        ]
    )

    with multiprocessing.Pool() as pool:
        pool.map(cmake_app, targets)


if __name__ == "__main__":
    os.makedirs(WORKSPACE, exist_ok=True)
    compile_graph()
    compile_cpp_pie_app()
