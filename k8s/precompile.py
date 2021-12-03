#!/usr/bin/env python3

import glob
import hashlib
import multiprocessing
import os
import shutil
import subprocess
import sys
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
WORKSPACE = Path("/tmp/gs/builtin")


def cmake_and_make(cmake_commands):
    try:
        cmake_process = subprocess.run(
            cmake_commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True
        )
        make_process = subprocess.run(
            ["make", "-j4"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True
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
    property_template = "vineyard::ArrowFragment<{},{}>"
    project_template = "gs::ArrowProjectedFragment<{},{},{},{}>"
    flatten_template = "gs::ArrowFlattenedFragment<{},{},{},{}>"
    dynamic_template = "gs::DynamicProjectedFragment<{},{}>"

    lu = property_template.format("int64_t", "uint64_t")
    psull = project_template.format("std::string", "uint64_t", "int64_t", "int64_t")
    pllul = project_template.format("int64_t", "uint64_t", "int64_t", "int64_t")
    pluee = project_template.format(
        "int64_t", "uint64_t", "grape::EmptyType", "grape::EmptyType"
    )
    pluel = project_template.format("int64_t", "uint64_t", "grape::EmptyType", "int64_t")
    lued = project_template.format("int64_t", "uint64_t", "grape::EmptyType", "double")
    fluee = flatten_template.format(
        "int64_t", "uint64_t", "grape::EmptyType", "grape::EmptyType"
    )
    fluel = flatten_template.format(
        "int64_t", "uint64_t", "grape::EmptyType", "int64_t"
    )
    flued = flatten_template.format("int64_t", "uint64_t", "grape::EmptyType", "double")
    dtee = dynamic_template.format("grape::EmptyType", "grape::EmptyType")
    dtel = dynamic_template.format("grape::EmptyType", "int64_t")
    dted = dynamic_template.format("grape::EmptyType", "double")

    targets = [
        ("pagerank", pluee),
        ("pagerank", pluel),
        ("pagerank", pllul),
        ("hits", pluee),
        ("hits", fluee),
        ("hits", pllul),
        ("wcc", pluee),
        ("wcc", pluel),
        ("wcc", pllul),
        ("cdlp", pluee),
        ("cdlp", pluel),
        ("bfs", pluee),
        ("bfs", pluel),
        ("bfs", pllul),
        ("sssp", pluel),
        ("sssp", lued),
        ("sssp", pllul),
        ("sssp", psull),
        ("kcore", pluee),
        ("kcore", dtee),
        ("kshell", pluee),
        ("kshell", pllul),
        ("triangles", pluee),
        ("triangles", dtee),
        ("triangles", pllul),
        ("clustering", pluee),
        ("clustering", fluee),
        ("clustering", dtee),
        ("clustering", pllul),
        ("degree_centrality", pluee),
        ("degree_centrality", pllul),
        ("degree_centrality", fluee),
        ("degree_centrality", dtee),
        ("eigenvector_centrality", pluel),
        ("eigenvector_centrality", pllul),
        ("katz_centrality", pluel),
        ("katz_centrality", pllul),
        ("is_simple_path", pllul),
        ("louvain", pllul),
        ("sssp_has_path", pllul),
        ("property_sssp", lu),
    ]

    if "NIGHTLY" in os.environ:
        targets.extend(
            [
                ("sssp_average_length", pluee),
                ("sssp_average_length", pluel),
                ("sssp_average_length", lued),
                ("sssp_average_length", dtee),
                ("sssp_average_length", dtel),
                ("sssp_average_length", dted),
                ("pagerank_nx", fluee),
                ("pagerank_nx", dtee),
                ("hits", dtee),
                ("eigenvector_centrality", pluee),
                ("eigenvector_centrality", lued),
                ("eigenvector_centrality", fluee),
                ("eigenvector_centrality", fluel),
                ("eigenvector_centrality", flued),
                ("eigenvector_centrality", dtee),
                ("eigenvector_centrality", dtel),
                ("eigenvector_centrality", dted),
                ("katz_centrality", pluee),
                ("katz_centrality", lued),
                ("katz_centrality", fluee),
                ("katz_centrality", fluel),
                ("katz_centrality", flued),
                ("katz_centrality", dtee),
                ("katz_centrality", dtel),
                ("katz_centrality", dted),
                ("transitivity", pluee),
                ("transitivity", fluee),
                ("transitivity", dtee),
                ("avg_clustering", pluee),
                ("avg_clustering", fluee),
                ("avg_clustering", dtee),
                ("sssp_projected", dtee),
                ("sssp_projected", dtel),
                ("sssp_projected", dted),
                ("sssp_projected", fluee),
                ("sssp_projected", fluel),
                ("sssp_projected", flued),
            ]
        )

    with multiprocessing.Pool() as pool:
        pool.map(cmake_app, targets)


if __name__ == "__main__":
    os.makedirs(WORKSPACE, exist_ok=True)
    compile_graph()
    compile_cpp_pie_app()
