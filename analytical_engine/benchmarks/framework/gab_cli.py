# gab_cli.py
import os
import subprocess
import argparse
from dotenv import load_dotenv
import shutil
import requests, zipfile, io
import sys


# Load environment variables from .env file at the start
load_dotenv()

def run_flash_perf(algorithm, data_path=None):
    THREAD_LIST = [1, 2, 4, 8, 16, 32]
    MACHINE_LIST = [2, 4, 8, 16]
    MEMORY = "100Gi"
    CPU = "32"
    yaml_dir = "config"
    template_path = os.path.join(yaml_dir, "flash-mpijob-template.yaml")

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    if algorithm == "k-core-search":
        ALGORITHM_PARAMETER_ = 3
    elif algorithm == "clique":
        ALGORITHM_PARAMETER_ = 5
    else:
        ALGORITHM_PARAMETER_ = 1

    VOLUMES_BLOCK = ""
    VOLUME_MOUNTS_BLOCK = ""

    if algorithm != "sssp":
        VOLUMES_BLOCK = (
            f"volumes:\n          - name: flash-data\n            hostPath:\n              path: {data_path}\n              type: Directory"
            if data_path
            else ""
        )
        VOLUME_MOUNTS_BLOCK = (
            f"volumeMounts:\n            - name: flash-data\n              mountPath: /opt/data"
            if data_path
            else ""
        )
    else:
        VOLUMES_BLOCK = (
            f"volumes:\n          - name: flash-data\n            hostPath:\n              path: {data_path}\n              type: Directory"
            if data_path
            else ""
        )
        VOLUME_MOUNTS_BLOCK = (
            f"volumeMounts:\n            - name: flash-data\n              mountPath: /opt/data_sssp"
            if data_path
            else ""
        )

    try:
        with open(template_path, "r") as f:
            template_str = f.read()
    except FileNotFoundError:
        print(f"[ERROR] Template file not found: {template_path}")
        return

    for thread in THREAD_LIST:
        params = {
            "SLOTS_PER_WORKER": thread,
            "HOST_PATH": data_path,
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": 1,
            "MPIRUN_NP": thread,
            "ALGORITHM": algorithm,
            "ALGORITHM_PARAMETER": ALGORITHM_PARAMETER_,
            "SINGLE_MACHINE": 1,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }

        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"flash-mpijob-{algorithm}-single.yaml")

        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)

            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")

            print(
                f"[INFO] Submitting MPIJob: {algorithm} single-machine, threads={thread}"
            )
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run(
                [
                    kubectl,
                    "wait",
                    "--for=condition=Succeeded",
                    "mpijob/flash-mpijob",
                    "--timeout=100m",
                ],
                check=True,
            )
            dataset_name = os.path.basename(os.path.normpath(data_path))
            log_file = os.path.join(
                output_dir, f"flash_{algorithm}-{dataset_name}-n1-p{thread}.log"
            )
            subprocess.run(
                [kubectl, "logs", "job/flash-mpijob-launcher"],
                stdout=open(log_file, "w"),
                check=True,
            )
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    for machines in MACHINE_LIST:
        params = {
            "SLOTS_PER_WORKER": 32,
            "HOST_PATH": data_path,
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": machines,
            "MPIRUN_NP": machines * 32,
            "ALGORITHM": algorithm,
            "ALGORITHM_PARAMETER": ALGORITHM_PARAMETER_,
            "SINGLE_MACHINE": 0,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }

        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"flash-mpijob-{algorithm}-multi.yaml")

        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)

            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")

            print(
                f"[INFO] Submitting MPIJob: {algorithm} multi-machine, machines={machines}"
            )
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run(
                [
                    kubectl,
                    "wait",
                    "--for=condition=Succeeded",
                    "mpijob/flash-mpijob",
                    "--timeout=10m",
                ],
                check=True,
            )
            dataset_name = os.path.basename(os.path.normpath(data_path))
            log_file = os.path.join(
                output_dir, f"flash_{algorithm}-{dataset_name}-n{machines}-p32.log"
            )
            subprocess.run(
                [kubectl, "logs", "job/flash-mpijob-launcher"],
                stdout=open(log_file, "w"),
                check=True,
            )
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    print("[INFO] ✅ All experiments completed.")


def run_ligra_perf(algorithm, data_path=None):
    THREAD_LIST = [1, 2, 4, 8, 16, 32]
    MEMORY = "100Gi"
    CPU = "32"
    yaml_dir = "config"
    template_path = os.path.join(yaml_dir, "ligra-mpijob-template.yaml")

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    if algorithm == "BellmanFord":
        VOLUMES_BLOCK = (
            f"volumes:\n          - name: ligra-data\n            hostPath:\n              path: {data_path}\n              type: File"
            if data_path
            else ""
        )
        VOLUME_MOUNTS_BLOCK = (
            f"volumeMounts:\n            - name: ligra-data\n              mountPath: /opt/data/graph_sssp.txt"
            if data_path
            else ""
        )
    else:
        VOLUMES_BLOCK = (
            f"volumes:\n          - name: ligra-data\n            hostPath:\n              path: {data_path}\n              type: File"
            if data_path
            else ""
        )
        VOLUME_MOUNTS_BLOCK = (
            f"volumeMounts:\n            - name: ligra-data\n              mountPath: /opt/data/graph.txt"
            if data_path
            else ""
        )

    try:
        with open(template_path, "r") as f:
            template_str = f.read()
    except FileNotFoundError:
        print(f"[ERROR] Template file not found: {template_path}")
        return

    for thread in THREAD_LIST:
        params = {
            "SLOTS_PER_WORKER": thread,
            "HOST_PATH": data_path,
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": 1,
            "MPIRUN_NP": thread,
            "ALGORITHM": algorithm,
            "SINGLE_MACHINE": 1,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }
        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"ligra-mpijob-{algorithm}-single.yaml")
        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")
            print(f"[INFO] Submitting Ligra MPIJob: {algorithm} threads={thread}")
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run(
                [
                    kubectl,
                    "wait",
                    "--for=condition=Succeeded",
                    "mpijob/ligra-mpijob",
                    "--timeout=10m",
                ],
                check=True,
            )
            log_file = os.path.join(output_dir, f"ligra_{algorithm}-n1-p{thread}.log")
            subprocess.run(
                [kubectl, "logs", "job/ligra-mpijob-launcher"],
                stdout=open(log_file, "w"),
                check=True,
            )
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    print("[INFO] ✅ Ligra experiments completed.")


def run_grape_perf(algorithm, data_path=None):
    THREAD_LIST = [1, 2, 4, 8, 16, 32]
    MACHINE_LIST = [2, 4, 8, 16]
    MEMORY = "100Gi"
    CPU = "32"
    yaml_dir = "config"
    template_path = os.path.join(yaml_dir, "grape-mpijob-template.yaml")

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    if algorithm == "sssp":
        VOLUMES_BLOCK = (
            f"volumes:\n          - name: grape-data-v\n            hostPath:\n              path: {data_path}.v\n              type: File\n          - name: grape-data-e\n            hostPath:\n              path: {data_path}.e\n              type: File"
            if data_path
            else ""
        )
        VOLUME_MOUNTS_BLOCK = (
            f"volumeMounts:\n            - name: grape-data-v\n              mountPath: /opt/data/graph_sssp.v\n            - name: grape-data-e\n              mountPath: /opt/data/graph_sssp.e"
            if data_path
            else ""
        )

    else:
        VOLUMES_BLOCK = (
            f"volumes:\n          - name: grape-data-v\n            hostPath:\n              path: {data_path}.v\n              type: File\n          - name: grape-data-e\n            hostPath:\n              path: {data_path}.e\n              type: File"
            if data_path
            else ""
        )
        VOLUME_MOUNTS_BLOCK = (
            f"volumeMounts:\n            - name: grape-data-v\n              mountPath: /opt/data/graph.v\n            - name: grape-data-e\n              mountPath: /opt/data/graph.e"
            if data_path
            else ""
        )

    try:
        with open(template_path, "r") as f:
            template_str = f.read()
    except FileNotFoundError:
        print(f"[ERROR] Template file not found: {template_path}")
        return

    for thread in THREAD_LIST:
        params = {
            "SLOTS_PER_WORKER": thread,
            "HOST_PATH_V": data_path + ".v",
            "HOST_PATH_E": data_path + ".e",
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": 1,
            "MPIRUN_NP": thread,
            "ALGORITHM": algorithm,
            "SINGLE_MACHINE": 1,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }
        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"grape-mpijob-{algorithm}-single.yaml")
        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")
            print(
                f"[INFO] Submitting MPIJob: {algorithm} single-machine, threads={thread}"
            )
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run(
                [
                    kubectl,
                    "wait",
                    "--for=condition=Succeeded",
                    "mpijob/grape-mpijob",
                    "--timeout=10m",
                ],
                check=True,
            )
            log_file = os.path.join(output_dir, f"grape_{algorithm}-n1-p{thread}.log")
            subprocess.run(
                [kubectl, "logs", "job/grape-mpijob-launcher"],
                stdout=open(log_file, "w"),
                check=True,
            )
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    for machines in MACHINE_LIST:
        params = {
            "SLOTS_PER_WORKER": 32,
            "HOST_PATH_V": data_path + ".v",
            "HOST_PATH_E": data_path + ".e",
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": machines,
            "MPIRUN_NP": machines * 32,
            "ALGORITHM": algorithm,
            "SINGLE_MACHINE": 0,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }
        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"grape-mpijob-{algorithm}-multi.yaml")
        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")
            print(
                f"[INFO] Submitting MPIJob: {algorithm} multi-machine, machines={machines}"
            )
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run(
                [
                    kubectl,
                    "wait",
                    "--for=condition=Succeeded",
                    "mpijob/grape-mpijob",
                    "--timeout=10m",
                ],
                check=True,
            )
            log_file = os.path.join(
                output_dir, f"grape_{algorithm}-n{machines}-p32.log"
            )
            subprocess.run(
                [kubectl, "logs", "job/grape-mpijob-launcher"],
                stdout=open(log_file, "w"),
                check=True,
            )
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    print("[INFO] ✅ Grape experiments completed.")


def run_pregel_perf(algorithm, data_path=None):
    THREAD_LIST = [1, 2, 4, 8, 16, 32]
    MACHINE_LIST = [2, 4, 8, 16]
    MEMORY = "100Gi"
    CPU = "32"
    yaml_dir = "config"
    template_path = os.path.join(yaml_dir, "pregel-mpijob-template.yaml")

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    VOLUMES_BLOCK = (
        f"- name: pregeldata\n              hostPath:\n                path: {data_path}\n                type: File"
        if data_path
        else ""
    )
    VOLUME_MOUNTS_BLOCK = (
        f"- name: pregeldata\n                  mountPath: /opt/data/graph.txt"
        if data_path
        else ""
    )

    for thread in THREAD_LIST:
        params = {
            "SLOTS_PER_WORKER": thread,
            "HOST_PATH": data_path,
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": 1,
            "MPIRUN_NP": thread,
            "ALGORITHM": algorithm,
            "SINGLE_MACHINE": 1,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }
        try:
            with open(template_path, "r") as f:
                template_str = f.read()
        except FileNotFoundError:
            print(f"[ERROR] Template file not found: {template_path}")
            return

        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"pregel-mpijob-{algorithm}-single.yaml")
        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")
            print(
                f"[INFO] Submitting Pregel+ MPIJob: {algorithm} single-machine, threads={thread}"
            )
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run(
                [
                    kubectl,
                    "wait",
                    "--for=condition=Succeeded",
                    "mpijob/pregel-mpijob",
                    "--timeout=10m",
                ],
                check=True,
            )
            log_file = os.path.join(output_dir, f"pregel_{algorithm}-n1-p{thread}.log")
            subprocess.run(
                [kubectl, "logs", "job/pregel-mpijob-launcher"],
                stdout=open(log_file, "w"),
                check=True,
            )
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    for machines in MACHINE_LIST:
        params = {
            "SLOTS_PER_WORKER": 32,
            "HOST_PATH": data_path,
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": machines,
            "MPIRUN_NP": machines * 32,
            "ALGORITHM": algorithm,
            "SINGLE_MACHINE": 0,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }
        try:
            with open(template_path, "r") as f:
                template_str = f.read()
        except FileNotFoundError:
            print(f"[ERROR] Template file not found: {template_path}")
            return

        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"pregel-mpijob-{algorithm}-multi.yaml")
        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")
            print(
                f"[INFO] Submitting Pregel+ MPIJob: {algorithm} multi-machine, machines={machines}"
            )
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run(
                [
                    kubectl,
                    "wait",
                    "--for=condition=Succeeded",
                    "mpijob/pregel-mpijob",
                    "--timeout=10m",
                ],
                check=True,
            )
            log_file = os.path.join(
                output_dir, f"pregel_{algorithm}-n{machines}-p32.log"
            )
            subprocess.run(
                [kubectl, "logs", "job/pregel-mpijob-launcher"],
                stdout=open(log_file, "w"),
                check=True,
            )
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    print("[INFO] ✅ Pregel+ experiments completed.")


def run_gthinker_perf(algorithm, data_path=None):
    THREAD_LIST = [1, 2, 4, 8, 16, 32]
    MACHINE_LIST = [2, 4, 8, 16]
    MEMORY = "100Gi"
    CPU = "32"
    yaml_dir = "config"
    template_path = os.path.join(yaml_dir, "gthinker-mpijob-template.yaml")

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    VOLUMES_BLOCK = (
        f"- name: gthinkerdata\n              hostPath:\n                path: {data_path}\n                type: File"
        if data_path
        else ""
    )
    VOLUME_MOUNTS_BLOCK = (
        f"- name: gthinkerdata\n                  mountPath: /opt/data/graph.txt"
        if data_path
        else ""
    )

    for thread in THREAD_LIST:
        params = {
            "SLOTS_PER_WORKER": thread,
            "HOST_PATH": data_path,
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": 1,
            "MPIRUN_NP": thread,
            "ALGORITHM": algorithm,
            "SINGLE_MACHINE": 1,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }
        try:
            with open(template_path, "r") as f:
                template_str = f.read()
        except FileNotFoundError:
            print(f"[ERROR] Template file not found: {template_path}")
            return

        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"gthinker-mpijob-{algorithm}-single.yaml")
        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")
            print(
                f"[INFO] Submitting Gthinker MPIJob: {algorithm} single-machine, threads={thread}"
            )
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run(
                [
                    kubectl,
                    "wait",
                    "--for=condition=Succeeded",
                    "mpijob/gthinker-mpijob",
                    "--timeout=10m",
                ],
                check=True,
            )
            log_file = os.path.join(
                output_dir, f"gthinker_{algorithm}-n1-p{thread}.log"
            )
            subprocess.run(
                [kubectl, "logs", "job/gthinker-mpijob-launcher"],
                stdout=open(log_file, "w"),
                check=True,
            )
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    for machines in MACHINE_LIST:
        params = {
            "SLOTS_PER_WORKER": 32,
            "HOST_PATH": data_path,
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": machines,
            "MPIRUN_NP": machines * 32,
            "ALGORITHM": algorithm,
            "SINGLE_MACHINE": 0,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }
        try:
            with open(template_path, "r") as f:
                template_str = f.read()
        except FileNotFoundError:
            print(f"[ERROR] Template file not found: {template_path}")
            return

        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"gthinker-mpijob-{algorithm}-multi.yaml")
        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")
            print(
                f"[INFO] Submitting Gthinker MPIJob: {algorithm} multi-machine, machines={machines}"
            )
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run(
                [
                    kubectl,
                    "wait",
                    "--for=condition=Succeeded",
                    "mpijob/gthinker-mpijob",
                    "--timeout=10m",
                ],
                check=True,
            )
            log_file = os.path.join(
                output_dir, f"gthinker_{algorithm}-n{machines}-p32.log"
            )
            subprocess.run(
                [kubectl, "logs", "job/gthinker-mpijob-launcher"],
                stdout=open(log_file, "w"),
                check=True,
            )
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    print("[INFO] ✅ Gthinker experiments completed.")


def run_powergraph_perf(algorithm, data_path=None):
    THREAD_LIST = [1, 2, 4, 8, 16, 32]
    MACHINE_LIST = [2, 4, 8, 16]
    MEMORY = "100Gi"
    CPU = "32"
    yaml_dir = "config"
    template_path = os.path.join(yaml_dir, "powergraph-mpijob-template.yaml")

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    VOLUMES_BLOCK = (
        f"- name: graphlabdata\n              hostPath:\n                path: {data_path}\n                type: File"
        if data_path
        else ""
    )
    VOLUME_MOUNTS_BLOCK = (
        f"- name: graphlabdata\n                  mountPath: /opt/data/graph.txt"
        if data_path
        else ""
    )

    for thread in THREAD_LIST:
        params = {
            "SLOTS_PER_WORKER": thread,
            "HOST_PATH": data_path,
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": 1,
            "MPIRUN_NP": thread,
            "ALGORITHM": algorithm,
            "SINGLE_MACHINE": 1,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }
        try:
            with open(template_path, "r") as f:
                template_str = f.read()
        except FileNotFoundError:
            print(f"[ERROR] Template file not found: {template_path}")
            return

        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"powergraph-mpijob-{algorithm}-single.yaml")
        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")
            print(
                f"[INFO] Submitting PowerGraph MPIJob: {algorithm} single-machine, threads={thread}"
            )
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run(
                [
                    kubectl,
                    "wait",
                    "--for=condition=Succeeded",
                    "mpijob/powergraph-mpijob",
                    "--timeout=10m",
                ],
                check=True,
            )
            log_file = os.path.join(
                output_dir, f"powergraph_{algorithm}-n1-p{thread}.log"
            )
            subprocess.run(
                [kubectl, "logs", "job/powergraph-mpijob-launcher"],
                stdout=open(log_file, "w"),
                check=True,
            )
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    for machines in MACHINE_LIST:
        params = {
            "SLOTS_PER_WORKER": 32,
            "HOST_PATH": data_path,
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": machines,
            "MPIRUN_NP": machines * 32,
            "ALGORITHM": algorithm,
            "SINGLE_MACHINE": 0,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }
        try:
            with open(template_path, "r") as f:
                template_str = f.read()
        except FileNotFoundError:
            print(f"[ERROR] Template file not found: {template_path}")
            return

        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"powergraph-mpijob-{algorithm}-multi.yaml")
        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")
            print(
                f"[INFO] Submitting PowerGraph MPIJob: {algorithm} multi-machine, machines={machines}"
            )
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run(
                [
                    kubectl,
                    "wait",
                    "--for=condition=Succeeded",
                    "mpijob/powergraph-mpijob",
                    "--timeout=10m",
                ],
                check=True,
            )
            log_file = os.path.join(
                output_dir, f"powergraph_{algorithm}-n{machines}-p32.log"
            )
            subprocess.run(
                [kubectl, "logs", "job/powergraph-mpijob-launcher"],
                stdout=open(log_file, "w"),
                check=True,
            )
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    print("[INFO] ✅ PowerGraph experiments completed.")


# --- Helper Functions ---
def run_command(command, working_dir="."):
    """Executes a shell command and prints its output."""
    print(f"▶️  Executing in '{working_dir}': {' '.join(command)}")
    try:
        # Using shell=False and a list of args is safer
        result = subprocess.run(
            command,
            cwd=working_dir,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        print("✅ Command executed successfully.")
        if result.stdout:
            print("--- STDOUT ---")
            print(result.stdout)
        if result.stderr:
            print("--- STDERR ---")
            print(result.stderr)
    except FileNotFoundError:
        print(
            f"❌ Error: Command '{command[0]}' not found. Make sure it's in your PATH or in the '{working_dir}' directory."
        )
    except subprocess.CalledProcessError as e:
        print(f"❌ Command failed with exit code: {e.returncode}")
        if e.stdout:
            print("--- STDOUT ---")
            print(e.stdout)
        if e.stderr:
            print("--- STDERR ---")
            print(e.stderr)
    except Exception as e:
        print(f"💥 An unexpected error occurred: {e}")


# --- Helper Functions for Performance Evaluation ---
def download_sample_datasets():
    """Download sample datasets if not already present."""
    dataset_urls = [
        "https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/sample_data.zip",
    ]

    # Check if sample_data folder exists; if so, skip download
    if os.path.exists("sample_data"):
        print("sample_data folder already exists. Skipping dataset download.")
        return

    for url in dataset_urls:
        filename = os.path.basename(url)
        file_path = os.path.join(filename)
        if not os.path.exists(file_path):
            print(f"Downloading dataset: {filename} ...")
            try:
                response = requests.get(url, stream=True)
                response.raise_for_status()
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"✅ Downloaded: {filename}")
                if filename.endswith(".zip"):
                    with zipfile.ZipFile(file_path, "r") as zip_ref:
                        zip_ref.extractall()
                    print(f"✅ Extracted: {filename}")
                    # Remove zip file after extraction
                    os.remove(file_path)
                    print(f"🗑️ Removed zip file: {filename}")
            except Exception as e:
                print(f"❌ Failed to download {filename}: {e}")
        else:
            print(f"Dataset already exists: {filename}")


def setup_graphx_files(platform_dir):
    """Download GraphX-specific jar and shell script files."""
    graphx_base_url = (
        "https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/"
    )
    graphx_files = [
        "pagerankexample_2.11-0.1.jar",
        "ssspexample_2.11-0.1.jar",
        "trianglecountingexample_2.11-0.1.jar",
        "labelpropagationexample_2.11-0.1.jar",
        "coreexample_2.11-0.1.jar",
        "connectedcomponentexample_2.11-0.1.jar",
        "betweennesscentralityexample_2.11-0.1.jar",
        "kcliqueexample_2.11-0.1.jar",
    ]
    graphx_sh_files = [
        "pagerank.sh",
        "sssp.sh",
        "trianglecounting.sh",
        "labelpropagation.sh",
        "core.sh",
        "connectedcomponent.sh",
        "betweennesscentrality.sh",
        "kclique.sh",
    ]

    # If GraphX directory does not exist, create it
    if not os.path.exists(platform_dir):
        print(f"GraphX directory '{platform_dir}' not found. Creating...")
        os.makedirs(platform_dir, exist_ok=True)

    # Download .jar files if missing
    for fname in graphx_files:
        fpath = os.path.join(platform_dir, fname)
        url = graphx_base_url + fname
        if not os.path.exists(fpath):
            print(f"Downloading GraphX jar: {fname} ...")
            try:
                resp = requests.get(url, stream=True)
                resp.raise_for_status()
                with open(fpath, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"✅ Downloaded: {fname}")
            except Exception as e:
                print(f"❌ Failed to download {fname}: {e}")
        else:
            print(f"Jar already exists: {fname}")

    # Download .sh files if missing
    for sh_file in graphx_sh_files:
        sh_path = os.path.join(platform_dir, sh_file)
        url = graphx_base_url + sh_file
        if not os.path.exists(sh_path):
            print(f"Downloading GraphX script: {sh_file} ...")
            try:
                resp = requests.get(url, stream=True)
                resp.raise_for_status()
                with open(sh_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"✅ Downloaded: {sh_file}")
            except Exception as e:
                print(f"❌ Failed to download {sh_file}: {e}")
        else:
            print(f"Script already exists: {sh_file}")


# --- Platform & Algorithm Definitions ---
PLATFORM_CONFIG = {
    "flash": {
        "dir": "Flash",
        "algos": {
            "pagerank": "pagerank",
            "sssp": "sssp",
            "triangle": "triangle",
            "lpa": "lpa",
            "cd": "k-core-search",
            "kclique": "clique",
            "cc": "cc",
            "bc": "bc",
        },
    },
    "ligra": {
        "dir": "Ligra",
        "algos": {
            "pagerank": "PageRank",
            "sssp": "BellmanFord",
            "bc": "BC",
            "kclique": "KCLIQUE",
            "cd": "KCore",
            "lpa": "LPA",
            "cc": "Components",
            "triangle": "Triangle",
        },
    },
    "grape": {
        "dir": "Grape",
        "algos": {
            "pagerank": "pagerank",
            "sssp": "sssp",
            "bc": "bc",
            "kclique": "kclique",
            "cd": "core_decomposition",
            "lpa": "cdlp",
            "cc": "wcc",
            "triangle": "lcc",
        },
    },
    "pregel+": {
        "dir": "Pregel+",
        "algos": {
            "pagerank": "pagerank",
            "sssp": "sssp",
            "bc": "betweenness",
            "lpa": "lpa",
            "kclique": "clique",
            "triangle": "triangle",
            "cc": "cc",
        },
    },
    "gthinker": {
        "dir": "Gthinker",
        "algos": {"kclique": "clique", "triangle": "triangle"},
    },
    "powergraph": {
        "dir": "PowerGraph",
        "algos": {
            "pagerank": "pagerank",
            "sssp": "sssp",
            "triangle": "triangle",
            "lpa": "lpa",
            "cd": "kcore",
            "cc": "cc",
            "bc": "betweenness",
        },
    },
    "graphx": {
        "dir": "GraphX",
        "algos": {
            "pagerank": "pagerank",
            "sssp": "sssp",
            "triangle": "triangle",
            "lpa": "lpa",
            "cd": "cd",
            "cc": "cc",
            "bc": "bc",
            "kclique": "kclique",
        },
    },
}


# --- CLI Subcommand Handlers ---
def data_generator(args):
    """Run the FFT-DG data generator."""
    scale = args.scale
    platform = args.platform
    feature = args.feature

    generator_dir = "Data_Generator"
    generator_exe = "./generator"

    if not os.path.exists(os.path.join(generator_dir, "FFT-DG.cpp")):
        print(f"'{generator_dir}/FFT-DG.cpp' not found. Downloading Data_Generator.zip...")
        url = "https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/Data_Generator.zip"
        try:
            r = requests.get(url)
            r.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                z.extractall(generator_dir)
            print("✅ Data_Generator.zip downloaded and extracted.")
        except Exception as e:
            print(f"Error downloading or extracting Data_Generator.zip: {e}")
            return

    if not os.path.exists(os.path.join(generator_dir, "generator")):
        compile_cmd = ["g++", "FFT-DG.cpp", "-o", "generator", "-O3"]
        run_command(compile_cmd, working_dir=generator_dir)

    run_cmd = [generator_exe, str(scale), platform, feature]
    run_command(run_cmd, working_dir=generator_dir)


def llm_evaluation(args):
    """Run the LLM-based usability evaluation."""
    platform = args.platform
    algorithm = args.algorithm

    # Ensure .env exists for API key loading
    if not os.path.exists(".env"):
        print("'.env' file not found. Copying from '.env.example'...")
        shutil.copy(".env.example", ".env")

    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key or api_key == "your_openai_api_key_here":
        print("❌ Error: OPENAI_API_KEY is not set in the .env file.")
        return

    docker_image = "graphanalysisbenchmarks/llm-eval:latest"

    # Get docker executable path for security
    docker = shutil.which("docker")
    if docker is None:
        print("❌ Error: docker command not found in PATH. Please install Docker.")
        return

    # Check if the docker image exists locally, if not, try to pull it
    try:
        result = subprocess.run(
            [docker, "image", "inspect", docker_image], capture_output=True
        )
        if result.returncode != 0:
            print(f"'{docker_image}' image not found locally. Pulling from Docker Hub...")
            subprocess.run([docker, "pull", docker_image], check=True)
            print(f"✅ '{docker_image}' image pulled successfully.")
    except Exception as e:
        print(f"Error checking or pulling Docker image '{docker_image}': {e}")
        return

    if platform and algorithm:
        cmd = [
            docker,
            "run",
            "--rm",
            "-e",
            f"OPENAI_API_KEY={api_key}",
            "-e",
            f"PLATFORM={platform}",
            "-e",
            f"ALGORITHM={algorithm}",
            docker_image,
        ]
        run_command(cmd)
    else:
        print("Error: Both --platform and --algorithm are required.")


def perf(args):
    """Run a performance benchmark for a specified platform and algorithm."""
    platform = args.platform.lower()
    algorithm = args.algorithm
    data_path = args.path
    spark_master = args.spark_master

    config = PLATFORM_CONFIG.get(platform)
    algos_map = config.get("algos", {})

    # Validate if the user's input is a valid standard algorithm name (a key in the map)
    if algorithm not in algos_map:
        print(
            f"❌ Error: Algorithm '{algorithm}' is not supported by platform '{platform}'."
        )
        print(
            f"Supported standard algorithms for '{platform}': {', '.join(algos_map.keys())}"
        )
        sys.exit(1)

    # Translate the standard name to the platform-specific name for execution
    platform_specific_algorithm = algos_map[algorithm]
    platform_dir = config["dir"]

    # Download sample datasets
    download_sample_datasets()

    if platform == "graphx":
        # Setup GraphX-specific files
        setup_graphx_files(platform_dir)

        if not spark_master:
            print("❌ Error: --spark-master is required for the 'graphx' platform.")
            return

        script_name_map = {
            "pagerank": "pagerank.sh",
            "sssp": "sssp.sh",
            "triangle": "trianglecounting.sh",
            "lpa": "labelpropagation.sh",
            "cd": "core.sh",
            "cc": "connectedcomponent.sh",
            "bc": "betweennesscentrality.sh",
            "kclique": "kclique.sh",
        }
        # The key 'algorithm' is the standard name from user input
        script_filename = script_name_map.get(algorithm)

        if not script_filename:
            print(
                f"Internal error: No script mapping for GraphX algorithm '{algorithm}'."
            )
            return

        script_path = os.path.join(platform_dir, script_filename)

        if not os.path.exists(script_path):
            print(f"❌ Error: Script '{script_path}' not found.")
            return

        # If data_path is None, use default sample graph according to algorithm
        if data_path is None:
            if algorithm == "pagerank":
                data_path = os.path.abspath("sample_data/graphx_sample_graph.txt")
            else:
                data_path = os.path.abspath(
                    "sample_data/graphx_weight_sample_graph.txt"
                )
        cmd = [f"./{script_filename}", spark_master, data_path]
        run_command(cmd, working_dir=platform_dir)

    # General handling for all other platforms
    else:
        if platform == "ligra":
            run_ligra_perf(platform_specific_algorithm, data_path)
        elif platform == "grape":
            run_grape_perf(platform_specific_algorithm, data_path)
        elif platform == "pregel+":
            run_pregel_perf(platform_specific_algorithm, data_path)
        elif platform == "gthinker":
            run_gthinker_perf(platform_specific_algorithm, data_path)
        elif platform == "powergraph":
            run_powergraph_perf(platform_specific_algorithm, data_path)
        elif platform == "flash":
            run_flash_perf(platform_specific_algorithm, data_path)


# --- CLI Main Entry ---
def main():
    """
    Unified CLI for Graph-Analytics-Benchmarks.

    Provides a single entry point for data generation, LLM usability evaluation,
    and cross-platform performance benchmarking.
    """
    parser = argparse.ArgumentParser(
        description="Unified CLI for Graph-Analytics-Benchmarks."
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # --- 1. datagen ---
    PLATFORM_CHOICES = ["flash", "ligra", "grape", "gthinker", "pregel+", "powergraph", "graphx"]
    datagen_parser = subparsers.add_parser("datagen", help="Run the FFT-DG data generator.")
    datagen_parser.add_argument("--scale", required=True, type=str, help="Dataset scale (e.g., 8, 9, 10).")
    datagen_parser.add_argument("--platform", required=True, type=str.lower, choices=PLATFORM_CHOICES, help="Target platform for output format.")
    datagen_parser.add_argument("--feature", required=True, type=str, choices=["Standard", "Density", "Diameter"], help="Dataset feature.")
    datagen_parser.set_defaults(func=data_generator)

    # --- 2. llm-eval ---
    llm_parser = subparsers.add_parser("llm-eval", help="Run the LLM-based usability evaluation.")
    llm_parser.add_argument("--platform", required=True, help="The target platform to evaluate (e.g., 'grape').")
    llm_parser.add_argument("--algorithm", required=True, help="The target algorithm to evaluate (e.g., 'pagerank').")
    llm_parser.set_defaults(func=llm_evaluation)

    # --- 3. perf-eval ---
    perf_parser = subparsers.add_parser("perf-eval", help="Run a performance benchmark.")
    perf_parser.add_argument("--platform", required=True, type=str.lower, choices=list(PLATFORM_CONFIG.keys()), help="The platform to run the benchmark on.")
    perf_parser.add_argument("--algorithm", required=True, type=str, help="The algorithm to run.")
    perf_parser.add_argument("--path", default=None, help="Path to the dataset file.")
    perf_parser.add_argument("--spark-master", default=None, help="Spark Master URL. Required only for GraphX platform.")
    perf_parser.set_defaults(func=perf)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
