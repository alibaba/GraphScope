# main.py
import os
import subprocess
import click
from dotenv import load_dotenv
from config import render_mpijob_yaml
import tempfile
import shutil


# Load environment variables from .env file at the start
load_dotenv()

def run_flash_perf(algorithm, data_path):
    THREAD_LIST = [1, 2, 4, 8, 16, 32]
    MACHINE_LIST = [2, 4, 8, 16]
    DATASETS = ["Standard", "Density", "Diameter"]
    MEMORY = "100Gi"
    CPU = "32"
    yaml_dir = "kubectl-yaml"
    os.makedirs(yaml_dir, exist_ok=True)

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    if algorithm == "k-core-search":
        ALGORITHM_PARAMETER_ = 3
    elif algorithm == "clique":
        ALGORITHM_PARAMETER_ = 5
    else:
        ALGORITHM_PARAMETER_ = 0

    for dataset in DATASETS:
        if algorithm == "sssp":
            DATASET_NAME = f"flash-sssp-edges-8-{dataset}"
        else:
            DATASET_NAME = f"flash-edges-8-{dataset}"
        for thread in THREAD_LIST:
            params = {
                "job_name": "flash-mpijob",
                "SLOTS_PER_WORKER": thread,
                "HOST_PATH": data_path,
                "CPU": CPU,
                "MEMORY": MEMORY,
                "REPLICAS": 1,
                "MPIRUN_NP": thread,
                "DATASET": DATASET_NAME,
                "ALGORITHM": algorithm,
                "ALGORITHM_PARAMETER": ALGORITHM_PARAMETER_,
                "SINGLE_MACHINE": 1,
            }
            yaml_str = render_mpijob_yaml("flash", params)
            yaml_path = f"{yaml_dir}/{params['job_name']}-{algorithm}.yaml"
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            try:
                kubectl = shutil.which("kubectl")
                if kubectl is None:
                    raise RuntimeError("kubectl not found in PATH")
                else:
                    subprocess.run([kubectl, "-n", "hadoop", "delete", "-f", yaml_path], check=True)
                    print(f"[INFO] Submitting MPIJob: {algorithm} single-machine, threads={thread}")
                    subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
                    subprocess.run([kubectl, "wait", "--for=condition=Succeeded", "mpijob/flash-mpijob", "--timeout=100m"], check=True)
                    log_file = os.path.join(output_dir, f"flash_{algorithm}-{DATASET_NAME}-n1-p{thread}.log")
                    subprocess.run([kubectl, "logs", "job/flash-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
                    subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
            finally:
                pass
 

    for dataset in DATASETS:
        if algorithm == "sssp":
            DATASET_NAME = f"flash-sssp-edges-9-{dataset}"
        else:
            DATASET_NAME = f"flash-edges-9-{dataset}"
        for machines in MACHINE_LIST:
            params = {
                "job_name": "flash-mpijob",
                "SLOTS_PER_WORKER": 32,
                "HOST_PATH": data_path,
                "CPU": CPU,
                "MEMORY": MEMORY,
                "REPLICAS": machines,
                "MPIRUN_NP": machines * 32,
                "DATASET": DATASET_NAME,
                "ALGORITHM": algorithm,
                "ALGORITHM_PARAMETER": ALGORITHM_PARAMETER_,
                "SINGLE_MACHINE": 0,
            }
            yaml_str = render_mpijob_yaml("flash", params)
            
            yaml_path = f"{yaml_dir}/{params['job_name']}-{algorithm}.yaml"
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            try:
                kubectl = shutil.which("kubectl")
                if kubectl is None:
                    raise RuntimeError("kubectl not found in PATH")
                else:
                    print(f"[INFO] Submitting MPIJob: {algorithm} multi-machine, machines={machines}")
                    subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
                    subprocess.run([kubectl, "wait", "--for=condition=Succeeded", "mpijob/flash-mpijob", "--timeout=10m"], check=True)
                    log_file = os.path.join(output_dir, f"flash_{algorithm}-{DATASET_NAME}-n{machines}-p32.log")
                    subprocess.run([kubectl, "logs", "job/flash-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
                    subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
            finally:
                pass
                os.remove(yaml_path)

    print("[INFO] ✅ All experiments completed.")

def run_ligra_perf(algorithm, data_path):
    THREAD_LIST = [1, 2, 4, 8, 16, 32]
    DATASETS = ["Standard", "Density", "Diameter"]
    MEMORY = "100Gi"
    CPU = "32"
    yaml_dir = "kubectl-yaml"
    os.makedirs(yaml_dir, exist_ok=True)

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    for dataset in DATASETS:
        if algorithm == "BellmanFord":
            DATASET_NAME = f"ligra-sssp-adj-8-{dataset}.txt"
        else:
            DATASET_NAME = f"ligra-adj-8-{dataset}.txt"
        for thread in THREAD_LIST:
            params = {
                "job_name": "ligra-mpijob",
                "SLOTS_PER_WORKER": thread,
                "HOST_PATH": data_path,
                "CPU": CPU,
                "MEMORY": MEMORY,
                "REPLICAS": 1,
                "MPIRUN_NP": thread,
                "DATASET": DATASET_NAME,
                "ALGORITHM": algorithm,
                "SINGLE_MACHINE": 1,
            }
            yaml_str = render_mpijob_yaml("ligra", params)
            
            yaml_path = f"{yaml_dir}/{params['job_name']}-{algorithm}.yaml"
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            try:
                kubectl = shutil.which("kubectl")
                if kubectl is None:
                    raise RuntimeError("kubectl not found in PATH")
                else:
                    print(f"[INFO] Submitting Ligra MPIJob: {algorithm} threads={thread}")
                    subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
                    subprocess.run([kubectl, "wait", "--for=condition=Succeeded", "mpijob/ligra-mpijob", "--timeout=10m"], check=True)
                    log_file = os.path.join(output_dir, f"ligra_{algorithm}-{DATASET_NAME}-n1-p{thread}.log")
                    subprocess.run([kubectl, "logs", "job/ligra-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
                    subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
            finally:
                pass

    print("[INFO] ✅ Ligra experiments completed.")

def run_grape_perf(algorithm, data_path):
    THREAD_LIST = [1, 2, 4, 8, 16, 32]
    MACHINE_LIST = [2, 4, 8, 16]
    DATASETS = ["Standard", "Density", "Diameter"]
    MEMORY = "100Gi"
    CPU = "32"
    yaml_dir = "kubectl-yaml"
    os.makedirs(yaml_dir, exist_ok=True)

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    for dataset in DATASETS:
        if algorithm == "sssp":
            DATASET_NAME = f"grape-sssp-edges-8-{dataset}"
        else:
            DATASET_NAME = f"grape-edges-8-{dataset}"
        for thread in THREAD_LIST:
            params = {
                "job_name": "grape-mpijob",
                "SLOTS_PER_WORKER": thread,
                "HOST_PATH": data_path,
                "CPU": CPU,
                "MEMORY": MEMORY,
                "REPLICAS": 1,
                "MPIRUN_NP": thread,
                "DATASET": DATASET_NAME,
                "ALGORITHM": algorithm,
                "SINGLE_MACHINE": 1,
            }
            yaml_str = render_mpijob_yaml("grape", params)
            yaml_path = f"{yaml_dir}/{params['job_name']}-{algorithm}.yaml"
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            try:
                kubectl = shutil.which("kubectl")
                if kubectl is None:
                    raise RuntimeError("kubectl not found in PATH")
                else:
                    print(f"[INFO] Submitting Grape MPIJob: {algorithm} single-machine, threads={thread}")
                    subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
                    subprocess.run([kubectl, "wait", "--for=condition=Succeeded", "mpijob/grape-mpijob", "--timeout=10m"], check=True)
                    log_file = os.path.join(output_dir, f"grape_{algorithm}-{DATASET_NAME}-n1-p{thread}.log")
                    subprocess.run([kubectl, "logs", "job/grape-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
                    subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
            finally:
                pass


    for dataset in DATASETS:
        if algorithm == "sssp":
            DATASET_NAME = f"grape-sssp-edges-9-{dataset}"
        else:
            DATASET_NAME = f"grape-edges-9-{dataset}"
        for machines in MACHINE_LIST:
            params = {
                "job_name": "grape-mpijob",
                "SLOTS_PER_WORKER": 32,
                "HOST_PATH": data_path,
                "CPU": CPU,
                "MEMORY": MEMORY,
                "REPLICAS": machines,
                "MPIRUN_NP": machines * 32,
                "DATASET": DATASET_NAME,
                "ALGORITHM": algorithm,
                "SINGLE_MACHINE": 0,
            }
            yaml_str = render_mpijob_yaml("grape", params)
            yaml_path = f"{yaml_dir}/{params['job_name']}-{algorithm}.yaml"
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            try:
                kubectl = shutil.which("kubectl")
                if kubectl is None:
                    raise RuntimeError("kubectl not found in PATH")
                else:
                    print(f"[INFO] Submitting Grape MPIJob: {algorithm} multi-machine, machines={machines}")
                    subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
                    subprocess.run([kubectl, "wait", "--for=condition=Succeeded", "mpijob/grape-mpijob", "--timeout=10m"], check=True)
                    log_file = os.path.join(output_dir, f"grape_{algorithm}-{DATASET_NAME}-n{machines}-p32.log")
                    subprocess.run([kubectl, "logs", "job/grape-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
                    subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
            finally:
                pass

    print("[INFO] ✅ Grape experiments completed.")

def run_pregel_perf(algorithm, data_path):
    THREAD_LIST = [1, 2, 4, 8, 16, 32]
    MACHINE_LIST = [2, 4, 8, 16]
    DATASETS = ["Standard", "Density", "Diameter"]
    MEMORY = "100Gi"
    CPU = "32"
    yaml_dir = "kubectl-yaml"
    os.makedirs(yaml_dir, exist_ok=True)

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    for dataset in DATASETS:
        if algorithm == "sssp":
            DATASET_NAME = f"pregel+-adj-8-{dataset}.txt"
        else:
            DATASET_NAME = f"pregel+-adj-8-{dataset}.txt"
        for thread in THREAD_LIST:
            params = {
                "job_name": "pregel-mpijob",
                "SLOTS_PER_WORKER": thread,
                "HOST_PATH": data_path,
                "CPU": CPU,
                "MEMORY": MEMORY,
                "REPLICAS": 1,
                "MPIRUN_NP": thread,
                "DATASET": DATASET_NAME,
                "ALGORITHM": algorithm,
                "SINGLE_MACHINE": 1,
            }
            yaml_str = render_mpijob_yaml("pregel+", params)
            yaml_path = f"{yaml_dir}/{params['job_name']}-{algorithm}.yaml"
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            try:
                kubectl = shutil.which("kubectl")
                if kubectl is None:
                    raise RuntimeError("kubectl not found in PATH")
                else:
                    print(f"[INFO] Submitting Pregel+ MPIJob: {algorithm} single-machine, threads={thread}")
                    subprocess.run([kubectl, "-n", "hadoop", "apply", "-f", yaml_path], check=True)
                    subprocess.run([kubectl, "-n", "hadoop", "wait", "--for=condition=Succeeded", "mpijob/pregel-mpijob", "--timeout=10m"], check=True)
                    log_file = os.path.join(output_dir, f"pregel_{algorithm}-{DATASET_NAME}-n1-p{thread}.log")
                    subprocess.run([kubectl, "-n", "hadoop", "logs", "job/pregel-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
                    subprocess.run([kubectl, "-n", "hadoop", "delete", "-f", yaml_path], check=True)
            finally:
                pass


    for dataset in DATASETS:
        if algorithm == "sssp":
            DATASET_NAME = f"pregel+-adj-9-{dataset}.txt"
        else:
            DATASET_NAME = f"pregel+-adj-9-{dataset}.txt"
        for machines in MACHINE_LIST:
            params = {
                "job_name": "pregel-mpijob",
                "SLOTS_PER_WORKER": 32,
                "HOST_PATH": data_path,
                "CPU": CPU,
                "MEMORY": MEMORY,
                "REPLICAS": machines,
                "MPIRUN_NP": machines * 32,
                "DATASET": DATASET_NAME,
                "ALGORITHM": algorithm,
                "SINGLE_MACHINE": 0,
            }
            yaml_str = render_mpijob_yaml("pregel+", params)
            yaml_path = f"{yaml_dir}/{params['job_name']}-{algorithm}.yaml"
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            try:
                kubectl = shutil.which("kubectl")
                if kubectl is None:
                    raise RuntimeError("kubectl not found in PATH")
                else:
                    print(f"[INFO] Submitting Pregel+ MPIJob: {algorithm} multi-machine, machines={machines}")
                    subprocess.run([kubectl, "-n", "hadoop", "apply", "-f", yaml_path], check=True)
                    subprocess.run([kubectl, "-n", "hadoop", "wait", "--for=condition=Succeeded", "mpijob/pregel-mpijob", "--timeout=10m"], check=True)
                    log_file = os.path.join(output_dir, f"pregel_{algorithm}-{DATASET_NAME}-n{machines}-p32.log")
                    subprocess.run([kubectl, "-n", "hadoop", "logs", "job/pregel-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
                    subprocess.run([kubectl, "-n", "hadoop", "delete", "-f", yaml_path], check=True)
            finally:
                pass

    print("[INFO] ✅ Pregel+ experiments completed.")

def run_gthinker_perf(algorithm, data_path):
    THREAD_LIST = [1, 2, 4, 8, 16, 32]
    MACHINE_LIST = [2, 4, 8, 16]
    DATASETS = ["Standard", "Density", "Diameter"]
    MEMORY = "100Gi"
    CPU = "32"
    yaml_dir = "kubectl-yaml"
    os.makedirs(yaml_dir, exist_ok=True)

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    for dataset in DATASETS:
        if algorithm == "sssp":
            DATASET_NAME = f"gthinker-adj-8-{dataset}.txt"
        else:
            DATASET_NAME = f"gthinker-adj-8-{dataset}.txt"
        for thread in THREAD_LIST:
            params = {
                "job_name": "gthinker-mpijob",
                "SLOTS_PER_WORKER": thread,
                "HOST_PATH": data_path,
                "CPU": CPU,
                "MEMORY": MEMORY,
                "REPLICAS": 1,
                "MPIRUN_NP": thread,
                "DATASET": DATASET_NAME,
                "ALGORITHM": algorithm,
                "SINGLE_MACHINE": 1,
            }
            yaml_str = render_mpijob_yaml("gthinker", params)
            yaml_path = f"{yaml_dir}/{params['job_name']}-{algorithm}.yaml"
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            try:
                kubectl = shutil.which("kubectl")
                if kubectl is None:
                    raise RuntimeError("kubectl not found in PATH")
                else:
                    print(f"[INFO] Submitting Gthinker MPIJob: {algorithm} single-machine, threads={thread}")
                    subprocess.run([kubectl, "-n", "hadoop", "apply", "-f", yaml_path], check=True)
                    subprocess.run([kubectl, "-n", "hadoop", "wait", "--for=condition=Succeeded", "mpijob/gthinker-mpijob", "--timeout=10m"], check=True)
                    log_file = os.path.join(output_dir, f"gthinker_{algorithm}-{DATASET_NAME}-n1-p{thread}.log")
                    subprocess.run([kubectl, "-n", "hadoop", "logs", "job/gthinker-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
                    subprocess.run([kubectl, "-n", "hadoop", "delete", "-f", yaml_path], check=True)
            finally:
                pass


    for dataset in DATASETS:
        if algorithm == "sssp":
            DATASET_NAME = f"gthinker-adj-9-{dataset}.txt"
        else:
            DATASET_NAME = f"gthinker-adj-9-{dataset}.txt"
        for machines in MACHINE_LIST:
            params = {
                "job_name": "gthinker-mpijob",
                "SLOTS_PER_WORKER": 32,
                "HOST_PATH": data_path,
                "CPU": CPU,
                "MEMORY": MEMORY,
                "REPLICAS": machines,
                "MPIRUN_NP": machines * 32,
                "DATASET": DATASET_NAME,
                "ALGORITHM": algorithm,
                "SINGLE_MACHINE": 0,
            }
            yaml_str = render_mpijob_yaml("gthinker", params)
            yaml_path = f"{yaml_dir}/{params['job_name']}-{algorithm}.yaml"
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            try:
                kubectl = shutil.which("kubectl")
                if kubectl is None:
                    raise RuntimeError("kubectl not found in PATH")
                else:
                    print(f"[INFO] Submitting Gthinker MPIJob: {algorithm} multi-machine, machines={machines}")
                    subprocess.run([kubectl, "-n", "hadoop", "apply", "-f", yaml_path], check=True)
                    subprocess.run([kubectl, "-n", "hadoop", "wait", "--for=condition=Succeeded", "mpijob/gthinker-mpijob", "--timeout=10m"], check=True)
                    log_file = os.path.join(output_dir, f"gthinker_{algorithm}-{DATASET_NAME}-n{machines}-p32.log")
                    subprocess.run([kubectl, "-n", "hadoop", "logs", "job/gthinker-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
                    subprocess.run([kubectl, "-n", "hadoop", "delete", "-f", yaml_path], check=True)
            finally:
                pass

    print("[INFO] ✅ Gthinker experiments completed.")

def run_powergraph_perf(algorithm, data_path):
    THREAD_LIST = [1, 2, 4, 8, 16, 32]
    MACHINE_LIST = [2, 4, 8, 16]
    DATASETS = ["Standard", "Diameter", "Density"]
    MEMORY = "100Gi"
    CPU = "32"
    yaml_dir = "kubectl-yaml"
    os.makedirs(yaml_dir, exist_ok=True)

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    for dataset in DATASETS:
        DATASET_NAME = f"graphlab-adj-8-{dataset}.txt"
        for thread in THREAD_LIST:
            params = {
                "job_name": "graphlab-mpijob",
                "SLOTS_PER_WORKER": thread,
                "HOST_PATH": data_path,
                "CPU": CPU,
                "MEMORY": MEMORY,
                "REPLICAS": 1,
                "MPIRUN_NP": thread,
                "DATASET": DATASET_NAME,
                "ALGORITHM": algorithm,
                "SINGLE_MACHINE": 1,
            }
            yaml_str = render_mpijob_yaml("powergraph", params)
            yaml_path = f"{yaml_dir}/{params['job_name']}-{algorithm}.yaml"
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            try:
                kubectl = shutil.which("kubectl")
                if kubectl is None:
                    raise RuntimeError("kubectl not found in PATH")
                else:
                    print(f"[INFO] Submitting PowerGraph MPIJob: {algorithm} single-machine, threads={thread}")
                    subprocess.run([kubectl, "-n", "hadoop", "apply", "-f", yaml_path], check=True)
                    subprocess.run([kubectl, "-n", "hadoop", "wait", "--for=condition=Succeeded", "mpijob/graphlab-mpijob", "--timeout=10m"], check=True)
                    log_file = os.path.join(output_dir, f"powergraph_{algorithm}-{DATASET_NAME}-n1-p{thread}.log")
                    subprocess.run([kubectl, "-n", "hadoop", "logs", "job/graphlab-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
                    subprocess.run([kubectl, "-n", "hadoop", "delete", "-f", yaml_path], check=True)
            finally:
                pass


    for dataset in DATASETS:
        DATASET_NAME = f"graphlab-adj-9-{dataset}.txt"
        for machines in MACHINE_LIST:
            params = {
                "job_name": "graphlab-mpijob",
                "SLOTS_PER_WORKER": 32,
                "HOST_PATH": data_path,
                "CPU": CPU,
                "MEMORY": MEMORY,
                "REPLICAS": machines,
                "MPIRUN_NP": machines * 32,
                "DATASET": DATASET_NAME,
                "ALGORITHM": algorithm,
                "SINGLE_MACHINE": 0,
            }
            yaml_str = render_mpijob_yaml("powergraph", params)
            yaml_path = f"{yaml_dir}/{params['job_name']}-{algorithm}.yaml"
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            try:
                kubectl = shutil.which("kubectl")
                if kubectl is None:
                    raise RuntimeError("kubectl not found in PATH")
                else:
                    print(f"[INFO] Submitting PowerGraph MPIJob: {algorithm} multi-machine, machines={machines}")
                    subprocess.run([kubectl, "-n", "hadoop", "apply", "-f", yaml_path], check=True)
                    subprocess.run([kubectl, "-n", "hadoop", "wait", "--for=condition=Succeeded", "mpijob/graphlab-mpijob", "--timeout=10m"], check=True)
                    log_file = os.path.join(output_dir, f"powergraph_{algorithm}-{DATASET_NAME}-n{machines}-p32.log")
                    subprocess.run([kubectl, "-n", "hadoop", "logs", "job/graphlab-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
                    subprocess.run([kubectl, "-n", "hadoop", "delete", "-f", yaml_path], check=True)
            finally:
                os.remove(yaml_path) 

    print("[INFO] ✅ PowerGraph experiments completed.")

# --- Helper Functions ---
def run_command(command, working_dir="."):
    """Executes a shell command and prints its output."""
    click.echo(f"▶️  Executing in '{working_dir}': {' '.join(command)}")
    try:
        # Using shell=False and a list of args is safer
        result = subprocess.run(
            command,
            cwd=working_dir,
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8'
        )
        click.echo("✅ Command executed successfully.")
        if result.stdout:
            click.echo("--- STDOUT ---")
            click.echo(result.stdout)
        if result.stderr:
            click.echo("--- STDERR ---")
            click.echo(result.stderr)
    except FileNotFoundError:
        click.secho(
            f"❌ Error: Command '{command[0]}' not found. Make sure it's in your PATH or in the '{working_dir}' directory.",
            fg="red")
    except subprocess.CalledProcessError as e:
        click.secho(f"❌ Command failed with exit code: {e.returncode}", fg="red")
        if e.stdout:
            click.secho("--- STDOUT ---", fg="yellow")
            click.secho(e.stdout, fg="yellow")
        if e.stderr:
            click.secho("--- STDERR ---", fg="yellow")
            click.secho(e.stderr, fg="yellow")
    except Exception as e:
        click.secho(f"💥 An unexpected error occurred: {e}", fg="red")


# --- Platform & Algorithm Definitions ---
PLATFORM_CONFIG = {
    "flash": {"dir": "Flash", "algos": {"pagerank": "pagerank", "sssp": "sssp", "triangle": "triangle", "lpa": "lpa", "cd": "k-core-search", "kclique": "clique", "cc": "cc", "bc": "bc"}},
    "ligra": {"dir": "Ligra",
              "algos": {"pagerank": "PageRank", "sssp": "BellmanFord", "bc": "BC", "kclique": "KCLIQUE", "cd": "KCore", "lpa": "LPA", "cc": "Components", "triangle": "Triangle"}},
    "grape": {"dir": "Grape",
              "algos": {"pagerank": "pagerank", "sssp": "sssp", "bc": "bc", "kclique": "kclique", "cd": "core_decomposition", "lpa": "cdlp", "cc": "wcc", "triangle": "lcc"}},
    "pregel+": {"dir": "Pregel+", "algos": {"pagerank": "pagerank", "sssp": "sssp", "bc": "betweenness", "lpa": "lpa", "kclique": "clique", "triangle": "triangle", "cc": "cc"}},
    "gthinker": {"dir": "Gthinker", "algos": {"kclique": "clique", "triangle": "triangle"}},
    "powergraph": {"dir": "PowerGraph", "algos": {"pagerank": "pagerank", "sssp": "sssp", "triangle": "triangle", "lpa": "lpa", "cd": "kcore", "cc": "cc", "bc": "betweenness"}},
    "graphx": {"dir": "GraphX",
               "algos": {"pagerank": "pagerank", "sssp": "sssp", "triangle": "triangle", "lpa": "lpa", "cd": "cd", "cc": "cc", "bc": "bc", "kclique": "kclique"}}
}


# --- CLI Main Group ---
@click.group(context_settings=dict(help_option_names=['-h', '--help']))
def main():
    """
    Unified CLI for Graph-Analytics-Benchmarks.

    Provides a single entry point for data generation, LLM usability evaluation,
    and cross-platform performance benchmarking.
    """
    pass


# --- 1. Data Generator ---
@main.command(name="datagen", help="--scale <S> --platform <P> --feature <F>")
@click.option('--scale', required=True, type=str, help="Dataset scale (e.g., 8, 9, 10).")
@click.option('--platform', required=True, type=click.Choice(['flash', 'ligra', 'grape', 'gthinker', 'pregel+', 'powergraph', 'graphx'], case_sensitive=False),
              help="Target platform for output format.")
@click.option('--feature', required=True, type=click.Choice(['Standard', 'Density', 'Diameter'], case_sensitive=False),
              help="Dataset feature.")
def data_generator(scale, platform, feature, compile):
    """Run the FFT-DG data generator."""
    generator_dir = "Data_Generator"
    generator_exe = "./generator"

    if not os.path.exists(os.path.join(generator_dir, "FFT-DG.cpp")):
        click.secho(f"Error: '{generator_dir}/FFT-DG.cpp' not found. Please ensure Data_Generator.zip is unzipped.",
                    fg="red")
        return

    if not os.path.exists(os.path.join(generator_dir, "generator")):
        compile_cmd = ["g++", "FFT-DG.cpp", "-o", "generator", "-O3"]
        run_command(compile_cmd, working_dir=generator_dir)

    run_cmd = [generator_exe, str(scale), platform, feature]
    run_command(run_cmd, working_dir=generator_dir)


# --- 2. LLM-based Usability Evaluation ---
@main.command(name="llm-eval", help="--platform <P> --algorithm <A>")
@click.option('--platform', help="The target platform to evaluate (e.g., 'grape').")
@click.option('--algorithm', help="The target algorithm to evaluate (e.g., 'pagerank').")
def llm_evaluation(platform, algorithm):
    """Run the LLM-based usability evaluation."""
    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key or api_key == "your_openai_api_key_here":
        click.secho("❌ Error: OPENAI_API_KEY is not set in the .env file.", fg="red")
        return

    docker_image = "llm-eval"

    if platform and algorithm:
        cmd = ["docker", "run", "--rm", "-e", f"OPENAI_API_KEY={api_key}", "-e", f"PLATFORM={platform}", "-e",
               f"ALGORITHM={algorithm}", docker_image]
        run_command(cmd)
    else:
        click.secho("Error: Both --platform and --algorithm are required.", fg="red")


# --- 3. Performance Evaluation ---
@main.command(name="perf-eval", help="--platform <P> --algorithm <A> --path <D> --spark-master <M>")
@click.option('--platform', required=True, type=click.Choice(PLATFORM_CONFIG.keys(), case_sensitive=False),
              help="The platform to run the benchmark on.")
@click.option('--algorithm', required=True, type=str, help="The algorithm to run.")
@click.option('--path', 'data_path', required=True, type=click.Path(exists=True),
              help="Path to the dataset folder or file.")
@click.option('--spark-master', help="Spark Master URL. Required only for GraphX platform.")
def perf(platform, algorithm, data_path, spark_master):
    """Run a performance benchmark for a specified platform and algorithm."""

    platform = platform.lower()  # Normalize platform name
    config = PLATFORM_CONFIG.get(platform)
    algos_map = config.get('algos', {})


    # Validate if the user's input is a valid standard algorithm name (a key in the map)
    if algorithm not in algos_map.keys():
        click.secho(f"❌ Error: Algorithm '{algorithm}' is not supported by platform '{platform}'.", fg="red")
        click.echo(f"Supported standard algorithms for '{platform}': {', '.join(algos_map.keys())}")
        return

    # Translate the standard name to the platform-specific name for execution
    platform_specific_algorithm = algos_map[algorithm]

    platform_dir = config['dir']

    # Special handling for GraphX
    if platform == 'graphx':
        if not spark_master:
            click.secho("❌ Error: --spark-master is required for the 'graphx' platform.", fg="red")
            return

        script_name_map = {
            "pagerank": "pagerank.sh", "sssp": "sssp.sh", "triangle": "trianglecounting.sh",
            "lpa": "labelpropagation.sh", "cd": "core.sh", "cc": "connectedcomponent.sh",
            "bc": "betweennesscentrality.sh", "kclique": "kclique.sh"
        }
        # The key 'algorithm' is the standard name from user input
        script_filename = script_name_map.get(algorithm)

        if not script_filename:
            click.secho(f"Internal error: No script mapping for GraphX algorithm '{algorithm}'.", fg="red")
            return

        script_path = os.path.join(platform_dir, script_filename)

        if not os.path.exists(script_path):
            click.secho(f"❌ Error: Script '{script_path}' not found.", fg="red")
            return

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


if __name__ == "__main__":
    main()
