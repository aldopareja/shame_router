#!/bin/bash
# scout_m2.sbatch: the scout job on m2. It runs cluster/scout.py, which starts one cluster/scout_one.py per node;
#   each finds the vLLM / SGLang servers on its node and keeps one sidecar per server as its child process. The
#   job's memory is mostly those processes: about 44 MB per node, and about 170 MB per serving sidecar.
#   Submit it from the repo root: Slurm runs a copy of this file but keeps the submit directory as the working
#   directory, and uv comes from the submitting shell's PATH. Stop it with scancel --name=shame-router-scout:
#   Slurm signals every process in the job, and the router drops the sidecars once its health checks fail (it
#   stops sending them requests after about 2.5 min and removes them after about 10).
#   Events go to logs/m2.jsonl; Slurm's own output (a crash's traceback) goes to slurm-<job id>.out in the repo
#   root.
#   SLOTS_TOTAL caps the port checks in flight across the node's scout_one processes (fleet_semaphore.py). A SysV
#   semaphore holds at most 32,767.
#   e.g.  sbatch cluster/scout_m2.sbatch
#SBATCH --job-name=shame-router-scout
#SBATCH --partition=main
#SBATCH --account=k2m
#SBATCH --qos=k2m
#SBATCH --cpus-per-task=20
#SBATCH --mem=128G
#SBATCH --time=UNLIMITED
CLUSTER=m2 SLOTS_TOTAL=32000 exec uv run --script cluster/scout.py
