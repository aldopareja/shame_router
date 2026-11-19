#!/bin/bash
# scout_m1.sbatch: the scout job on m1. It runs cluster/scout.py, which starts one cluster/scout_one.py per node;
#   each finds the vLLM / SGLang servers on its node and keeps one sidecar per server as its child process. The
#   job's memory is mostly those processes: about 44 MB per node, and about 170 MB per serving sidecar.
#   Submit it from the repo root: Slurm runs a copy of this file but keeps the submit directory as the working
#   directory, and uv comes from the submitting shell's PATH. Stop it with scancel --name=shame-router-scout:
#   Slurm signals every process in the job, and the router drops the sidecars once its health checks fail (it
#   stops sending them requests after about 2.5 min and removes them after about 10).
#   Events go to logs/m1.jsonl; Slurm's own output (a crash's traceback) goes to slurm-<job id>.out in the repo
#   root.
#   SLOTS_TOTAL caps the port checks in flight across the node's scout_one processes (fleet_semaphore.py). Some
#   networks drop new connections under heavy probing, the scout node's own included: lower it if jobs on the scout's
#   node start failing to connect.
#   e.g.  sbatch cluster/scout_m1.sbatch
#SBATCH --job-name=shame-router-scout
#SBATCH --partition=main
#SBATCH --cpus-per-task=20
#SBATCH --mem=96G
#SBATCH --time=UNLIMITED
CLUSTER=m1 SLOTS_TOTAL=2500 exec uv run --script cluster/scout.py
