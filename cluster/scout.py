# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx", "sysv_ipc"]
# [tool.uv]
# no-binary-package = ["sysv_ipc"]          # built from source: the PyPI wheel may lack acquire() timeouts
# ///
"""scout.py: the scout job's entry point. It starts one scout_one.py per node of this cluster, then waits.

Each scout_one.py watches its own node: it sweeps the node's ports while a job there is new or in its grace window, and
keeps one sidecar per vLLM / SGLang server it finds. The starts are spread over one round, so the first sweeps don't all
land at once. The children run on this script's Python, so they share its uv environment (httpx and sysv_ipc are
declared above for them) and no uv process is started per node.

Input: CLUSTER (m1 or m2) from the environment, set by scout_m1.sbatch / scout_m2.sbatch. Output: one JSON line per
event in logs/<cluster>.jsonl, shared with the scout_one processes and the sidecars. The node list is read once: a node
added later is watched from the next scout start. A scout_one that exits is logged, not restarted, so its sidecars are
never doubled. Stop it with `scancel --name=shame-router-scout`: Slurm signals every process in the job.
"""
import asyncio, json, os, subprocess, sys, time
from pathlib import Path

CLUSTER = os.environ["CLUSTER"]
SPREAD = 300                                                  # seconds the starts are spread over: scout_one's ROUND
SCOUT_ONE = Path(__file__).with_name("scout_one.py")
LOG = Path(__file__).parents[1] / "logs" / f"{CLUSTER}.jsonl"


def log(event, url="", **fields):
    """Append one event to LOG: time, cluster, url, event, then the event's own fields."""
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a") as f:                                  # opened for each line: the other processes append too
        f.write(json.dumps({"time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "cluster": CLUSTER, "url": url,
                            "event": event, **fields}) + "\n")


async def watch(host: str, process: asyncio.subprocess.Process):
    """Log host's scout_one when it exits, with its exit code."""
    log("scout_one_exit", node=host, code=await process.wait())


async def main():
    # Every node, once: sinfo lists a node once per partition it's in.
    hosts = sorted(set(subprocess.run(["sinfo", "-h", "-N", "-o", "%N"], capture_output=True, text=True,
                                      check=True).stdout.split()))
    log("scout_start", hosts=len(hosts))
    watchers = []
    for host in hosts:
        process = await asyncio.create_subprocess_exec(sys.executable, str(SCOUT_ONE), host)
        watchers.append(asyncio.create_task(watch(host, process)))
        await asyncio.sleep(SPREAD / len(hosts))              # e.g. 1,000 nodes: one start every 0.3 s
    await asyncio.gather(*watchers)


if __name__ == "__main__":
    asyncio.run(main())
