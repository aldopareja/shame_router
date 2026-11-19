# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx", "sysv_ipc"]
# [tool.uv]
# no-binary-package = ["sysv_ipc"]                # built from source: the PyPI wheel may lack acquire() timeouts
# ///
"""scout_one.py HOST: watch one host and keep one sidecar per vLLM / SGLang server on it.

scout.py starts one per host. Every round: forget the sidecars that exited; then, if every running job on HOST belongs
to an opted-in user and one of them is new, requeued or still in its grace window, sweep every port of HOST and start
a sidecar for each server that has none. A job's grace window starts when the first full sweep after it appeared ends.
Each port check holds one of the node's slots, shared by every scout_one on the node (fleet_semaphore.py sets how
many)."""
import asyncio, json, os, socket, subprocess, sys, time
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from fleet_semaphore import SemState, open_slots, slot

HOST = sys.argv[1]
CLUSTER = os.environ["CLUSTER"]
ROUND = 300                                       # seconds between this host's rounds
GRACE = 60 * 60                                   # seconds a job's host keeps being swept after its first full sweep
PROBE_TIMEOUT = 3                                 # seconds per connection attempt or HTTP request
IN_FLIGHT = 1024                                  # port checks at once on this host, as the node's slots allow
ENGINES = {"vllm", "vllm-frontend-rs", "sglang"}  # owned_by in /v1/models; routers and gateways say something else
# Swept first: the ranges where servers turned up most often. Every other port follows, so none is skipped.
FIRST_PORTS = [range(8000, 9001), range(30000, 30300), range(19100, 19200), range(23000, 24000), range(31000, 31200),
               range(32000, 32100), range(33000, 40000, 1000), range(15000, 15100), range(17500, 17600),
               range(18000, 18100), range(39100, 39200)]
PORTS = list(dict.fromkeys([port for ports in FIRST_PORTS for port in ports] + list(range(1024, 65536))))
SIDECAR = Path(__file__).with_name("sidecar.py")
LOG = Path(__file__).parents[1] / "logs" / f"{CLUSTER}.jsonl"
OPTED_IN = Path(__file__).parents[1] / "opted_in.txt"   # one username per line; read every round, so edits apply live
CLIENT = httpx.AsyncClient(timeout=PROBE_TIMEOUT, trust_env=False)


@dataclass
class SeenJob:
    started: str                                  # Slurm start time: a requeued job keeps its id but not this
    user: str                                     # whose job: only opted-in users' jobs make HOST due
    since: float | None = None                    # start of its grace window; None until a full sweep ends


@dataclass
class State:
    jobs: dict[str, SeenJob] = field(default_factory=dict)                         # running jobs on HOST, by id
    sidecars: dict[str, asyncio.subprocess.Process] = field(default_factory=dict)  # server URL -> its sidecar
    slots: SemState = field(default_factory=open_slots)                            # this host's share of the slots


def log(event, url="", **fields):
    """Append one event to LOG, as scout.py does."""
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a") as f:
        f.write(json.dumps({"time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "cluster": CLUSTER, "url": url,
                            "event": event, "node": HOST, **fields}) + "\n")


def due(state: State) -> bool:
    """Whether HOST needs a sweep: every running job on it belongs to a user in OPTED_IN, and one of them is new,
    requeued, or in its grace window. Every job counts, not only opted-in ones: a sweep finds every server on HOST and
    can't tell whose job each belongs to, so one job of anyone else keeps HOST out. Updates state.jobs on the way, and
    logs each new or requeued job once (with its user and name, so later lines carry only ids)."""
    opted_in = set(OPTED_IN.read_text().split())
    out = subprocess.run(["squeue", "-h", "-t", "R", "-w", HOST, "-O", "JobID:|,StartTime:|,UserName:|,Name:"],
                         capture_output=True, text=True, check=True).stdout
    jobs = {}
    for line in out.splitlines():
        job, started, user, job_name = line.split("|", 3)          # Name last: it may hold "|"
        seen = state.jobs.get(job)
        if seen is None or seen.started != started:
            seen = SeenJob(started, user)
            log("job", job=job, user=user, job_name=job_name, started=started)
        jobs[job] = seen
    state.jobs = jobs
    return (all(seen.user in opted_in for seen in jobs.values())
            and any(seen.since is None or time.time() - seen.since < GRACE for seen in jobs.values()))


async def run_limited(fn, items, limit):
    """Await fn(item) for every item, at most limit at once, in items' order."""
    items = iter(items)
    async def worker():
        for item in items:
            await fn(item)
    await asyncio.gather(*(worker() for _ in range(limit)))


async def get_json(url: str) -> tuple[int, object] | None:
    """GET url within PROBE_TIMEOUT: (status, its JSON, or None if the body isn't JSON), or None when nothing answers
    in HTTP."""
    try:
        answer = await CLIENT.get(url)
    except httpx.HTTPError:
        return None
    try:
        return answer.status_code, answer.json()
    except ValueError:
        return answer.status_code, None


async def server_url(ip: str, port: int) -> str | None:
    """http://HOST:port if it lists a model served by vLLM or SGLang and isn't a router, else None. It connects first,
    so a closed port costs one refused connection. A port that answers HTTP but isn't a server is logged, so a miss can
    be explained; one that doesn't speak HTTP isn't, since that's most open ports (NCCL, ZMQ, ...)."""
    try:
        _, writer = await asyncio.wait_for(asyncio.open_connection(ip, port), PROBE_TIMEOUT)
    except OSError:                               # refused, unreachable, or timed out
        return None
    writer.close()
    url = f"http://{HOST}:{port}"
    models = await get_json(f"{url}/v1/models")
    if models is None:                            # open, but no HTTP answer within PROBE_TIMEOUT
        return None
    try:
        owned_by = models[1]["data"][0]["owned_by"] if models[0] == 200 else None
    except (KeyError, IndexError, TypeError):     # not an OpenAI-style model list, or an empty one
        owned_by = None
    if owned_by not in ENGINES:
        log("not_server", url, models=models[0], owned_by=owned_by)
        return None
    workers = await get_json(f"{url}/workers")    # a router passes its engines' owned_by on; smg lists its workers
    if workers and workers[0] == 200 and isinstance(workers[1], dict) and "workers" in workers[1]:
        log("router", url)
        return None
    return url


async def start_sidecar(state: State, url: str):
    """Start url's sidecar unless it has one. A sweep meets each URL once, so nothing can start it in between."""
    if url in state.sidecars:
        return
    process = state.sidecars[url] = await asyncio.create_subprocess_exec("uv", "run", "--script", str(SIDECAR), url)
    log("sidecar_start", url, pid=process.pid, jobs=list(state.jobs))


def reap(state: State):
    """Forget the sidecars that exited. Each exit restarts every job's grace window, so HOST gets swept again: its
    server may come back on another port."""
    for url, process in list(state.sidecars.items()):
        if process.returncode is not None:
            del state.sidecars[url]
            for seen in state.jobs.values():
                seen.since = None
            log("sidecar_exit", url, code=process.returncode)


async def sweep(state: State):
    """Every port of HOST, likely ones first, up to IN_FLIGHT at a time within the node's slots; a sidecar for each
    server found."""
    ip = socket.gethostbyname(HOST)               # once a sweep: asyncio would look the name up for every port
    async def scan_one(port):
        async with slot(state.slots):             # held for the check only, not while a sidecar starts
            url = await server_url(ip, port)
        if url:
            await start_sidecar(state, url)
    began = time.time()
    await run_limited(scan_one, PORTS, IN_FLIGHT)
    for seen in state.jobs.values():
        seen.since = seen.since or time.time()    # a grace window starts when the first full sweep ends
    log("swept", took=round(time.time() - began), sidecars=len(state.sidecars))


async def main():
    state = State()
    while True:
        reap(state)
        if due(state):
            try:
                await sweep(state)
            except socket.gaierror:               # Slurm knows HOST but DNS doesn't: nothing to reach it by
                log("unresolvable")
        await asyncio.sleep(ROUND)


if __name__ == "__main__":
    asyncio.run(main())
