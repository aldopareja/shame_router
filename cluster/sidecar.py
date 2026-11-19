# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx", "smg"]
# ///
"""sidecar.py SERVER_URL: put one server behind the shame router while its owner's own traffic is low.

The scout starts one per host it finds a server on, as `uv run --script cluster/sidecar.py SERVER_URL`. SERVER_URL is
all it is given; the rest it works out or reads:
  CLUSTER        from the environment (the scout job sets it: m1 or m2)
  .env           the repo's one .env: GATEWAY_HOST and GATEWAY_TOKEN (relay.sh reads the tailscale keys itself)
  cluster/model_names_normalized.tsv   next to this file: normalized name <TAB> regex, read again on every check
Once the model maps to a name, it runs its own smg in front of the server (taking the normalized name and passing the
served id on) and tailscale_relay/relay.sh publishing that smg on the tailnet as sr-<cluster>-<node>-<port>, both for
the rest of its life. It joins and leaves the router by itself, through Caddy, and writes JSON lines to
logs/<cluster>.jsonl, shared with the scout.

It ends once the server has been gone GONE_CHECKS checks in a row, or on any error. Either way it leaves the router and
stops its smg and relay first, and the scout's later rounds find the server again and start a new sidecar. scancel
needs nothing here: Slurm signals every process in the job, smg and relay.sh included, and the router drops the dead
entry once its health checks fail.
"""
import asyncio, contextlib, json, os, re, signal, socket, sys, time, urllib.parse
from dataclasses import dataclass, field
from pathlib import Path

import httpx

CLUSTER = os.environ["CLUSTER"]
SERVER = sys.argv[1]                                          # the owner's server: http://<node>:<port>
REPO = Path(__file__).resolve().parents[1]                    # uv runs the file in place, so this is the repo root
NAMES = Path(__file__).with_name("model_names_normalized.tsv")
LOG = REPO / "logs" / f"{CLUSTER}.jsonl"
RELAY = REPO / "tailscale_relay" / "relay.sh"
SMG = Path(sys.executable).with_name("smg")                   # smg's command in this script's own uv environment

# The design's numbers.
CHECK_SECS = 15                                               # one check: health, owner's traffic, join or leave
LEAVE_AT, LEAVE_CHECKS = 8, 2                                 # leave at 8+ owner requests on 2 checks in a row
JOIN_AT, JOIN_CHECKS = 4, 4                                   # join at 4 or fewer on 4 in a row; it starts out
DOWN_CHECKS = 3                                               # 3 failed health checks in a row: leave the router
GONE_CHECKS = 40                                              # 40 in a row (10 min): the server's job is over
HEALTH_TIMEOUT = 30                                           # SGLang's /health generates a token: slow under load
# Running + waiting requests in a server's /metrics, every label set. Exact names: vLLM also exports
# vllm:num_requests_waiting_by_reason, which a prefix match would count a second time.
METRICS = re.compile(r"^(?:vllm:num_requests_running|vllm:num_requests_waiting|sglang:num_running_reqs|"
                     r"sglang:num_queue_reqs)(?:\{[^}]*\})? (\S+)$", re.MULTILINE)


def read_env() -> dict[str, str]:
    """The repo's .env as a dict, read the way bash reads its plain KEY=value lines. Its values are never logged."""
    lines = (line.partition("=") for line in (REPO / ".env").read_text().splitlines() if "=" in line)
    return {key.strip(): value.strip().strip("\"'") for key, _, value in lines if not key.startswith("#")}


ENV = read_env()
# Two clients. The server and this sidecar's smg are nodes of this cluster, so no proxy from the job's environment.
# The router is reached at its public name through Caddy, which checks the token and takes it off.
LOCAL = httpx.AsyncClient(timeout=10, trust_env=False)
ROUTER = httpx.AsyncClient(base_url=f"https://{ENV['GATEWAY_HOST']}", timeout=30,
                           headers={"Authorization": f"Bearer {ENV['GATEWAY_TOKEN']}"})


def log(event, **fields):
    """Append one event to LOG, as the scout does: time, cluster, url, event, then the event's own fields."""
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a") as f:                                  # one write per line, so the scout's and the other
        f.write(json.dumps({"time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "cluster": CLUSTER,
                            "url": SERVER, "event": event, **fields}) + "\n")   # sidecars' lines never mix


@dataclass
class Sidecar:
    """This sidecar's state, changed in place by check()."""
    served_id: str                                            # from /v1/models: what requests to the server must name
    path: str                                                 # the checkpoint path, when the server gives it
    name: str | None = None                                   # normalized name; None until a rule in NAMES matches
    smg_port: int = 0                                         # its own smg on 127.0.0.1
    tailnet_url: str = ""                                     # where the router reaches it, once relay.sh is up
    children: list[asyncio.subprocess.Process] = field(default_factory=list)   # smg and relay.sh
    inside: bool = False                                      # in the router
    busy: int = 0                                             # checks in a row with the owner at LEAVE_AT or more
    quiet: int = 0                                            # checks in a row with the owner at JOIN_AT or fewer
    misses: int = 0                                           # failed health checks in a row


async def identify() -> tuple[str, str]:
    """The server's served id and checkpoint path. vLLM gives the path as its model's root; SGLang puts the served id
    there and gives the path at /get_model_info, which vLLM doesn't have."""
    model = (await LOCAL.get(f"{SERVER}/v1/models")).json()["data"][0]
    info = await LOCAL.get(f"{SERVER}/get_model_info")
    return model["id"], info.json()["model_path"] if info.status_code == 200 else model.get("root", "")


def normalize(served_id: str, path: str) -> str | None:
    """The name of the first rule in NAMES whose regex matches the served id or the path, ignoring case, or None. NAMES
    is read on every call, so an edit takes effect at the next check without restarting anything."""
    for line in NAMES.read_text().splitlines():
        if line.strip() and not line.startswith("#"):
            name, regex = line.split("\t", 1)
            if any(re.search(regex, text, re.IGNORECASE) for text in (served_id, path) if text):
                return name
    return None


async def healthy() -> bool:
    """One health check of the server: /health answers 200 within HEALTH_TIMEOUT. No retries: DOWN_CHECKS is the
    retry."""
    try:
        return (await LOCAL.get(f"{SERVER}/health", timeout=HEALTH_TIMEOUT)).status_code == 200
    except httpx.HTTPError:
        return False


async def engine_requests() -> int:
    """The server's running + waiting requests, whoever sent them: from /metrics (vLLM, and SGLang started with
    metrics on), else SGLang's /get_load. A server that shows neither raises, which ends the sidecar: a server whose
    owner can't be watched is never shared."""
    answer = await LOCAL.get(f"{SERVER}/metrics")
    found = METRICS.findall(answer.text) if answer.status_code == 200 else []
    if found:
        return round(sum(map(float, found)))
    # One entry per data-parallel rank. If num_reqs already includes the waiting ones, they count twice here, which
    # errs toward leaving.
    return sum(rank["num_reqs"] + rank["num_waiting_reqs"] for rank in (await LOCAL.get(f"{SERVER}/get_load")).json())


async def own_requests(sidecar: Sidecar) -> int:
    """Requests in flight through this sidecar: the load its smg counts on its one worker."""
    return (await LOCAL.get(f"http://127.0.0.1:{sidecar.smg_port}/workers")).json()["workers"][0]["load"]


def free_port() -> int:
    """A port nothing on this node listens on right now."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def poll(ask, what: str):
    """ask()'s first non-empty answer, asked once a second; RuntimeError after 2 min."""
    for _ in range(120):
        if answer := await ask():
            return answer
        await asyncio.sleep(1)
    raise RuntimeError(f"{what} not ready after 2 min")


async def smg_workers(port: int) -> list:
    """The workers this sidecar's smg lists, or [] while it is still starting."""
    try:
        return (await LOCAL.get(f"http://127.0.0.1:{port}/workers")).json()["workers"]
    except (httpx.HTTPError, ValueError, KeyError):
        return []


async def tailnet_name(device: str) -> str:
    """The device's tailnet name once relay.sh's tailscale has joined (Self.DNSName, without its final dot), or ""."""
    folder = Path(f"/tmp/tsrelay-{device}")                   # where relay.sh keeps its tailscale and socket
    if not (folder / "sock").exists():
        return ""
    process = await asyncio.create_subprocess_exec(str(folder / "tailscale"), f"--socket={folder / 'sock'}", "status",
                                                   "--json", stdout=asyncio.subprocess.PIPE,
                                                   stderr=asyncio.subprocess.DEVNULL)
    out, _ = await process.communicate()
    return ((json.loads(out or "{}").get("Self") or {}).get("DNSName") or "").rstrip(".")


async def serve(sidecar: Sidecar):
    """Start this sidecar's smg in front of the server, publish it on the tailnet with relay.sh, and wait for both.
    Each child leads its own process group, so stopping it stops what it started (relay.sh's tailscaled)."""
    parts = urllib.parse.urlsplit(SERVER)
    device = f"sr-{CLUSTER}-{parts.hostname.split('.')[0]}-{parts.port}".lower()
    output = (REPO / "logs" / f"{device}.log").open("ab")     # both children's output, kept out of the job's
    sidecar.smg_port = free_port()
    sidecar.children.append(await asyncio.create_subprocess_exec(
        str(SMG), "launch", "--host", "127.0.0.1", "--port", str(sidecar.smg_port),
        "--prometheus-host", "127.0.0.1", "--prometheus-port", str(free_port()),   # sidecars share nodes
        "--worker-urls", SERVER,
        # Takes the name and sends the served id on. smg refuses an alias equal to the served id: none when they match.
        # The default policy, not round_robin: with --disable-retries, round_robin skips the rename (smg 1.13-1.15).
        *(["--model-alias", f"{sidecar.name}={sidecar.served_id}"] if sidecar.name != sidecar.served_id else []),
        "--disable-health-check",                             # this sidecar checks the server itself, leniently
        "--disable-retries", "--disable-circuit-breaker",     # the router does both
        "--request-timeout-secs", "7200", "--max-payload-size", "1073741824",      # the router's long-call limits
        "--idle-connection-timeout-secs", "0",                # as on the router: the caller closes idle connections
        "--log-level", "warn",
        stdout=output, stderr=output, start_new_session=True))
    await poll(lambda: smg_workers(sidecar.smg_port), "smg")
    sidecar.children.append(await asyncio.create_subprocess_exec(
        "bash", str(RELAY), device, "80", f"127.0.0.1:{sidecar.smg_port}",
        stdout=output, stderr=output, start_new_session=True))
    sidecar.tailnet_url = f"http://{await poll(lambda: tailnet_name(device), 'relay.sh')}:80"


async def join(sidecar: Sidecar):
    """Add this sidecar to the router, or confirm it's there: smg answers 409 to a URL it already lists. Called on
    every check while in, so a sidecar the router dropped comes back at the next check."""
    body = {"url": sidecar.tailnet_url, "models": [{"id": sidecar.name}],
            "labels": {"cluster": CLUSTER, "served_id": sidecar.served_id}}
    try:
        answer = await ROUTER.post("/workers", json=body)
    except httpx.HTTPError as error:
        log("join_failed", error=repr(error))
        return
    if answer.status_code not in (200, 201, 202, 409):
        log("join_failed", status=answer.status_code, body=answer.text[:300])


async def leave(sidecar: Sidecar) -> bool:
    """Take this sidecar out of the router: find its id by URL in GET /workers, then DELETE /workers/<id>. True once
    it's out. Requests in flight still finish: the router only stops sending new ones, and smg and the relay stay up."""
    try:
        for worker in (await ROUTER.get("/workers")).raise_for_status().json()["workers"]:
            if worker["url"] == sidecar.tailnet_url:
                answer = await ROUTER.delete(f"/workers/{worker['id']}")
                if answer.status_code != 404:                 # 404: the router removed it in between
                    answer.raise_for_status()
        return True
    except httpx.HTTPError as error:                          # still in: the caller tries again at the next check
        log("leave_failed", error=repr(error))
        return False


async def check(sidecar: Sidecar):
    """One check, changing sidecar in place: health first; then NAMES, until the model maps, which starts smg and the
    relay; once serving, the owner's traffic decides join or leave."""
    if any(child.returncode is not None for child in sidecar.children):
        raise RuntimeError("smg or relay.sh exited")          # ends this sidecar; the scout starts a new one
    if not await healthy():
        sidecar.misses += 1
        sidecar.quiet = 0                                     # a miss breaks a quiet streak
        if sidecar.misses == DOWN_CHECKS:
            log("unhealthy")
        if sidecar.misses >= DOWN_CHECKS and sidecar.inside and await leave(sidecar):
            sidecar.inside = False                            # back in by the join rule once healthy and quiet
        return
    sidecar.misses = 0
    if sidecar.name is None:
        if (name := normalize(sidecar.served_id, sidecar.path)) is None:
            return                                            # unmapped: idle, holding the host for the scout
        await engine_requests()                               # raises if there's no load to watch, before serving
        sidecar.name = name
        await serve(sidecar)
        log("identified", served_id=sidecar.served_id, path=sidecar.path, name=name, tailnet_url=sidecar.tailnet_url)
        return                                                # traffic is read from the next check on
    engine, own = await asyncio.gather(engine_requests(), own_requests(sidecar))
    owner = max(engine - own, 0)                              # the server's requests that didn't come through here
    sidecar.busy, sidecar.quiet = ((sidecar.busy + 1, 0) if owner >= LEAVE_AT else
                                   (0, sidecar.quiet + 1) if owner <= JOIN_AT else (0, 0))
    if sidecar.inside and sidecar.busy >= LEAVE_CHECKS:
        if await leave(sidecar):
            sidecar.inside = False
            log("left", owner_traffic=owner, own_traffic=own)
    elif not sidecar.inside and sidecar.quiet >= JOIN_CHECKS:
        sidecar.inside = True
        log("joined", owner_traffic=owner, own_traffic=own)
    if sidecar.inside and sidecar.busy < LEAVE_CHECKS:
        await join(sidecar)                                   # every check: re-adds it if the router dropped it


async def main():
    sidecar = Sidecar(*await identify())
    if normalize(sidecar.served_id, sidecar.path) is None:
        log("unmapped", served_id=sidecar.served_id, path=sidecar.path)
    try:
        while sidecar.misses < GONE_CHECKS:
            await check(sidecar)
            await asyncio.sleep(CHECK_SECS)
        log("gone")
    finally:
        if sidecar.inside:
            await leave(sidecar)
        for child in sidecar.children:
            with contextlib.suppress(ProcessLookupError):     # its group may be gone already
                os.killpg(child.pid, signal.SIGTERM)


if __name__ == "__main__":
    asyncio.run(main())
