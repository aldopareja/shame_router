"""A semaphore whose slots are shared by every process on the host, used from asyncio.

    slots = open_slots()                              # once per process
    async with slot(slots):
        await do_request()

The node's free slots are counted by one SysV kernel semaphore: no files, no disk I/O. Taking slots from the kernel
is a system call that may wait, so each process:

  * takes slots in batches of BATCH, from a worker thread, so its event loop keeps running;
  * hands the slots it holds straight from one of its coroutines to the next, oldest waiter first;
  * gives every REUSE-th released slot back to the kernel, so a busy process keeps handing slots to the others
    instead of holding its peak share forever.

With undo on, the kernel counts the slots each process holds and adds them back when that process exits, crash
included. The semaphore itself outlives its processes: a later run that finds one under KEY opens it, with whatever
total it was created with.

acquire() timeouts need sysv_ipc built with them. The PyPI wheel (1.2.0) may be built without them: then
SEMAPHORE_TIMEOUT_SUPPORTED is False and every acquire() waits until it succeeds. Built from source (uv's
no-binary-package), it has them.
"""
import asyncio, collections, contextlib, os
from dataclasses import dataclass, field

import sysv_ipc

KEY = 0x5E3A01                                        # names the node's semaphore: every process using KEY shares it
TOTAL = int(os.environ["SLOTS_TOTAL"])                # slots for the whole node, from the sbatch; SysV caps it at 32767
BATCH = 100                                           # slots taken from the kernel per call
REUSE = 10                                            # every REUSE-th release goes back to the kernel


@dataclass
class SemState:
    """This process's side of the node's semaphore."""
    sem: sysv_ipc.Semaphore                                                  # the node's free slots, in the kernel
    waiters: collections.deque = field(default_factory=collections.deque)  # futures of waiting coroutines, oldest first
    releases: int = 0                                                        # slots released so far
    refill: asyncio.Task | None = None                                       # takes kernel slots while any wait


def open_slots() -> SemState:
    """Open the node's semaphore, creating it with TOTAL free slots if this process is first."""
    try:
        sem = sysv_ipc.Semaphore(KEY, sysv_ipc.IPC_CREX, initial_value=TOTAL)
    except sysv_ipc.ExistentialError:                 # another process created it first
        sem = sysv_ipc.Semaphore(KEY)
    sem.undo = True                                   # the kernel gives this process's slots back when it exits
    return SemState(sem)


@contextlib.asynccontextmanager
async def slot(slots: SemState):
    """One of the node's slots for the life of the block. Entering queues this coroutine and starts a refill if none
    runs; leaving hands the slot to the next waiter here, or every REUSE-th time back to the kernel."""
    fut = asyncio.get_running_loop().create_future()
    slots.waiters.append(fut)
    if slots.refill is None or slots.refill.done():
        slots.refill = asyncio.create_task(refill(slots))
    try:
        await fut
    except asyncio.CancelledError:
        if not fut.cancelled():                       # handed a slot just as it was cancelled: pass it on
            give(slots)
        raise
    try:
        yield
    finally:
        slots.releases += 1
        if slots.releases % REUSE:
            give(slots)                               # most releases: the next waiter here runs, no system call
        else:
            slots.sem.release()                       # every REUSE-th: back to the node, for any process to take


def give(slots: SemState):
    """Hand one held slot to the oldest live waiter, or back to the kernel if nobody waits."""
    while slots.waiters:
        fut = slots.waiters.popleft()
        if not fut.done():                            # a cancelled waiter's future is done: skip it
            fut.set_result(None)
            return
    slots.sem.release()


async def refill(slots: SemState):
    """While anyone waits, take BATCH slots at a time from the kernel and hand them out. The wait runs in a worker
    thread (sysv_ipc lets go of the GIL while it waits); its 0.1 s timeout lets it notice when nobody waits any more."""
    while slots.waiters:
        try:
            await asyncio.to_thread(slots.sem.acquire, 0.1, BATCH)
        except sysv_ipc.BusyError:                    # timed out: check again whether anyone still waits
            continue
        for _ in range(BATCH):                        # a slot that finds no live waiter goes straight back
            give(slots)
