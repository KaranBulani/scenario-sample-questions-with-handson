"""
================================================================================
PYTHON INTERVIEW SCENARIO #1 — Mini-systems: rate limiter, LRU cache,
                                topological sort (topic 8, in one piece)
================================================================================
THE ASK (three short coding questions in one round)
    1. "Implement a rate limiter that allows bursts but enforces an average
       rate."            → token bucket
    2. "Implement an LRU cache with O(1) get/put."      → OrderedDict
    3. "Given tasks and their dependencies, return a valid execution order
       and detect cycles."  → Kahn's algorithm (what Airflow does before
       scheduling a DAG run — that's why this gets asked in DE interviews)

HOW TO RUN (stdlib only)
    python python/01_rate_limiter_lru_toposort.py

IMPLEMENTATION WALKTHROUGH
    1. TokenBucketRateLimiter — bucket holds up to `capacity` tokens,
       refills at `refill_per_sec`. allow() is O(1): top up by elapsed
       time (monotonic clock!), take one token or refuse. Bursts drain the
       bucket; sustained abuse is capped by refill. Compare with a fixed
       window (2x burst at boundaries — 5 req at t=0.99 and 5 at t=1.01)
       and sliding window (exact but O(N) memory). Thread-safe via a Lock
       because one limiter is shared by all workers in a process.
    2. LRUCache — OrderedDict IS a doubly-linked list + hash map in one:
       move_to_end(key) marks most-recently-used in O(1);
       popitem(last=False) evicts the least-recently-used in O(1). This
       is literally how functools.lru_cache works internally — say so.
       get() must ALSO touch recency, or you've built a FIFO, not an LRU
       (the #1 bug candidates make).
    3. topological_sort (Kahn's) — indegree[task] = number of unfinished
       dependencies; tasks with indegree 0 are runnable, appended to the
       queue (sorted seed → deterministic output). Consuming a task
       decrements its dependents; anything left with indegree > 0 when the
       queue empties is part of a cycle → the DAG is unrunnable.
       O(V + E). DFS + "back edge" detection is the recursive alternative;
       Kahn's is iterative and returns a usable order on success.

CONCEPTS THE INTERVIEWER IS PROBING
    * Time sources: time.monotonic() (never wall clock — NTP jumps
      backwards would mint free tokens).
    * Complexity: all three structures must be O(1) per op (amortized);
       know why dict/OrderedDict/deque deliver that.
    * Thread safety: GIL saves you from torn reads on a dict but NOT from
       check-then-act races (two threads both take the last token) → Lock.
    * Distributed follow-up: per-process limiter isn't enough across
       replicas → Redis (INCR + EXPIRE sliding window, or Lua token
       bucket); functools.lru_cache per-process same story.

LIKELY FOLLOW-UP QUESTIONS
    Q: Why not just functools.lru_cache?
    A: Use it in real code (maxsize, thread-safe, C speed); interviews
       want you to build the underlying structure. Also: per-process only.
    Q: Rate limit across 10 API pods?
    A: Centralized limiter in Redis; the in-process bucket stays as a
       cheap first line to shed obvious abuse.
    Q: Cycle detection without Kahn's?
    A: DFS with a color/state set: entering a node already on the current
       recursion stack = back edge = cycle.
    Q: Tasks need priority when several are ready?
    A: Replace the FIFO deque with a heap keyed on priority_weight —
       exactly Airflow's pool-slot contention logic.
================================================================================
"""
from __future__ import annotations

import threading
import time
from collections import OrderedDict, defaultdict, deque


class TokenBucketRateLimiter:
    """Capacity C, refill R tokens/sec. Bursts up to C, average rate R."""

    def __init__(self, capacity: int, refill_per_sec: float) -> None:
        self.capacity = capacity
        self.refill = refill_per_sec
        self._tokens = float(capacity)
        self._updated = time.monotonic()
        self._lock = threading.Lock()          # shared across worker threads

    def allow(self) -> bool:
        with self._lock:                       # check-then-act must be atomic
            now = time.monotonic()
            self._tokens = min(self.capacity,
                               self._tokens + (now - self._updated) * self.refill)
            self._updated = now
            if self._tokens >= 1:
                self._tokens -= 1
                return True
            return False


class LRUCache:
    """OrderedDict = hash map + doubly-linked list → O(1) get/put/evict."""

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self._data: OrderedDict = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            if key not in self._data:
                return None
            self._data.move_to_end(key)        # touch = most recently used
            return self._data[key]

    def put(self, key, value) -> None:
        with self._lock:
            if key in self._data:
                self._data.move_to_end(key)
            self._data[key] = value
            if len(self._data) > self.capacity:
                self._data.popitem(last=False)  # evict least recently used


def topological_sort(tasks: dict[str, list[str]]) -> list[str]:
    """Kahn's algorithm. tasks = {task: [deps]}. Raises on cycles/unknown deps."""
    indegree = {task: 0 for task in tasks}
    dependents: dict[str, list[str]] = defaultdict(list)
    for task, deps in tasks.items():
        for dep in deps:
            if dep not in indegree:
                raise ValueError(f"unknown task: {dep}")
            indegree[task] += 1
            dependents[dep].append(task)

    ready = deque(sorted(t for t, d in indegree.items() if d == 0))  # deterministic
    order: list[str] = []
    while ready:
        task = ready.popleft()                 # O(1) left pop
        order.append(task)
        for dependent in dependents[task]:
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                ready.append(dependent)

    if len(order) != len(tasks):
        stuck = sorted(t for t, d in indegree.items() if d > 0)
        raise ValueError(f"cycle detected among tasks: {stuck}")
    return order


def main() -> None:
    # 1) Rate limiter: burst of 3 allowed instantly, then throttled.
    limiter = TokenBucketRateLimiter(capacity=3, refill_per_sec=0.1)
    results = [limiter.allow() for _ in range(5)]
    print("rate limiter (3 cap):", results)          # [True, True, True, False, False]

    # 2) LRU: touching 'a' saves it; 'b' is the one evicted when 'c' arrives.
    cache = LRUCache(capacity=2)
    cache.put("a", 1)
    cache.put("b", 2)
    cache.get("a")                                   # a becomes most recent
    cache.put("c", 3)                                # evicts b, not a
    print("lru get a/b/c:", cache.get("a"), cache.get("b"), cache.get("c"))  # 1 None 3

    # 3) DAG ordering + cycle detection (Airflow-style).
    dag = {
        "extract": [],
        "validate": ["extract"],
        "load_raw": ["extract"],
        "publish": ["validate", "load_raw"],
    }
    print("execution order:", topological_sort(dag))
    try:
        topological_sort({"a": ["b"], "b": ["a"]})
    except ValueError as err:
        print("cycle detected:", err)


if __name__ == "__main__":
    main()
