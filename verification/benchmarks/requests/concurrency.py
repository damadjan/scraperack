import sys
import time
from concurrent.futures import ThreadPoolExecutor

from scraperack import Cluster


def request():
    import requests

    start = time.perf_counter()
    status = requests.get(url="http://192.168.2.9:8080/").status_code
    return status, time.perf_counter() - start


def peak(executions):
    active = maximum = 0
    events = sorted(
        [(execution["started_at_ms"], 1) for execution in executions]
        + [(execution["finished_at_ms"], -1) for execution in executions],
        key=lambda event: (event[0], -event[1]),
    )
    for _, change in events:
        active += change
        maximum = max(maximum, active)
    return maximum


levels = [int(value) for value in sys.argv[1:]] or [1, 2, 4, 8, 16, 32]
cluster = Cluster(url="http://192.168.2.16:7070")
print("sent  peak  ok  total    req/s  p50     p95")
for concurrency in levels:
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        executions = list(pool.map(
            lambda _: cluster.wait(execution_id=cluster.run(
                environment="bench2", function=request,
            )["id"]),
            range(concurrency),
        ))
    total = time.perf_counter() - start
    elapsed = sorted(
        execution["result"]["value"]["data"][1]
        for execution in executions
        if not execution["error"]
    )
    p50 = elapsed[len(elapsed) // 2] if elapsed else 0
    p95 = elapsed[int((len(elapsed) - 1) * .95)] if elapsed else 0
    print(f"{concurrency:>4}  {peak(executions):>4}  {len(elapsed):>2}  "
          f"{total:>6.3f}s  {len(elapsed) / total:>5.1f}  {p50:>6.3f}s  {p95:>6.3f}s")
cluster.close()
