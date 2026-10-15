import time
from concurrent.futures import ThreadPoolExecutor

from scraperack import Cluster


cluster = Cluster(url="http://192.168.2.16:7070")
start = time.perf_counter()
with ThreadPoolExecutor(max_workers=5) as pool:
    executions = list(pool.map(
        lambda value: cluster.wait(execution_id=cluster.run_json(environment="bench2", payload=value)["id"]),
        range(5),
    ))
durations = [(execution["finished_at_ms"] - execution["started_at_ms"]) / 1000
             for execution in executions]
print(f"{time.perf_counter() - start:.3f}s total; "
      f"executions: {', '.join(f'{duration:.3f}s' for duration in durations)}")
cluster.close()
