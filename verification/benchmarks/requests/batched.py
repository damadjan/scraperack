import time
from concurrent.futures import ThreadPoolExecutor

from scraperack import Cluster


def request():
    import requests

    start = time.perf_counter()
    response = requests.get(url="http://192.168.2.9:8080/")
    return response.status_code, time.perf_counter() - start


cluster = Cluster(url="http://192.168.2.16:7070")


def execute(_):
    execution = cluster.run(environment="requests", function=request)
    return cluster.result(execution["id"])


started = time.perf_counter()
results = []
with ThreadPoolExecutor(max_workers=10) as pool:
    for _ in range(20):
        results.extend(pool.map(execute, range(10)))
cluster.close()

roundtrips = [elapsed for _, elapsed in results]
print(f"requests: {len(results)}")
print(f"total: {time.perf_counter() - started:.3f}s")
print(f"average request roundtrip: {sum(roundtrips) / len(roundtrips):.3f}s")
