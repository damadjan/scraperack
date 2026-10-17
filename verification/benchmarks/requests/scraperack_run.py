import time
from concurrent.futures import ThreadPoolExecutor

from scraperack import Cluster


def get():
    import requests

    start = time.perf_counter()
    response = requests.get(url="http://192.168.2.9:8080/")
    return response.status_code, time.perf_counter() - start


cluster = Cluster(url="http://192.168.2.16:7070")


def execute(_):
    execution = cluster.run(environment="requests", function=get)
    return cluster.result(execution["id"])


start = time.perf_counter()
with ThreadPoolExecutor(max_workers=2) as pool:
    results = list(pool.map(execute, range(2)))
cluster.close()

print(f"total: {time.perf_counter() - start:.3f}s")
for status, elapsed in results:
    print(f"{status}: {elapsed:.3f}s")
