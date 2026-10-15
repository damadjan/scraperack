import time
from concurrent.futures import ThreadPoolExecutor

import requests


def get(_):
    start = time.perf_counter()
    response = requests.get(url="http://192.168.2.9:8080/")
    return response.status_code, time.perf_counter() - start


start = time.perf_counter()
with ThreadPoolExecutor(max_workers=5) as pool:
    results = list(pool.map(get, range(5)))

print(f"total: {time.perf_counter() - start:.3f}s")
for status, elapsed in results:
    print(f"{status}: {elapsed:.3f}s")
