import os

from scraperack import Cluster


cluster = Cluster(os.getenv("SCRAPERACK_URL", "http://192.168.2.16:7070"))
environment = os.getenv("SCRAPERACK_ENVIRONMENT", "python")
jobs = [
    ("hello", "def main():\n    return 'hello from a microVM'", ()),
    ("math", "def main(a, b):\n    return {'sum': a + b, 'product': a * b}", (6, 7)),
    ("python", "def main(values):\n    return [value ** 2 for value in values]", ([1, 2, 3, 4, 5],)),
]

try:
    for name, source, params in jobs:
        execution = cluster.run_source(environment, source, params)
        result = cluster.wait(execution["id"])
        print(name, result["status"], result.get("result") or result.get("error"))
finally:
    cluster.close()
