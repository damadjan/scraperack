import os

from scraperack import Cluster


cluster = Cluster(os.getenv("SCRAPERACK_URL", "http://192.168.2.16:7070"))
environment = os.getenv("SCRAPERACK_ENVIRONMENT", "python")


def hello():
    return "hello from a microVM"


def math(a, b):
    return {"sum": a + b, "product": a * b}


def squares(values):
    return [value**2 for value in values]


jobs = [
    ("hello", hello, ()),
    ("math", math, (6, 7)),
    ("python", squares, ([1, 2, 3, 4, 5],)),
]

try:
    for name, function, params in jobs:
        execution = cluster.run(environment, function, params)
        print(name, cluster.result(execution["id"]))
finally:
    cluster.close()
