import os

from scraperack import Cluster


def hello(name):
    return f"hello {name} from a microVM"


def add(a, b):
    return a + b


def squares(values):
    return [value**2 for value in values]


def python_version():
    import sys
    return sys.version


cluster = Cluster("http://192.168.2.16:7070")
environment = os.getenv("SCRAPERACK_ENVIRONMENT", "python")
jobs = [
    (python_version, ()),
    (hello, ("Todor",)),
    (add, (6, 7)),
    (squares, ([1, 2, 3, 4, 5],)),
]

try:
    for function, params in jobs:
        execution = cluster.run(environment, function, params)
        print(function.__name__, cluster.result(execution["id"]))
finally:
    cluster.close()
