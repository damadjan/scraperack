import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import httpx

from scraperack import APIError, AsyncCluster, Cluster


def cluster_with(handler):
    return Cluster("http://controller",
                   client=httpx.Client(base_url="http://controller",
                                       transport=httpx.MockTransport(handler)))


class ClusterTests(unittest.TestCase):
    def test_tasks_use_core_endpoints(self):
        requests = []
        statuses = iter(["running", "succeeded"])

        def handler(request):
            requests.append((request.method, request.url.path, request.url.query.decode()))
            if request.url.path.endswith("/wait"):
                return httpx.Response(200, json={"id": "t1", "status": next(statuses)})
            if request.url.path.endswith("/result"):
                return httpx.Response(200, json={"status": "succeeded", "value": 1, "error": None})
            if request.method == "POST":
                body = json.loads(request.content)
                self.assertEqual(body["pool"], "jobs")
                self.assertEqual(body["secrets"], {"TOKEN": "x"})
                return httpx.Response(202, json={"id": "t1", "status": "queued"})
            return httpx.Response(200, json=[])

        with cluster_with(handler) as cluster:
            task = cluster.submit("jobs", {"encoding": "json", "data": 1}, secrets={"TOKEN": "x"})
            self.assertEqual(cluster.result(task["id"])["value"], 1)
            cluster.tasks(limit=5, pool=["a", "b"], owner="plugin:python")
        self.assertEqual(requests[0][:2], ("POST", "/v1/tasks"))
        self.assertEqual([r[1] for r in requests[1:4]],
                         ["/v1/tasks/t1/wait", "/v1/tasks/t1/wait", "/v1/tasks/t1/result"])
        self.assertEqual(requests[-1][2], "limit=5&pool=a%2Cb&owner=plugin%3Apython")

    def test_errors_carry_the_controller_message(self):
        with cluster_with(lambda request: httpx.Response(
                400, json={"error": "unknown pool missing"})) as cluster:
            with self.assertRaises(APIError) as raised:
                cluster.submit("missing", {})
        self.assertEqual(raised.exception.status, 400)
        self.assertEqual(raised.exception.message, "unknown pool missing")

    def test_update_pool_merges_changes(self):
        sent = []

        def handler(request):
            if request.method == "GET":
                return httpx.Response(200, json={"name": "p", "warm_instances": 1, "cpu_millis": 500})
            sent.append(json.loads(request.content))
            return httpx.Response(200, json=sent[-1])

        with cluster_with(handler) as cluster:
            cluster.update_pool("p", warm_instances=3)
        self.assertEqual(sent, [{"name": "p", "warm_instances": 3, "cpu_millis": 500}])

    def test_working_dirs_are_uploaded_once_per_content(self):
        uploaded, puts = set(), []

        def handler(request):
            digest = request.url.path.rsplit("/", 1)[-1]
            if request.method == "HEAD":
                return httpx.Response(204 if digest in uploaded else 404)
            self.assertEqual(hashlib.sha256(request.content).hexdigest(), digest)
            uploaded.add(digest)
            puts.append(digest)
            return httpx.Response(201)

        with tempfile.TemporaryDirectory() as directory, cluster_with(handler) as cluster:
            Path(directory, "a.py").write_text("A = 1", encoding="utf-8")
            first = cluster.upload_working_dir(directory)
            self.assertEqual(cluster.upload_working_dir(directory), first)
            Path(directory, "a.py").write_text("A = 2", encoding="utf-8")
            self.assertNotEqual(cluster.upload_working_dir(directory), first)
        self.assertEqual(len(puts), 2)


class AsyncClusterTests(unittest.IsolatedAsyncioTestCase):
    async def test_submit_and_wait(self):
        def handler(request):
            if request.url.path.endswith("/wait"):
                return httpx.Response(200, json={"id": "t1", "status": "failed"})
            return httpx.Response(202, json={"id": "t1", "status": "queued"})

        client = httpx.AsyncClient(base_url="http://controller",
                                   transport=httpx.MockTransport(handler))
        async with AsyncCluster("http://controller", client=client) as cluster:
            task = await cluster.submit("jobs", {"encoding": "json", "data": 1})
            self.assertEqual((await cluster.wait(task["id"]))["status"], "failed")


if __name__ == "__main__":
    unittest.main()
