import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import httpx

from scraperack import AsyncCluster, Cluster


def task(value):
    return value


class ClientTests(unittest.TestCase):
    def test_run_uploads_and_reuses_content_addressed_working_directory(self):
        uploaded = set()
        requests = []

        def handler(request):
            requests.append(request)
            if request.method == "HEAD":
                return httpx.Response(204 if request.url.path.rsplit("/", 1)[-1] in uploaded else 404)
            if request.method == "PUT":
                digest = request.url.path.rsplit("/", 1)[-1]
                self.assertEqual(hashlib.sha256(request.content).hexdigest(), digest)
                uploaded.add(digest)
                return httpx.Response(201)
            return httpx.Response(202, json={"id": "execution"})

        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory, "helper.py")
            source.write_text("VALUE = 1", encoding="utf-8")
            cluster = Cluster("http://controller")
            cluster.client.close()
            cluster.client = httpx.Client(
                base_url="http://controller", transport=httpx.MockTransport(handler)
            )
            try:
                first = cluster.run("python", task, (1,), working_dir=directory)
                second = cluster.run("python", task, (2,), working_dir=directory)
                source.write_text("VALUE = 2", encoding="utf-8")
                third = cluster.run("python", task, (3,), working_dir=directory)
            finally:
                cluster.close()

        self.assertEqual(first, {"id": "execution"})
        self.assertEqual(second, {"id": "execution"})
        self.assertEqual(third, {"id": "execution"})
        self.assertEqual(sum(request.method == "PUT" for request in requests), 2)
        payloads = [
            json.loads(request.content)["payload"]
            for request in requests
            if request.method == "POST"
        ]
        self.assertEqual(payloads[0]["working_dir"], payloads[1]["working_dir"])
        self.assertEqual(payloads[0]["cache_key"], payloads[1]["cache_key"])
        self.assertNotEqual(payloads[1]["working_dir"], payloads[2]["working_dir"])
        self.assertNotEqual(payloads[1]["cache_key"], payloads[2]["cache_key"])

    def test_only_run_is_public_for_execution_submission(self):
        for client in (Cluster, AsyncCluster):
            self.assertTrue(hasattr(client, "run"))
            for removed in ("run_json", "run_source", "run_files", "submit"):
                self.assertFalse(hasattr(client, removed))


class AsyncClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_uploads_working_directory(self):
        methods = []

        def handler(request):
            methods.append(request.method)
            if request.method == "HEAD":
                return httpx.Response(404)
            if request.method == "PUT":
                return httpx.Response(201)
            return httpx.Response(202, json={"id": "execution"})

        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "helper.py").write_text("VALUE = 1", encoding="utf-8")
            cluster = AsyncCluster("http://controller")
            await cluster.client.aclose()
            cluster.client = httpx.AsyncClient(
                base_url="http://controller", transport=httpx.MockTransport(handler)
            )
            try:
                execution = await cluster.run("python", task, working_dir=directory)
            finally:
                await cluster.close()

        self.assertEqual(execution, {"id": "execution"})
        self.assertEqual(methods, ["HEAD", "PUT", "POST"])


if __name__ == "__main__":
    unittest.main()
