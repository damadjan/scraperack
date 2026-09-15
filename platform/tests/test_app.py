import os
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from ray import cloudpickle
from scraperack_platform.app import CONTENT_TYPE, app, get_ray


def add(left, right=0):
    return left + right


def fail():
    raise ValueError("broken remotely")


class RemoteFunction:
    def __init__(self, function, ray):
        self.function = function
        self.ray = ray

    def options(self, **options):
        self.ray.options = options
        return self

    def remote(self, *args, **kwargs):
        return self.function, args, kwargs


class FakeRay:
    def __init__(self):
        self.health_checks = 0
        self.options = None

    def nodes(self):
        self.health_checks += 1
        return [{"Alive": True}]

    def remote(self, function):
        return RemoteFunction(function, self)

    def get(self, reference):
        function, args, kwargs = reference
        return function(*args, **kwargs)


class PlatformTests(unittest.TestCase):
    def setUp(self):
        self.ray = FakeRay()
        app.dependency_overrides[get_ray] = lambda: self.ray
        self.client = TestClient(app)

    def tearDown(self):
        app.dependency_overrides.clear()

    def invoke(self, function, *args, requirements=None, **kwargs):
        return self.client.post(
            "/invoke",
            content=cloudpickle.dumps(
                {
                    "function": cloudpickle.dumps(function),
                    "arguments": cloudpickle.dumps((args, kwargs)),
                    "requirements": requirements or [],
                }
            ),
            headers={"content-type": CONTENT_TYPE},
        )

    def result(self, response):
        result = cloudpickle.loads(response.content)
        if result["ok"]:
            result["value"] = cloudpickle.loads(result["value"])
        return result

    def test_health_checks_ray(self):
        response = self.client.get("/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})
        self.assertEqual(self.ray.health_checks, 1)

    def test_invocation_returns_function_result(self):
        response = self.invoke(add, 3, right=4)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.result(response), {"ok": True, "value": 7})
        self.assertIsNone(self.ray.options)

    def test_requirements_are_passed_to_ray_runtime_environment(self):
        response = self.invoke(
            add,
            3,
            right=4,
            requirements=["requests==2.32.5", "beautifulsoup4==4.13.4"],
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            self.ray.options,
            {"runtime_env": {"pip": ["requests==2.32.5", "beautifulsoup4==4.13.4"]}},
        )

    def test_invocation_returns_remote_traceback(self):
        response = self.invoke(fail)
        result = self.result(response)

        self.assertEqual(response.status_code, 500)
        self.assertFalse(result["ok"])
        self.assertIn("ValueError: broken remotely", result["traceback"])

    def test_invalid_payload_is_rejected(self):
        response = self.client.post(
            "/invoke",
            content=b"not a serialized invocation",
            headers={"content-type": CONTENT_TYPE},
        )

        self.assertEqual(response.status_code, 400)

    def test_invalid_requirements_are_rejected(self):
        response = self.invoke(add, 1, requirements=[""])

        self.assertEqual(response.status_code, 400)

    @patch.dict(os.environ, {"SCRAPERACK_MAX_PAYLOAD_BYTES": "4"})
    def test_oversized_payload_is_rejected(self):
        response = self.client.post(
            "/invoke",
            content=b"12345",
            headers={"content-type": CONTENT_TYPE},
        )

        self.assertEqual(response.status_code, 413)


if __name__ == "__main__":
    unittest.main()
