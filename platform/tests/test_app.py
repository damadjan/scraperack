import hashlib
import io
import os
import tempfile
import unittest
import zipfile
from unittest.mock import patch

import cloudpickle
from fastapi.testclient import TestClient
from scraperack_platform.app import (
    CONTENT_TYPE,
    app,
    get_ray,
    invocation_cache_status,
    pip_runtime_env,
)
from scraperack_platform.function_cache import enabled as function_cache_enabled


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
        self.ray.task_count += 1
        return FakeReference(
            (self.function, args, kwargs), f"{self.ray.task_count:064x}"
        )


class FakeTaskID:
    def __init__(self, value):
        self.value = value

    def hex(self):
        return self.value


class FakeReference:
    def __init__(self, value, task_id):
        self.value = value
        self._task_id = FakeTaskID(task_id)

    def task_id(self):
        return self._task_id


class FakeRay:
    def __init__(self):
        self.health_checks = 0
        self.options = None
        self.task_count = 0

    def nodes(self):
        self.health_checks += 1
        return [{"Alive": True}]

    def remote(self, function):
        return RemoteFunction(function, self)

    def get(self, reference):
        function, args, kwargs = reference.value
        return function(*args, **kwargs)


class PlatformTests(unittest.TestCase):
    def setUp(self):
        self.cache = tempfile.TemporaryDirectory()
        self.environment = patch.dict(
            os.environ,
            {
                "RAY_PIP_CACHING": "false",
                "SCRAPERACK_FUNCTION_CACHING": "true",
                "SCRAPERACK_FUNCTION_CACHE": os.path.join(self.cache.name, "functions"),
                "SCRAPERACK_WORKING_DIR_CACHE": os.path.join(
                    self.cache.name, "working-dirs"
                ),
                "SCRAPERACK_WORKING_DIR_BASE_URL": "http://gateway:8080/working-dirs",
            },
        )
        self.environment.start()
        self.ray = FakeRay()
        invocation_cache_status.clear()
        app.dependency_overrides[get_ray] = lambda: self.ray
        self.client = TestClient(app)

    def tearDown(self):
        app.dependency_overrides.clear()
        self.environment.stop()
        self.cache.cleanup()

    def invoke(
        self,
        function,
        *args,
        requirements=None,
        working_dir=None,
        cache=None,
        **kwargs,
    ):
        return self.client.post(
            "/invoke",
            content=cloudpickle.dumps(
                {
                    "function": (
                        function
                        if isinstance(function, str)
                        else cloudpickle.dumps(function)
                    ),
                    "arguments": cloudpickle.dumps((args, kwargs)),
                    "requirements": requirements or [],
                    "working_dir": working_dir,
                    "cache": cache or {},
                }
            ),
            headers={"content-type": CONTENT_TYPE},
        )

    def result(self, response):
        result = cloudpickle.loads(response.content)
        if result["ok"]:
            result["value"] = cloudpickle.loads(result["value"])
        return result

    def upload_working_dir(self, files):
        body = io.BytesIO()
        digest = hashlib.sha256()
        with zipfile.ZipFile(body, "w", zipfile.ZIP_DEFLATED) as archive:
            for name, content in sorted(files.items()):
                data = content.encode()
                encoded = name.encode()
                digest.update(len(encoded).to_bytes(8, "big"))
                digest.update(encoded)
                digest.update(len(data).to_bytes(8, "big"))
                digest.update(data)
                archive.writestr(name, data)
        value = digest.hexdigest()
        response = self.client.put(
            f"/working-dirs/{value}.zip",
            content=body.getvalue(),
            headers={"content-type": "application/zip"},
        )
        return value, body.getvalue(), response

    def upload_function(self, function):
        body = cloudpickle.dumps(function)
        digest = hashlib.sha256(body).hexdigest()
        response = self.client.put(f"/functions/{digest}", content=body)
        return digest, body, response

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

    def test_invocation_cache_status_is_exposed_by_ray_task_id(self):
        response = self.invoke(
            add,
            1,
            2,
            cache={"function": "disabled", "working_dir": "none"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            self.client.get("/invocations/cache-status").json(),
            {
                f"{1:064x}": {
                    "function_cache": "disabled",
                    "working_dir_cache": "none",
                }
            },
        )

    def test_invalid_invocation_cache_status_is_rejected(self):
        response = self.invoke(add, 1, cache={"function": "maybe"})

        self.assertEqual(response.status_code, 400)

    def test_function_is_cached_and_invoked_by_digest(self):
        digest, body, first = self.upload_function(add)

        second = self.client.put(f"/functions/{digest}", content=body)
        response = self.invoke(digest, 3, right=4)

        self.assertEqual(first.status_code, 201)
        self.assertEqual(second.status_code, 204)
        self.assertEqual(self.client.head(f"/functions/{digest}").status_code, 200)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.result(response), {"ok": True, "value": 7})

    def test_function_digest_is_verified(self):
        response = self.client.put(f"/functions/{'0' * 64}", content=b"wrong")

        self.assertEqual(response.status_code, 400)

    def test_missing_cached_function_is_rejected(self):
        response = self.invoke("0" * 64, 1)

        self.assertEqual(response.status_code, 400)

    @patch.dict(os.environ, {"SCRAPERACK_FUNCTION_CACHING": "false"})
    def test_function_cache_can_be_disabled(self):
        digest = "0" * 64

        self.assertEqual(self.client.head(f"/functions/{digest}").status_code, 204)
        self.assertEqual(self.client.put(f"/functions/{digest}").status_code, 409)
        self.assertEqual(self.invoke(add, 1, 2).status_code, 200)

    @patch.dict(os.environ, {"SCRAPERACK_FUNCTION_CACHING": "maybe"})
    def test_function_cache_configuration_must_be_valid(self):
        with self.assertRaisesRegex(RuntimeError, "must be set to true or false"):
            self.client.get("/health")

    @patch.dict(os.environ, {}, clear=True)
    def test_function_cache_is_enabled_by_default(self):
        self.assertTrue(function_cache_enabled())

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

    def test_working_dir_is_stored_and_passed_to_ray(self):
        digest, archive, upload = self.upload_working_dir(
            {"project/parser.py": "VALUE = 7"}
        )

        response = self.invoke(
            add,
            3,
            right=4,
            requirements=["example==1.2.3"],
            working_dir=digest,
        )

        self.assertEqual(upload.status_code, 201)
        self.assertEqual(
            self.client.head(f"/working-dirs/{digest}.zip").status_code, 200
        )
        self.assertEqual(
            self.client.get(f"/working-dirs/{digest}.zip").content, archive
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            self.ray.options,
            {
                "runtime_env": {
                    "pip": ["example==1.2.3"],
                    "working_dir": f"http://gateway:8080/working-dirs/{digest}.zip",
                }
            },
        )

    def test_working_dir_upload_is_idempotent(self):
        digest, body, first = self.upload_working_dir({"module.py": "VALUE = 1"})

        second = self.client.put(f"/working-dirs/{digest}.zip", content=body)

        self.assertEqual(first.status_code, 201)
        self.assertEqual(second.status_code, 204)

    def test_working_dir_digest_is_verified(self):
        response = self.client.put(
            f"/working-dirs/{'0' * 64}.zip", content=b"not a zip"
        )

        self.assertEqual(response.status_code, 400)

    def test_missing_working_dir_is_rejected(self):
        response = self.invoke(add, 1, working_dir="0" * 64)

        self.assertEqual(response.status_code, 400)

    @patch.dict(os.environ, {"RAY_PIP_CACHING": "true"})
    def test_pip_cache_can_be_enabled(self):
        response = self.invoke(add, 1, requirements=["requests==2.32.5"])

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            self.ray.options,
            {
                "runtime_env": {
                    "pip": {
                        "packages": ["requests==2.32.5"],
                        "pip_install_options": ["--disable-pip-version-check"],
                    }
                }
            },
        )

    @patch.dict(os.environ, {}, clear=True)
    def test_pip_cache_is_enabled_by_default(self):
        self.assertEqual(
            pip_runtime_env(["requests==2.32.5"]),
            {
                "pip": {
                    "packages": ["requests==2.32.5"],
                    "pip_install_options": ["--disable-pip-version-check"],
                }
            },
        )

    @patch.dict(os.environ, {"RAY_PIP_CACHING": "maybe"})
    def test_pip_cache_configuration_must_be_valid(self):
        with self.assertRaisesRegex(RuntimeError, "must be set to true or false"):
            self.client.get("/health")

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
