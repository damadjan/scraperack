import hashlib
import io
import sys
import tempfile
import types
import unittest
import urllib.error
import warnings
import zipfile
from email.message import Message
from pathlib import Path
from unittest.mock import patch

import cloudpickle
import scraperack
from scraperack import _function_cache, _working_dir


def add(left, right=0):
    return left + right


def multiply(left, right):
    return left * right


class FakeResponse:
    def __init__(self, body, status=200):
        self.body = body
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def read(self):
        return self.body

    def close(self):
        pass


class ScrapeRackTests(unittest.TestCase):
    def setUp(self):
        scraperack.configure("http://gateway.test/")
        self.function_cache = patch(
            "scraperack.prepare_function",
            side_effect=lambda _, target: (cloudpickle.dumps(target), "disabled"),
        )
        self.function_cache.start()
        _working_dir._cache.clear()
        _working_dir._directory_locks.clear()

    def tearDown(self):
        self.function_cache.stop()

    @patch("scraperack.urllib.request.urlopen")
    def test_decorated_function_sends_requirements_and_invocation(self, urlopen):
        urlopen.return_value = FakeResponse(
            cloudpickle.dumps({"ok": True, "value": cloudpickle.dumps(7)})
        )

        remote_add = scraperack.function(requirements=["example==1.2.3"])(add)
        result = remote_add(3, right=4)

        request = urlopen.call_args.args[0]
        invocation = cloudpickle.loads(request.data)
        target = cloudpickle.loads(invocation["function"])
        args, kwargs = cloudpickle.loads(invocation["arguments"])
        self.assertEqual(result, 7)
        self.assertEqual(request.full_url, "http://gateway.test/invoke")
        self.assertEqual(request.get_header("Content-type"), scraperack.CONTENT_TYPE)
        self.assertEqual(target(3, right=4), 7)
        self.assertEqual(args, (3,))
        self.assertEqual(kwargs, {"right": 4})
        self.assertEqual(invocation["requirements"], ["example==1.2.3"])
        self.assertEqual(invocation["function_name"], "add")
        self.assertIsNone(invocation["project"])
        self.assertIsNone(invocation["working_dir"])
        self.assertEqual(
            invocation["cache"],
            {"function": "disabled", "working_dir": "none"},
        )

    @patch("scraperack.urllib.request.urlopen")
    def test_working_dir_is_uploaded_and_referenced(self, urlopen):
        uploaded = {}

        def response(request):
            if request.get_method() == "HEAD":
                raise urllib.error.HTTPError(
                    request.full_url, 404, "Not found", Message(), FakeResponse(b"")
                )
            if request.get_method() == "PUT":
                uploaded["archive"] = request.data.read()
                return FakeResponse(b"")
            uploaded["invocation"] = request.data
            return FakeResponse(
                cloudpickle.dumps({"ok": True, "value": cloudpickle.dumps(3)})
            )

        urlopen.side_effect = response

        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "module.py").write_text("VALUE = 3")
            Path(directory, "credentials.txt").write_text("secret")
            remote_add = scraperack.function(working_dir=directory)(add)
            result = remote_add(1, 2)

        invocation = cloudpickle.loads(uploaded["invocation"])
        self.assertEqual(result, 3)
        self.assertRegex(invocation["working_dir"], r"^[0-9a-f]{64}$")
        self.assertEqual(invocation["cache"]["working_dir"], "miss")
        with zipfile.ZipFile(io.BytesIO(uploaded["archive"])) as archive:
            self.assertEqual(archive.namelist(), ["module.py"])

    def test_working_dir_must_be_a_directory(self):
        with self.assertRaisesRegex(ValueError, "working_dir"):
            scraperack.function(working_dir="missing-directory")(add)

    @patch("scraperack.urllib.request.urlopen")
    def test_large_working_dir_warns_by_default(self, urlopen):
        def response(request):
            if request.get_method() == "HEAD":
                return FakeResponse(b"")
            return FakeResponse(
                cloudpickle.dumps({"ok": True, "value": cloudpickle.dumps(3)})
            )

        urlopen.side_effect = response
        with tempfile.TemporaryDirectory() as directory:
            with Path(directory, "large.bin").open("wb") as file:
                file.truncate(5 * 1024 * 1024 + 1)
            remote_add = scraperack.function(working_dir=directory)(add)

            with self.assertWarnsRegex(UserWarning, "5.0 MiB"):
                remote_add(1, 2)

    @patch("scraperack.urllib.request.urlopen")
    def test_working_dir_warning_is_configurable(self, urlopen):
        def response(request):
            if request.get_method() == "HEAD":
                return FakeResponse(b"")
            return FakeResponse(
                cloudpickle.dumps({"ok": True, "value": cloudpickle.dumps(3)})
            )

        urlopen.side_effect = response
        scraperack.configure(
            "http://gateway.test",
            working_dir_warning=False,
            working_dir_warning_bytes=1,
        )
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "file.txt").write_text("larger than one byte")
            remote_add = scraperack.function(working_dir=directory)(add)

            with warnings.catch_warnings():
                warnings.simplefilter("error")
                self.assertEqual(remote_add(1, 2), 3)

    def test_working_dir_warning_configuration_is_validated(self):
        with self.assertRaisesRegex(TypeError, "working_dir_warning"):
            scraperack.configure("http://gateway.test", working_dir_warning="yes")
        with self.assertRaisesRegex(TypeError, "working_dir_warning_bytes"):
            scraperack.configure("http://gateway.test", working_dir_warning_bytes=-1)

    @patch("scraperack.urllib.request.urlopen")
    def test_project_is_sent_with_python_function_name(self, urlopen):
        urlopen.return_value = FakeResponse(
            cloudpickle.dumps({"ok": True, "value": cloudpickle.dumps(5)})
        )
        scraperack.configure("http://gateway.test", project="linkedin-jobs")

        scraperack.function(add)(2, 3)

        invocation = cloudpickle.loads(urlopen.call_args.args[0].data)
        self.assertEqual(invocation["project"], "linkedin-jobs")
        self.assertEqual(invocation["function_name"], "add")

    def test_project_must_be_a_slug(self):
        for project in ("LinkedIn", "linkedin_jobs", "linkedin jobs", "a--b", "-a"):
            with self.subTest(project=project), self.assertRaisesRegex(
                ValueError, "lowercase letters"
            ):
                scraperack.configure("http://gateway.test", project=project)
        with self.assertRaisesRegex(TypeError, "project"):
            scraperack.configure("http://gateway.test", project=1)

    @patch("scraperack.urllib.request.urlopen")
    def test_remote_error_includes_remote_traceback(self, urlopen):
        headers = Message()
        headers["content-type"] = scraperack.CONTENT_TYPE
        urlopen.side_effect = urllib.error.HTTPError(
            "http://gateway.test/invoke",
            500,
            "Remote failure",
            headers,
            FakeResponse(
                cloudpickle.dumps(
                    {"ok": False, "traceback": "ValueError: broken remotely"}
                )
            ),
        )

        with self.assertRaisesRegex(scraperack.RemoteError, "broken remotely"):
            scraperack.function(add)(1, 2)

    @patch("scraperack.urllib.request.urlopen")
    def test_connection_error_names_gateway(self, urlopen):
        urlopen.side_effect = urllib.error.URLError("offline")

        with self.assertRaisesRegex(RuntimeError, "gateway.test"):
            scraperack.function(add)(1, 2)

    def test_decorator_preserves_function_metadata(self):
        decorated = scraperack.function(add)

        self.assertEqual(decorated.__name__, "add")
        self.assertIs(decorated.__wrapped__, add)

    def test_import_does_not_import_ray(self):
        self.assertNotIn("ray", sys.modules)

    def test_requirements_must_be_non_empty_strings(self):
        with self.assertRaisesRegex(TypeError, "requirements"):
            scraperack.function(requirements="example==1.2.3")(add)
        with self.assertRaisesRegex(TypeError, "requirements"):
            scraperack.function(requirements=[""])(add)

    @patch("scraperack.urllib.request.urlopen")
    def test_function_module_is_serialized_by_value(self, urlopen):
        urlopen.return_value = FakeResponse(
            cloudpickle.dumps({"ok": True, "value": cloudpickle.dumps(6)})
        )
        module = types.ModuleType("temporary_user_module")
        module.multiply = types.FunctionType(
            multiply.__code__, module.__dict__, "multiply"
        )
        module.multiply.__module__ = module.__name__
        sys.modules[module.__name__] = module

        try:
            scraperack.function(module.multiply)(2, 3)
            payload = urlopen.call_args.args[0].data
        finally:
            del sys.modules[module.__name__]

        invocation = cloudpickle.loads(payload)
        target = cloudpickle.loads(invocation["function"])
        self.assertEqual(target(2, 3), 6)

    @patch("scraperack._function_cache.urllib.request.urlopen")
    def test_function_is_uploaded_and_referenced_by_digest(self, urlopen):
        uploaded = {}

        def response(request):
            if request.get_method() == "HEAD":
                raise urllib.error.HTTPError(
                    request.full_url, 404, "Not found", Message(), FakeResponse(b"")
                )
            uploaded["function"] = request.data
            return FakeResponse(b"")

        urlopen.side_effect = response

        reference, status = _function_cache.prepare("http://gateway.test", add)

        self.assertEqual(reference, hashlib.sha256(uploaded["function"]).hexdigest())
        self.assertEqual(status, "miss")
        self.assertEqual(
            [call.args[0].get_method() for call in urlopen.call_args_list],
            ["HEAD", "PUT"],
        )

    @patch("scraperack._function_cache.urllib.request.urlopen")
    def test_cached_function_is_not_uploaded_again(self, urlopen):
        urlopen.return_value = FakeResponse(b"")

        reference, status = _function_cache.prepare("http://gateway.test", add)

        self.assertRegex(reference, r"^[0-9a-f]{64}$")
        self.assertEqual(status, "hit")
        self.assertEqual(urlopen.call_count, 1)
        self.assertEqual(urlopen.call_args.args[0].get_method(), "HEAD")

    @patch("scraperack._function_cache.urllib.request.urlopen")
    def test_disabled_function_cache_sends_function_inline(self, urlopen):
        urlopen.return_value = FakeResponse(b"", status=204)

        reference, status = _function_cache.prepare("http://gateway.test", add)

        self.assertEqual(cloudpickle.loads(reference)(2, 3), 5)
        self.assertEqual(status, "disabled")
        self.assertEqual(urlopen.call_count, 1)


if __name__ == "__main__":
    unittest.main()
