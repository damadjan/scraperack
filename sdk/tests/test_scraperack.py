import sys
import types
import unittest
import urllib.error
from email.message import Message
from unittest.mock import patch

import cloudpickle
import scraperack


def add(left, right=0):
    return left + right


def multiply(left, right):
    return left * right


class FakeResponse:
    def __init__(self, body):
        self.body = body

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


if __name__ == "__main__":
    unittest.main()
