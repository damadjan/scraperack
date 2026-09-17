import inspect
import os
import urllib.error
import urllib.request
from functools import update_wrapper

import cloudpickle

from scraperack._function_cache import prepare as prepare_function
from scraperack._working_dir import resolve, upload

CONTENT_TYPE = "application/vnd.scraperack.function"
_address = os.getenv("SCRAPERACK_ADDRESS", "http://127.0.0.1:42800")
_working_dir_warning = True
_working_dir_warning_bytes = 5 * 1024 * 1024


class RemoteError(RuntimeError):
    pass


def configure(
    address,
    *,
    working_dir_warning=True,
    working_dir_warning_bytes=5 * 1024 * 1024,
):
    if not isinstance(working_dir_warning, bool):
        raise TypeError("working_dir_warning must be true or false")
    if (
        isinstance(working_dir_warning_bytes, bool)
        or not isinstance(working_dir_warning_bytes, int)
        or working_dir_warning_bytes < 0
    ):
        raise TypeError("working_dir_warning_bytes must be a non-negative integer")

    global _address, _working_dir_warning, _working_dir_warning_bytes
    _address = address.rstrip("/")
    _working_dir_warning = working_dir_warning
    _working_dir_warning_bytes = working_dir_warning_bytes


def invoke(target, requirements, working_dir, *args, **kwargs):
    function = prepare_function(_address, target)
    warning_bytes = _working_dir_warning_bytes if _working_dir_warning else None
    working_dir_digest = (
        upload(_address, working_dir, warning_bytes) if working_dir else None
    )
    payload = cloudpickle.dumps(
        {
            "function": function,
            "arguments": cloudpickle.dumps((args, kwargs)),
            "requirements": list(requirements),
            "working_dir": working_dir_digest,
        }
    )
    request = urllib.request.Request(
        f"{_address}/invoke",
        data=payload,
        headers={"content-type": CONTENT_TYPE},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request) as response:
            body = response.read()
    except urllib.error.HTTPError as error:
        body = error.read()
        if error.headers.get_content_type() != CONTENT_TYPE:
            raise RuntimeError(body.decode(errors="replace")) from error
    except urllib.error.URLError as error:
        raise RuntimeError(
            f"Cannot reach ScrapeRack at {_address}: {error.reason}"
        ) from error

    response = cloudpickle.loads(body)
    if not response["ok"]:
        raise RemoteError(response["traceback"])
    return cloudpickle.loads(response["value"])


class Function:
    def __init__(self, target, requirements, working_dir):
        self.target = target
        self.requirements = requirements
        self.working_dir = working_dir
        update_wrapper(self, target)

    def __call__(self, *args, **kwargs):
        return invoke(self.target, self.requirements, self.working_dir, *args, **kwargs)


def function(target=None, *, requirements=(), working_dir=None):
    if isinstance(requirements, str):
        raise TypeError("requirements must be a collection of non-empty strings")
    requirements = tuple(requirements)
    if any(
        not isinstance(requirement, str) or not requirement.strip()
        for requirement in requirements
    ):
        raise TypeError("requirements must be a collection of non-empty strings")
    working_dir = resolve(working_dir) if working_dir is not None else None

    def decorate(target):
        module = inspect.getmodule(target)
        if module and module.__name__ != "__main__":
            cloudpickle.register_pickle_by_value(module)
        return Function(target, tuple(requirements), working_dir)

    return decorate if target is None else decorate(target)
