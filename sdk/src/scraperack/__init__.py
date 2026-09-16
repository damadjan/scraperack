import inspect
import os
import urllib.error
import urllib.request
from functools import update_wrapper

import cloudpickle

CONTENT_TYPE = "application/vnd.scraperack.function"
_address = os.getenv("SCRAPERACK_ADDRESS", "http://127.0.0.1:42800")


class RemoteError(RuntimeError):
    pass


def configure(address):
    global _address
    _address = address.rstrip("/")


def invoke(target, requirements, *args, **kwargs):
    payload = cloudpickle.dumps(
        {
            "function": cloudpickle.dumps(target),
            "arguments": cloudpickle.dumps((args, kwargs)),
            "requirements": list(requirements),
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
    def __init__(self, target, requirements):
        self.target = target
        self.requirements = requirements
        update_wrapper(self, target)

    def __call__(self, *args, **kwargs):
        return invoke(self.target, self.requirements, *args, **kwargs)


def function(target=None, *, requirements=()):
    if isinstance(requirements, str):
        raise TypeError("requirements must be a collection of non-empty strings")
    requirements = tuple(requirements)
    if any(
        not isinstance(requirement, str) or not requirement.strip()
        for requirement in requirements
    ):
        raise TypeError("requirements must be a collection of non-empty strings")

    def decorate(target):
        module = inspect.getmodule(target)
        if module and module.__name__ != "__main__":
            cloudpickle.register_pickle_by_value(module)
        return Function(target, tuple(requirements))

    return decorate if target is None else decorate(target)
