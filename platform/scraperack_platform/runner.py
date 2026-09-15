import traceback

from ray import cloudpickle


def run(function, arguments):
    try:
        target = cloudpickle.loads(function)
        args, kwargs = cloudpickle.loads(arguments)
        return {"ok": True, "value": cloudpickle.dumps(target(*args, **kwargs))}
    except Exception:  # noqa: BLE001 - return user-code failures to the caller
        return {"ok": False, "traceback": traceback.format_exc()}
