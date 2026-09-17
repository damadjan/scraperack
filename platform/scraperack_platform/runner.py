import cloudpickle


def run(function, arguments):
    target = cloudpickle.loads(function)
    args, kwargs = cloudpickle.loads(arguments)
    return {"ok": True, "value": cloudpickle.dumps(target(*args, **kwargs))}
