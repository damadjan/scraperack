import os
import traceback
from functools import cache
from threading import Lock
from typing import Annotated

import cloudpickle
from fastapi import Body, Depends, FastAPI, HTTPException, Response

from scraperack_platform.runner import run

CONTENT_TYPE = "application/vnd.scraperack.function"
connection_lock = Lock()
app = FastAPI(title="ScrapeRack", version="0.1.0")


@cache
def get_ray():
    import ray

    with connection_lock:
        if not ray.is_initialized():
            ray.init(os.getenv("RAY_ADDRESS", "ray://control-plane:10001"))
    return ray


@app.get("/health")
def health(ray: Annotated[object, Depends(get_ray)]):
    ray.nodes()
    return {"status": "ok"}


@app.post("/invoke")
def invoke(
    payload: Annotated[bytes, Body(media_type=CONTENT_TYPE)],
    ray: Annotated[object, Depends(get_ray)],
):
    max_size = int(os.getenv("SCRAPERACK_MAX_PAYLOAD_BYTES", str(16 * 1024 * 1024)))
    if len(payload) > max_size:
        raise HTTPException(413, f"Invocation exceeds the {max_size}-byte limit")

    try:
        invocation = cloudpickle.loads(payload)
        function = invocation["function"]
        arguments = invocation["arguments"]
        requirements = invocation["requirements"]
        if (
            not isinstance(function, bytes)
            or not isinstance(arguments, bytes)
            or not isinstance(requirements, list)
            or any(
                not isinstance(requirement, str) or not requirement.strip()
                for requirement in requirements
            )
        ):
            raise TypeError
    except Exception as error:
        raise HTTPException(400, "Invalid invocation payload") from error

    try:
        remote = ray.remote(run)
        if requirements:
            remote = remote.options(runtime_env={"pip": requirements})
        result = ray.get(remote.remote(function, arguments))
        status_code = 200 if result["ok"] else 500
    except Exception:  # noqa: BLE001 - transport remote task failures to the caller
        result = {"ok": False, "traceback": traceback.format_exc()}
        status_code = 500

    return Response(
        cloudpickle.dumps(result),
        status_code=status_code,
        media_type=CONTENT_TYPE,
    )
