import os
import traceback
import uuid
from functools import cache
from threading import Lock
from typing import Annotated

import cloudpickle
from fastapi import Body, Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse

from scraperack_platform.runner import run
from scraperack_platform.working_dirs import path_for, url_for, validate

CONTENT_TYPE = "application/vnd.scraperack.function"
connection_lock = Lock()
app = FastAPI(title="ScrapeRack", version="0.1.0")


def pip_cache_enabled():
    value = os.getenv("RAY_PIP_CACHING", "true").strip().lower()
    if value not in {"true", "false"}:
        raise RuntimeError("RAY_PIP_CACHING must be set to true or false")
    return value == "true"


def pip_runtime_env(requirements):
    pip = requirements
    if pip_cache_enabled():
        pip = {
            "packages": requirements,
            "pip_install_options": ["--disable-pip-version-check"],
        }
    return {"pip": pip}


def runtime_env(requirements, working_dir):
    environment = pip_runtime_env(requirements) if requirements else {}
    if working_dir:
        environment["working_dir"] = url_for(working_dir)
    return environment


@cache
def get_ray():
    import ray

    with connection_lock:
        if not ray.is_initialized():
            ray.init(os.getenv("RAY_ADDRESS", "ray://control-plane:10001"))
    return ray


@app.get("/health")
def health(ray: Annotated[object, Depends(get_ray)]):
    pip_cache_enabled()
    ray.nodes()
    return {"status": "ok"}


@app.head("/working-dirs/{digest}.zip")
def working_dir_exists(digest: str):
    try:
        archive = path_for(digest)
    except ValueError as error:
        raise HTTPException(400, str(error)) from error
    if not archive.is_file():
        raise HTTPException(404, "working_dir not found")
    return Response(headers={"content-length": str(archive.stat().st_size)})


@app.get("/working-dirs/{digest}.zip")
def download_working_dir(digest: str):
    try:
        archive = path_for(digest)
    except ValueError as error:
        raise HTTPException(400, str(error)) from error
    if not archive.is_file():
        raise HTTPException(404, "working_dir not found")
    return FileResponse(archive, media_type="application/zip")


@app.put("/working-dirs/{digest}.zip", status_code=201)
async def upload_working_dir(digest: str, request: Request):
    try:
        archive = path_for(digest)
    except ValueError as error:
        raise HTTPException(400, str(error)) from error
    if archive.is_file():
        return Response(status_code=204)

    temporary = archive.with_name(f".{digest}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("wb") as destination:
            async for chunk in request.stream():
                destination.write(chunk)
        validate(temporary, digest)
        os.replace(temporary, archive)
    except HTTPException:
        raise
    except (OSError, ValueError) as error:
        raise HTTPException(400, str(error)) from error
    finally:
        temporary.unlink(missing_ok=True)
    return Response(status_code=201)


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
        working_dir = invocation.get("working_dir")
        if (
            not isinstance(function, bytes)
            or not isinstance(arguments, bytes)
            or not isinstance(requirements, list)
            or any(
                not isinstance(requirement, str) or not requirement.strip()
                for requirement in requirements
            )
            or (working_dir is not None and not isinstance(working_dir, str))
        ):
            raise TypeError
        if working_dir and not path_for(working_dir).is_file():
            raise ValueError("working_dir has not been uploaded")
    except Exception as error:
        raise HTTPException(400, "Invalid invocation payload") from error

    try:
        remote = ray.remote(run)
        environment = runtime_env(requirements, working_dir)
        if environment:
            remote = remote.options(runtime_env=environment)
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
