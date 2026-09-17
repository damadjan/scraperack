import os
import re
import traceback
import uuid
from collections import OrderedDict
from functools import cache
from threading import Lock
from typing import Annotated

import cloudpickle
from fastapi import Body, Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse

from scraperack_platform.function_cache import enabled as function_cache_enabled
from scraperack_platform.function_cache import path_for as function_path_for
from scraperack_platform.function_cache import validate as validate_function
from scraperack_platform.runner import run
from scraperack_platform.working_dirs import path_for, url_for, validate

CONTENT_TYPE = "application/vnd.scraperack.function"
PROJECT_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
connection_lock = Lock()
invocation_lock = Lock()
invocation_cache_status = OrderedDict()
app = FastAPI(title="ScrapeRack", version="0.1.0")


def remember_cache_status(task_id, status):
    with invocation_lock:
        invocation_cache_status[task_id] = status
        invocation_cache_status.move_to_end(task_id)
        if len(invocation_cache_status) > 10_000:
            invocation_cache_status.popitem(last=False)


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
    function_cache_enabled()
    pip_cache_enabled()
    ray.nodes()
    return {"status": "ok"}


@app.get("/invocations/cache-status")
def cache_statuses():
    with invocation_lock:
        return dict(invocation_cache_status)


@app.head("/functions/{digest}")
def function_exists(digest: str):
    if not function_cache_enabled():
        return Response(status_code=204)
    try:
        function = function_path_for(digest)
    except ValueError as error:
        raise HTTPException(400, str(error)) from error
    if not function.is_file():
        raise HTTPException(404, "function not found")
    return Response(headers={"content-length": str(function.stat().st_size)})


@app.put("/functions/{digest}", status_code=201)
async def upload_function(digest: str, request: Request):
    if not function_cache_enabled():
        raise HTTPException(409, "function caching is disabled")
    try:
        function = function_path_for(digest)
    except ValueError as error:
        raise HTTPException(400, str(error)) from error
    if function.is_file():
        return Response(status_code=204)

    body = await request.body()
    temporary = function.with_name(f".{digest}.{uuid.uuid4().hex}.tmp")
    try:
        validate_function(body, digest)
        temporary.write_bytes(body)
        os.replace(temporary, function)
    except (OSError, ValueError) as error:
        raise HTTPException(400, str(error)) from error
    finally:
        temporary.unlink(missing_ok=True)
    return Response(status_code=201)


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
        function_name = invocation.get("function_name")
        project = invocation.get("project")
        arguments = invocation["arguments"]
        requirements = invocation["requirements"]
        working_dir = invocation.get("working_dir")
        cache_status = invocation.get("cache", {})
        if (
            not isinstance(function, (bytes, str))
            or (
                function_name is not None
                and (
                    not isinstance(function_name, str)
                    or not function_name
                    or "/" in function_name
                )
            )
            or (project is not None and not PROJECT_PATTERN.fullmatch(project))
            or (project is not None and function_name is None)
            or not isinstance(arguments, bytes)
            or not isinstance(requirements, list)
            or any(
                not isinstance(requirement, str) or not requirement.strip()
                for requirement in requirements
            )
            or (working_dir is not None and not isinstance(working_dir, str))
            or not isinstance(cache_status, dict)
        ):
            raise TypeError
        cache_status = {
            "function_cache": cache_status.get("function", "unknown"),
            "working_dir_cache": cache_status.get(
                "working_dir", "none" if working_dir is None else "unknown"
            ),
            "function_name": function_name,
            "project": project,
        }
        if cache_status["function_cache"] not in {
            "hit",
            "miss",
            "disabled",
            "unknown",
        }:
            raise ValueError("invalid function cache status")
        if cache_status["working_dir_cache"] not in {
            "hit",
            "miss",
            "none",
            "unknown",
        }:
            raise ValueError("invalid working_dir cache status")
        if isinstance(function, str):
            if not function_cache_enabled():
                raise ValueError("function caching is disabled")
            function = function_path_for(function).read_bytes()
        if working_dir and not path_for(working_dir).is_file():
            raise ValueError("working_dir has not been uploaded")
    except Exception as error:
        raise HTTPException(400, "Invalid invocation payload") from error

    try:
        remote = ray.remote(run)
        environment = runtime_env(requirements, working_dir)
        options = {}
        if function_name:
            options["name"] = f"{project}/{function_name}" if project else function_name
        if environment:
            options["runtime_env"] = environment
        if options:
            remote = remote.options(**options)
        reference = remote.remote(function, arguments)
        remember_cache_status(reference.task_id().hex(), cache_status)
        result = ray.get(reference)
        status_code = 200 if result["ok"] else 500
    except Exception:  # noqa: BLE001 - transport remote task failures to the caller
        result = {"ok": False, "traceback": traceback.format_exc()}
        status_code = 500

    return Response(
        cloudpickle.dumps(result),
        status_code=status_code,
        media_type=CONTENT_TYPE,
    )
