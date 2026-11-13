"""Clients for the ScrapeRack core API: microVMs, pools, images, disks and tasks.

Language plugins ship their own SDKs on top of this one; for Python code see
``scraperack-python``.
"""
import asyncio
import hashlib
import io
from pathlib import Path
from typing import Any, BinaryIO, Mapping, Sequence
from urllib.parse import quote
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

import httpx

from .types import (
    FINISHED,
    DiskView,
    Disk,
    Image,
    ImageView,
    MicroVM,
    Node,
    Plugin,
    Pool,
    PoolView,
    StorageSettings,
    Task,
    TaskLogs,
    TaskResult,
)

DEFAULT_URL = "http://127.0.0.1:7070"


class APIError(RuntimeError):
    """The controller rejected a request."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"{status}: {message}")
        self.status = status
        self.message = message


def _check(response: httpx.Response) -> Any:
    if response.status_code >= 400:
        try:
            message = response.json()["error"]
        except Exception:
            message = response.text or response.reason_phrase
        raise APIError(response.status_code, message)
    if not response.content:
        return None
    return response.json()


def _segment(value: str) -> str:
    return quote(str(value), safe="")


def archive_directory(path: str | Path) -> tuple[str, bytes]:
    """Zip a directory reproducibly and return its SHA-256 and bytes."""
    root = Path(path).resolve()
    if not root.is_dir():
        raise ValueError(f"working directory does not exist: {path}")
    output = io.BytesIO()
    with ZipFile(output, "w") as archive:
        for file in sorted(root.rglob("*")):
            if file.is_symlink():
                raise ValueError(f"working directory contains a symlink: {file}")
            if not file.is_file():
                continue
            info = ZipInfo(file.relative_to(root).as_posix(), (1980, 1, 1, 0, 0, 0))
            info.compress_type = ZIP_DEFLATED
            archive.writestr(info, file.read_bytes())
    data = output.getvalue()
    return hashlib.sha256(data).hexdigest(), data


def _pool_body(
    name: str,
    image: str,
    cpu: float,
    memory_mib: int,
    disk_mib: int,
    timeout: int,
    warm: int,
    max_instances: int | None,
    idle_timeout: int,
    reuse: bool,
    network: bool,
    ports: Sequence[Mapping[str, Any]],
    extra: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "name": name,
        "image": image,
        "warm_instances": warm,
        "max_instances": max_instances,
        "cpu_millis": round(cpu * 1000),
        "memory_mib": memory_mib,
        "disk_mib": disk_mib,
        "timeout_secs": timeout,
        "idle_timeout_secs": idle_timeout,
        "reuse": reuse,
        "network": network,
        "ports": list(ports),
        **extra,
    }


def _task_body(
    pool: str,
    payload: Mapping[str, Any],
    queue: bool,
    variables: Mapping[str, str] | None,
    secrets: Mapping[str, str] | None,
) -> dict[str, Any]:
    return {
        "pool": pool,
        "payload": dict(payload),
        "queue": queue,
        "variables": dict(variables or {}),
        "secrets": dict(secrets or {}),
    }


def _task_query(limit: int | None, pool: str | Sequence[str] | None, owner: str | None) -> dict:
    params: dict[str, Any] = {}
    if limit is not None:
        params["limit"] = limit
    if pool is not None:
        params["pool"] = pool if isinstance(pool, str) else ",".join(pool)
    if owner is not None:
        params["owner"] = owner
    return params


class Cluster:
    """Synchronous client for a ScrapeRack controller."""

    def __init__(self, url: str = DEFAULT_URL, *, client: httpx.Client | None = None) -> None:
        self.url = url.rstrip("/")
        self.client = client or httpx.Client(base_url=self.url, timeout=60)

    def __enter__(self) -> "Cluster":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self.client.close()

    def request(self, method: str, path: str, **kwargs: Any) -> Any:
        """Call any controller endpoint, including plugin APIs under ``/plugins/<name>/api``."""
        return _check(self.client.request(method, path, **kwargs))

    # Images
    def images(self) -> list[ImageView]:
        return self.request("GET", "/v1/images")

    def import_image(self, name: str, reference: str, username: str | None = None,
                     password: str | None = None) -> Image:
        """Import a Docker image from a registry; blocks until it is converted."""
        return self.request("POST", "/v1/images/import", timeout=None, json={
            "name": name, "reference": reference, "username": username, "password": password,
        })

    def rename_image(self, image: str, name: str) -> Image:
        return self.request("PATCH", f"/v1/images/{_segment(image)}", json={"name": name})

    def delete_image(self, image: str) -> None:
        self.request("DELETE", f"/v1/images/{_segment(image)}")

    # Pools
    def pools(self) -> list[PoolView]:
        return self.request("GET", "/v1/pools")

    def pool(self, name: str) -> Pool:
        return self.request("GET", f"/v1/pools/{_segment(name)}")

    def create_pool(self, name: str, image: str, cpu: float = 1, memory_mib: int = 512,
                    disk_mib: int = 1024, timeout: int = 300, warm: int = 0,
                    max_instances: int | None = None, idle_timeout: int = 30, reuse: bool = True,
                    network: bool = False, ports: Sequence[Mapping[str, Any]] = (),
                    **extra: Any) -> Pool:
        """Create a pool. ``extra`` passes other pool fields such as ``disks`` or ``executor``."""
        return self.request("POST", "/v1/pools", json=_pool_body(
            name, image, cpu, memory_mib, disk_mib, timeout, warm, max_instances,
            idle_timeout, reuse, network, ports, extra))

    def update_pool(self, name: str, **changes: Any) -> Pool:
        """Change pool fields; VM-affecting changes replace idle microVMs."""
        pool = {**self.pool(name), **changes}
        return self.request("PUT", f"/v1/pools/{_segment(name)}", json=pool)

    def restart_pool(self, name: str) -> Pool:
        return self.request("POST", f"/v1/pools/{_segment(name)}/restart")

    def delete_pool(self, name: str) -> None:
        self.request("DELETE", f"/v1/pools/{_segment(name)}")

    # Disks
    def disks(self) -> list[DiskView]:
        return self.request("GET", "/v1/disks")

    def upload_disk(self, file: str | Path | BinaryIO) -> Disk:
        """Upload a read-only ext4 filesystem; nodes download it on demand."""
        if isinstance(file, (str, Path)):
            with open(file, "rb") as handle:
                return self.request("POST", "/v1/disks", content=handle, timeout=None)
        return self.request("POST", "/v1/disks", content=file, timeout=None)

    def delete_disk(self, key: str) -> None:
        self.request("DELETE", f"/v1/disks/{_segment(key)}")

    # Tasks
    def submit(self, pool: str, payload: Mapping[str, Any], queue: bool = True,
               variables: Mapping[str, str] | None = None,
               secrets: Mapping[str, str] | None = None) -> Task:
        """Queue an opaque payload for the pool's executor (pool name or alias)."""
        return self.request("POST", "/v1/tasks",
                            json=_task_body(pool, payload, queue, variables, secrets))

    def tasks(self, limit: int | None = None, pool: str | Sequence[str] | None = None,
              owner: str | None = None) -> list[Task]:
        """Newest tasks first, optionally only on some pools or of one owner."""
        return self.request("GET", "/v1/tasks", params=_task_query(limit, pool, owner))

    def task(self, task_id: str) -> Task:
        return self.request("GET", f"/v1/tasks/{_segment(task_id)}")

    def wait(self, task_id: str) -> Task:
        """Block until the task succeeds, fails, or times out."""
        while True:
            task = self.request("GET", f"/v1/tasks/{_segment(task_id)}/wait", timeout=None)
            if task["status"] in FINISHED:
                return task

    def result(self, task_id: str) -> TaskResult:
        """Wait for the task and return its status, encoded value and error."""
        self.wait(task_id)
        return self.request("GET", f"/v1/tasks/{_segment(task_id)}/result")

    def logs(self, task_id: str) -> TaskLogs:
        return self.request("GET", f"/v1/tasks/{_segment(task_id)}/logs")

    def payload(self, task_id: str) -> Any:
        return self.request("GET", f"/v1/tasks/{_segment(task_id)}/payload")

    def upload_working_dir(self, path: str | Path) -> str:
        """Upload a directory once (content-addressed) and return its digest."""
        digest, archive = archive_directory(path)
        response = self.client.head(f"/v1/working-dirs/{digest}")
        if response.status_code == 404:
            response = self.client.put(f"/v1/working-dirs/{digest}", content=archive,
                                       headers={"Content-Type": "application/zip"})
        _check(response)
        return digest

    # Inventory
    def vms(self) -> list[MicroVM]:
        return self.request("GET", "/v1/vms")

    def nodes(self) -> list[Node]:
        return self.request("GET", "/v1/nodes")

    def plugins(self) -> list[Plugin]:
        return self.request("GET", "/v1/plugins")

    def storage(self) -> StorageSettings:
        return self.request("GET", "/v1/settings/storage")


class AsyncCluster:
    """Asynchronous client for a ScrapeRack controller."""

    def __init__(self, url: str = DEFAULT_URL, *, client: httpx.AsyncClient | None = None) -> None:
        self.url = url.rstrip("/")
        self.client = client or httpx.AsyncClient(base_url=self.url, timeout=60)

    async def __aenter__(self) -> "AsyncCluster":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self.client.aclose()

    async def request(self, method: str, path: str, **kwargs: Any) -> Any:
        """Call any controller endpoint, including plugin APIs under ``/plugins/<name>/api``."""
        return _check(await self.client.request(method, path, **kwargs))

    # Images
    async def images(self) -> list[ImageView]:
        return await self.request("GET", "/v1/images")

    async def import_image(self, name: str, reference: str, username: str | None = None,
                           password: str | None = None) -> Image:
        return await self.request("POST", "/v1/images/import", timeout=None, json={
            "name": name, "reference": reference, "username": username, "password": password,
        })

    async def rename_image(self, image: str, name: str) -> Image:
        return await self.request("PATCH", f"/v1/images/{_segment(image)}", json={"name": name})

    async def delete_image(self, image: str) -> None:
        await self.request("DELETE", f"/v1/images/{_segment(image)}")

    # Pools
    async def pools(self) -> list[PoolView]:
        return await self.request("GET", "/v1/pools")

    async def pool(self, name: str) -> Pool:
        return await self.request("GET", f"/v1/pools/{_segment(name)}")

    async def create_pool(self, name: str, image: str, cpu: float = 1, memory_mib: int = 512,
                          disk_mib: int = 1024, timeout: int = 300, warm: int = 0,
                          max_instances: int | None = None, idle_timeout: int = 30,
                          reuse: bool = True, network: bool = False,
                          ports: Sequence[Mapping[str, Any]] = (), **extra: Any) -> Pool:
        return await self.request("POST", "/v1/pools", json=_pool_body(
            name, image, cpu, memory_mib, disk_mib, timeout, warm, max_instances,
            idle_timeout, reuse, network, ports, extra))

    async def update_pool(self, name: str, **changes: Any) -> Pool:
        pool = {**await self.pool(name), **changes}
        return await self.request("PUT", f"/v1/pools/{_segment(name)}", json=pool)

    async def restart_pool(self, name: str) -> Pool:
        return await self.request("POST", f"/v1/pools/{_segment(name)}/restart")

    async def delete_pool(self, name: str) -> None:
        await self.request("DELETE", f"/v1/pools/{_segment(name)}")

    # Disks
    async def disks(self) -> list[DiskView]:
        return await self.request("GET", "/v1/disks")

    async def upload_disk(self, file: str | Path) -> Disk:
        data = await asyncio.to_thread(Path(file).read_bytes)
        return await self.request("POST", "/v1/disks", content=data, timeout=None)

    async def delete_disk(self, key: str) -> None:
        await self.request("DELETE", f"/v1/disks/{_segment(key)}")

    # Tasks
    async def submit(self, pool: str, payload: Mapping[str, Any], queue: bool = True,
                     variables: Mapping[str, str] | None = None,
                     secrets: Mapping[str, str] | None = None) -> Task:
        return await self.request("POST", "/v1/tasks",
                                  json=_task_body(pool, payload, queue, variables, secrets))

    async def tasks(self, limit: int | None = None, pool: str | Sequence[str] | None = None,
                    owner: str | None = None) -> list[Task]:
        return await self.request("GET", "/v1/tasks", params=_task_query(limit, pool, owner))

    async def task(self, task_id: str) -> Task:
        return await self.request("GET", f"/v1/tasks/{_segment(task_id)}")

    async def wait(self, task_id: str) -> Task:
        while True:
            task = await self.request("GET", f"/v1/tasks/{_segment(task_id)}/wait", timeout=None)
            if task["status"] in FINISHED:
                return task

    async def result(self, task_id: str) -> TaskResult:
        await self.wait(task_id)
        return await self.request("GET", f"/v1/tasks/{_segment(task_id)}/result")

    async def logs(self, task_id: str) -> TaskLogs:
        return await self.request("GET", f"/v1/tasks/{_segment(task_id)}/logs")

    async def payload(self, task_id: str) -> Any:
        return await self.request("GET", f"/v1/tasks/{_segment(task_id)}/payload")

    async def upload_working_dir(self, path: str | Path) -> str:
        digest, archive = await asyncio.to_thread(archive_directory, path)
        response = await self.client.head(f"/v1/working-dirs/{digest}")
        if response.status_code == 404:
            response = await self.client.put(f"/v1/working-dirs/{digest}", content=archive,
                                             headers={"Content-Type": "application/zip"})
        _check(response)
        return digest

    # Inventory
    async def vms(self) -> list[MicroVM]:
        return await self.request("GET", "/v1/vms")

    async def nodes(self) -> list[Node]:
        return await self.request("GET", "/v1/nodes")

    async def plugins(self) -> list[Plugin]:
        return await self.request("GET", "/v1/plugins")

    async def storage(self) -> StorageSettings:
        return await self.request("GET", "/v1/settings/storage")
