import asyncio
import base64
import hashlib
import io
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

import cloudpickle
import httpx

from .types import Environment, Execution, Image, ImageView, MicroVM, Node, Pool, PortSpec


class RemoteError(RuntimeError):
    """An execution failed in the remote environment."""


def _payload(
    function: Callable[..., Any], params: Sequence[Any], working_dir: str | None
) -> dict[str, Any]:
    data = cloudpickle.dumps(function)
    payload = {
        "encoding": "python-function",
        "hash": hashlib.sha256(data).hexdigest(),
        "function": base64.b64encode(data).decode(),
        "params": base64.b64encode(cloudpickle.dumps(tuple(params))).decode(),
    }
    if working_dir:
        payload["working_dir"] = working_dir
        payload["cache_key"] = hashlib.sha256(data + working_dir.encode()).hexdigest()
    return payload


def _archive(path: str | Path) -> tuple[str, bytes]:
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


def _decode(value: Mapping[str, Any] | None) -> Any:
    if value is None:
        return None
    if value["encoding"] == "json":
        return value["data"]
    if value["encoding"] == "cloudpickle":
        return cloudpickle.loads(base64.b64decode(value["data"]))
    raise ValueError(f'unknown result encoding: {value["encoding"]}')


class Cluster:
    """Synchronous client for a Scraperack controller."""

    def __init__(self, url: str = "http://127.0.0.1:7070") -> None:
        """Connect to the controller at ``url``."""
        self.client = httpx.Client(base_url=url.rstrip("/"))

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self.client.close()

    def import_image(
        self,
        name: str,
        reference: str,
        username: str | None = None,
        password: str | None = None,
        kind: str | None = None,
    ) -> Image:
        """Import and convert a Docker image from a registry."""
        return self._post("/v1/images/import", {
            "name": name,
            "reference": reference,
            "username": username,
            "password": password,
            "kind": kind,
        })

    def images(self) -> list[ImageView]:
        """Return all golden images and their distribution state."""
        return self._get("/v1/images")

    def delete_image(self, image: str) -> None:
        """Delete an image by ID."""
        self._delete(f"/v1/images/{image}")

    def create_pool(
        self,
        name: str,
        image: str,
        cpu: int | float = 1,
        memory_mib: int = 512,
        disk_mib: int = 1024,
        timeout: int = 300,
        warm: int = 0,
        max_instances: int | None = None,
        idle_timeout: int = 30,
        reuse: bool = True,
        network: bool = False,
        ports: Sequence[PortSpec] = (),
    ) -> Pool:
        """Create a pool with execution, concurrency, and idle-VM settings."""
        return self._post("/v1/pools", {
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
            "ports": ports,
        })

    def create_environment(
        self,
        name: str,
        image: str,
        packages: Sequence[str] = (),
        cpu: int | float = 1,
        memory_mib: int = 512,
        disk_mib: int = 1024,
        timeout: int = 300,
        warm: int = 0,
        max_instances: int | None = None,
        idle_timeout: int = 30,
        reuse: bool = True,
        network: bool = True,
        preload: Sequence[str] = (),
    ) -> Environment:
        """Create a cached Python execution environment."""
        return self._post("/v1/environments", {
            "name": name,
            "image": image,
            "packages": list(packages),
            "preload": list(preload),
            "warm_instances": warm,
            "max_instances": max_instances,
            "cpu_millis": round(cpu * 1000),
            "memory_mib": memory_mib,
            "disk_mib": disk_mib,
            "timeout_secs": timeout,
            "idle_timeout_secs": idle_timeout,
            "reuse": reuse,
            "network": network,
        })

    def environments(self) -> list[Environment]:
        """Return Python execution environments and cache state."""
        return self._get("/v1/environments")

    def delete_environment(self, environment: str) -> None:
        """Delete an inactive execution environment."""
        self._delete(f"/v1/environments/{environment}")

    def run(
        self,
        environment: str,
        function: Callable[..., Any],
        params: Sequence[Any] = (),
        queue: bool = True,
        variables: Mapping[str, str] | None = None,
        secrets: Mapping[str, str] | None = None,
        working_dir: str | Path | None = None,
    ) -> Execution:
        """Submit a Python function, its arguments, and optional project directory."""
        digest = self._upload_working_dir(working_dir) if working_dir else None
        return self._submit(
            environment,
            _payload(function, params, digest),
            queue,
            variables,
            secrets,
        )

    def delete_pool(self, name: str) -> None:
        """Delete an inactive pool."""
        self._delete(f"/v1/pools/{name}")

    def _submit(
        self,
        environment: str,
        payload: Mapping[str, Any],
        queue: bool = True,
        variables: Mapping[str, str] | None = None,
        secrets: Mapping[str, str] | None = None,
    ) -> Execution:
        return self._post(
            "/v1/executions",
            {
                "environment": environment,
                "payload": payload,
                "queue": queue,
                "variables": dict(variables or {}),
                "secrets": dict(secrets or {}),
            },
        )

    def _upload_working_dir(self, path: str | Path) -> str:
        digest, archive = _archive(path)
        response = self.client.head(f"/v1/working-dirs/{digest}")
        if response.status_code == 404:
            response = self.client.put(
                f"/v1/working-dirs/{digest}",
                content=archive,
                headers={"Content-Type": "application/zip"},
            )
        response.raise_for_status()
        return digest

    def execution(self, execution_id: str) -> Execution:
        """Return one execution by ID."""
        return self._get(f"/v1/executions/{execution_id}")

    def wait(self, execution_id: str, interval: float = 0.1) -> Execution:
        """Wait until an execution succeeds, fails, or times out."""
        while True:
            response = self.client.get(f"/v1/executions/{execution_id}/wait", timeout=None)
            response.raise_for_status()
            execution = response.json()
            if execution["status"] in {"succeeded", "failed", "timed_out"}:
                return execution

    def result(self, execution_id: str) -> Any:
        """Wait for an execution and return its decoded value."""
        self.wait(execution_id)
        outcome = self._get(f"/v1/executions/{execution_id}/result")
        if outcome["status"] != "succeeded":
            raise RemoteError(outcome["error"] or f'execution {outcome["status"]}')
        return _decode(outcome["value"])

    def logs(self, execution_id: str) -> dict[str, str]:
        """Return captured stdout and stderr for an execution."""
        return self._get(f"/v1/executions/{execution_id}/logs")

    def nodes(self) -> list[Node]:
        """Return all registered worker nodes."""
        return self._get("/v1/nodes")

    def vms(self) -> list[MicroVM]:
        """Return live microVMs and their published ports."""
        return self._get("/v1/vms")

    def executions(self) -> list[Execution]:
        """Return all executions."""
        return self._get("/v1/executions")

    def _get(self, path: str) -> Any:
        response = self.client.get(path)
        response.raise_for_status()
        return response.json()

    def _post(self, path: str, data: Mapping[str, Any]) -> Any:
        response = self.client.post(path, json=data)
        response.raise_for_status()
        return response.json()

    def _delete(self, path: str) -> None:
        response = self.client.delete(path)
        response.raise_for_status()


class AsyncCluster:
    """Asynchronous client for a Scraperack controller."""

    def __init__(self, url: str = "http://127.0.0.1:7070") -> None:
        """Connect to the controller at ``url``."""
        self.client = httpx.AsyncClient(base_url=url.rstrip("/"))

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self.client.aclose()

    async def import_image(
        self,
        name: str,
        reference: str,
        username: str | None = None,
        password: str | None = None,
        kind: str | None = None,
    ) -> Image:
        """Import and convert a Docker image from a registry."""
        return await self._post("/v1/images/import", {
            "name": name,
            "reference": reference,
            "username": username,
            "password": password,
            "kind": kind,
        })

    async def create_pool(
        self,
        name: str,
        image: str,
        cpu: int | float = 1,
        memory_mib: int = 512,
        disk_mib: int = 1024,
        timeout: int = 300,
        warm: int = 0,
        max_instances: int | None = None,
        idle_timeout: int = 30,
        reuse: bool = True,
        network: bool = False,
        ports: Sequence[PortSpec] = (),
    ) -> Pool:
        """Create a pool with execution, concurrency, and idle-VM settings."""
        return await self._post("/v1/pools", {
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
            "ports": ports,
        })

    async def create_environment(
        self,
        name: str,
        image: str,
        packages: Sequence[str] = (),
        cpu: int | float = 1,
        memory_mib: int = 512,
        disk_mib: int = 1024,
        timeout: int = 300,
        warm: int = 0,
        max_instances: int | None = None,
        idle_timeout: int = 30,
        reuse: bool = True,
        network: bool = True,
        preload: Sequence[str] = (),
    ) -> Environment:
        """Create a cached Python execution environment."""
        return await self._post("/v1/environments", {
            "name": name,
            "image": image,
            "packages": list(packages),
            "preload": list(preload),
            "warm_instances": warm,
            "max_instances": max_instances,
            "cpu_millis": round(cpu * 1000),
            "memory_mib": memory_mib,
            "disk_mib": disk_mib,
            "timeout_secs": timeout,
            "idle_timeout_secs": idle_timeout,
            "reuse": reuse,
            "network": network,
        })

    async def environments(self) -> list[Environment]:
        """Return Python execution environments and cache state."""
        return await self._get("/v1/environments")

    async def delete_environment(self, environment: str) -> None:
        """Delete an inactive execution environment."""
        await self._delete(f"/v1/environments/{environment}")

    async def delete_image(self, image: str) -> None:
        """Delete an image by ID."""
        await self._delete(f"/v1/images/{image}")

    async def delete_pool(self, name: str) -> None:
        """Delete an inactive pool."""
        await self._delete(f"/v1/pools/{name}")

    async def run(
        self,
        environment: str,
        function: Callable[..., Any],
        params: Sequence[Any] = (),
        queue: bool = True,
        variables: Mapping[str, str] | None = None,
        secrets: Mapping[str, str] | None = None,
        working_dir: str | Path | None = None,
    ) -> Execution:
        """Submit a Python function, its arguments, and optional project directory."""
        digest = await self._upload_working_dir(working_dir) if working_dir else None
        return await self._submit(
            environment,
            _payload(function, params, digest),
            queue,
            variables,
            secrets,
        )

    async def _submit(
        self,
        environment: str,
        payload: Mapping[str, Any],
        queue: bool = True,
        variables: Mapping[str, str] | None = None,
        secrets: Mapping[str, str] | None = None,
    ) -> Execution:
        return await self._post("/v1/executions", {
            "environment": environment,
            "payload": payload,
            "queue": queue,
            "variables": dict(variables or {}),
            "secrets": dict(secrets or {}),
        })

    async def _upload_working_dir(self, path: str | Path) -> str:
        digest, archive = await asyncio.to_thread(_archive, path)
        response = await self.client.head(f"/v1/working-dirs/{digest}")
        if response.status_code == 404:
            response = await self.client.put(
                f"/v1/working-dirs/{digest}",
                content=archive,
                headers={"Content-Type": "application/zip"},
            )
        response.raise_for_status()
        return digest

    async def execution(self, execution_id: str) -> Execution:
        """Return one execution by ID."""
        return await self._get(f"/v1/executions/{execution_id}")

    async def wait(self, execution_id: str, interval: float = 0.1) -> Execution:
        """Wait until an execution succeeds, fails, or times out."""
        while True:
            response = await self.client.get(f"/v1/executions/{execution_id}/wait", timeout=None)
            response.raise_for_status()
            execution = response.json()
            if execution["status"] in {"succeeded", "failed", "timed_out"}:
                return execution

    async def result(self, execution_id: str) -> Any:
        """Wait for an execution and return its decoded value."""
        await self.wait(execution_id)
        outcome = await self._get(f"/v1/executions/{execution_id}/result")
        if outcome["status"] != "succeeded":
            raise RemoteError(outcome["error"] or f'execution {outcome["status"]}')
        return _decode(outcome["value"])

    async def logs(self, execution_id: str) -> dict[str, str]:
        """Return captured stdout and stderr for an execution."""
        return await self._get(f"/v1/executions/{execution_id}/logs")

    async def vms(self) -> list[MicroVM]:
        """Return live microVMs and their published ports."""
        return await self._get("/v1/vms")

    async def _get(self, path: str) -> Any:
        response = await self.client.get(path)
        response.raise_for_status()
        return response.json()

    async def _post(self, path: str, data: Mapping[str, Any]) -> Any:
        response = await self.client.post(path, json=data)
        response.raise_for_status()
        return response.json()

    async def _delete(self, path: str) -> None:
        response = await self.client.delete(path)
        response.raise_for_status()
