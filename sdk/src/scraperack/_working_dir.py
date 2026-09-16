import hashlib
import os
import tempfile
import urllib.error
import urllib.request
import warnings
import zipfile
from pathlib import Path
from threading import Lock

EXCLUDED_NAMES = {
    ".env",
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".tox",
    ".venv",
    "agents.md",
    "__pycache__",
    "credentials.txt",
    "venv",
}
_cache = {}
_cache_lock = Lock()
_directory_locks = {}


def resolve(directory):
    try:
        path = Path(directory).expanduser().resolve(strict=True)
    except (OSError, TypeError) as error:
        raise ValueError(f"working_dir does not exist: {directory}") from error
    if not path.is_dir():
        raise ValueError(f"working_dir is not a directory: {directory}")
    return path


def upload(address, directory, warning_bytes):
    with _cache_lock:
        lock = _directory_locks.setdefault(directory, Lock())
    with lock:
        return _upload(address, directory, warning_bytes)


def _upload(address, directory, warning_bytes):
    files = _files(directory)
    snapshot = tuple(
        (
            path.relative_to(directory).as_posix(),
            path.stat().st_size,
            path.stat().st_mtime_ns,
        )
        for path in files
    )
    size = sum(item[1] for item in snapshot)
    if warning_bytes is not None and size > warning_bytes:
        warnings.warn(
            f"working_dir is {size / 1024 / 1024:.1f} MiB; "
            f"configured warning threshold is {warning_bytes / 1024 / 1024:.1f} MiB",
            stacklevel=5,
        )
    with _cache_lock:
        cached = _cache.get(directory)
    digest = (
        cached[1] if cached and cached[0] == snapshot else _digest(directory, files)
    )
    url = f"{address}/working-dirs/{digest}.zip"

    try:
        urllib.request.urlopen(urllib.request.Request(url, method="HEAD")).close()
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise RuntimeError(
                f"Cannot check working_dir cache: HTTP {error.code}"
            ) from error
        archive = _archive(directory, files)
        try:
            with archive.open("rb") as body:
                request = urllib.request.Request(
                    url,
                    data=body,
                    headers={
                        "content-type": "application/zip",
                        "content-length": str(archive.stat().st_size),
                    },
                    method="PUT",
                )
                urllib.request.urlopen(request).close()
        except urllib.error.HTTPError as upload_error:
            raise RuntimeError(
                f"Cannot upload working_dir: HTTP {upload_error.code}"
            ) from upload_error
        except urllib.error.URLError as upload_error:
            raise RuntimeError(
                f"Cannot reach ScrapeRack at {address}: {upload_error.reason}"
            ) from upload_error
        finally:
            archive.unlink(missing_ok=True)
    except urllib.error.URLError as error:
        raise RuntimeError(
            f"Cannot reach ScrapeRack at {address}: {error.reason}"
        ) from error

    with _cache_lock:
        _cache[directory] = (snapshot, digest)
    return digest


def _files(directory):
    return [
        path
        for path in sorted(directory.rglob("*"))
        if path.is_file()
        and not path.is_symlink()
        and not EXCLUDED_NAMES.intersection(
            part.lower() for part in path.relative_to(directory).parts
        )
    ]


def _digest(directory, files):
    digest = hashlib.sha256()
    for path in files:
        name = path.relative_to(directory).as_posix().encode()
        size = path.stat().st_size
        digest.update(len(name).to_bytes(8, "big"))
        digest.update(name)
        digest.update(size.to_bytes(8, "big"))
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def _archive(directory, files):
    descriptor, name = tempfile.mkstemp(suffix=".zip")
    os.close(descriptor)
    path = Path(name)
    try:
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
            for file in files:
                archive.write(file, file.relative_to(directory).as_posix())
        return path
    except Exception:
        path.unlink(missing_ok=True)
        raise
