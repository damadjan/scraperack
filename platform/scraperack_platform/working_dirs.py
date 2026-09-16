import hashlib
import os
import re
import stat
import zipfile
from pathlib import Path, PurePosixPath

DIGEST = re.compile(r"[0-9a-f]{64}")


def path_for(digest):
    if not DIGEST.fullmatch(digest):
        raise ValueError("Invalid working_dir digest")
    root = Path(
        os.getenv(
            "SCRAPERACK_WORKING_DIR_CACHE",
            "/home/scraperack/.cache/working-dirs",
        )
    )
    root.mkdir(parents=True, exist_ok=True)
    return root / f"{digest}.zip"


def url_for(digest):
    base = os.getenv(
        "SCRAPERACK_WORKING_DIR_BASE_URL",
        "http://gateway:8080/working-dirs",
    ).rstrip("/")
    return f"{base}/{digest}.zip"


def validate(archive, expected_digest):
    digest = hashlib.sha256()
    names = set()

    try:
        with zipfile.ZipFile(archive) as bundle:
            files = sorted(
                (item for item in bundle.infolist() if not item.is_dir()),
                key=lambda item: item.filename,
            )
            for item in files:
                name = item.filename.replace("\\", "/")
                parts = PurePosixPath(name).parts
                if (
                    not name
                    or name.startswith("/")
                    or ".." in parts
                    or name in names
                    or stat.S_ISLNK(item.external_attr >> 16)
                ):
                    raise ValueError("Unsafe working_dir archive")
                names.add(name)

                encoded = name.encode()
                digest.update(len(encoded).to_bytes(8, "big"))
                digest.update(encoded)
                digest.update(item.file_size.to_bytes(8, "big"))
                with bundle.open(item) as source:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        digest.update(chunk)
    except zipfile.BadZipFile as error:
        raise ValueError("Invalid working_dir archive") from error

    if digest.hexdigest() != expected_digest:
        raise ValueError("working_dir digest does not match its contents")
