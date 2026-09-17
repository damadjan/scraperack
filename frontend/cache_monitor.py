from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

from watchdog.events import FileMovedEvent, FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer


@dataclass(frozen=True)
class CacheSnapshot:
    files: int
    bytes: int
    updated_at: datetime | None
    version: int


class CacheEvents(FileSystemEventHandler):
    def __init__(self, monitor):
        self.monitor = monitor

    def on_any_event(self, event: FileSystemEvent):
        if event.is_directory:
            if isinstance(event, FileMovedEvent):
                self.monitor.remove_tree(event.src_path)
                self.monitor.update_tree(event.dest_path)
            elif event.event_type == "deleted":
                self.monitor.remove_tree(event.src_path)
            return
        if isinstance(event, FileMovedEvent):
            self.monitor.remove(event.src_path)
            self.monitor.update(event.dest_path)
        elif event.event_type == "deleted":
            self.monitor.remove(event.src_path)
        elif event.event_type not in {"opened", "closed_no_write"}:
            self.monitor.update(event.src_path)


class CacheMonitor:
    def __init__(self, path):
        self.path = Path(path).resolve()
        self._entries = {}
        self._bytes = 0
        self._updated_at = None
        self._version = 0
        self._lock = Lock()
        self._observer = None

    def scan(self):
        entries = self._read_tree(self.path)
        with self._lock:
            if entries != self._entries:
                self._entries = entries
                self._bytes = sum(entry[0] for entry in entries.values())
                self._changed()

    def update_tree(self, path):
        entries = self._read_tree(Path(path))
        with self._lock:
            changed = False
            for entry_path, entry in entries.items():
                previous = self._entries.get(entry_path)
                if previous != entry:
                    self._entries[entry_path] = entry
                    self._bytes += entry[0] - (previous[0] if previous else 0)
                    changed = True
            if changed:
                self._changed()

    def remove_tree(self, path):
        path = Path(path)
        with self._lock:
            removed = [
                entry
                for entry in self._entries
                if entry == path or path in entry.parents
            ]
            if removed:
                for entry in removed:
                    self._bytes -= self._entries[entry][0]
                    del self._entries[entry]
                self._changed()

    def _read_tree(self, root):
        entries = {}
        for path in root.rglob("*"):
            try:
                if path.is_file() and not path.is_symlink():
                    stat = path.stat()
                    entries[path] = (stat.st_size, stat.st_mtime_ns)
            except OSError:
                continue
        return entries

    def update(self, path):
        path = Path(path).resolve()
        if not path.is_relative_to(self.path):
            return
        try:
            if not path.is_file() or path.is_symlink():
                return
            stat = path.stat()
            entry = (stat.st_size, stat.st_mtime_ns)
        except OSError:
            self.remove(path)
            return
        with self._lock:
            previous = self._entries.get(path)
            if previous != entry:
                self._entries[path] = entry
                self._bytes += entry[0] - (previous[0] if previous else 0)
                self._changed()

    def remove(self, path):
        path = Path(path)
        with self._lock:
            entry = self._entries.pop(path, None)
            if entry is not None:
                self._bytes -= entry[0]
                self._changed()

    def start(self):
        self.scan()
        observer = Observer()
        observer.schedule(CacheEvents(self), str(self.path), recursive=True)
        observer.start()
        self._observer = observer

    def stop(self):
        if self._observer:
            self._observer.stop()
            self._observer.join()

    def snapshot(self):
        with self._lock:
            return CacheSnapshot(
                files=len(self._entries),
                bytes=self._bytes,
                updated_at=self._updated_at,
                version=self._version,
            )

    def _changed(self):
        self._version += 1
        self._updated_at = datetime.now(timezone.utc)
