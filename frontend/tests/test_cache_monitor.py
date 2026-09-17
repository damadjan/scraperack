import tempfile
import time
import unittest
from pathlib import Path

from watchdog.events import (
    DirDeletedEvent,
    FileCreatedEvent,
    FileDeletedEvent,
    FileMovedEvent,
)

from cache_monitor import CacheEvents, CacheMonitor


class CacheMonitorTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name)
        self.monitor = CacheMonitor(self.path)
        self.events = CacheEvents(self.monitor)

    def tearDown(self):
        self.directory.cleanup()

    def test_initial_scan_counts_existing_files(self):
        (self.path / "one").write_bytes(b"123")
        (self.path / "nested").mkdir()
        (self.path / "nested" / "two").write_bytes(b"4567")

        self.monitor.scan()

        snapshot = self.monitor.snapshot()
        self.assertEqual(snapshot.files, 2)
        self.assertEqual(snapshot.bytes, 7)
        self.assertIsNotNone(snapshot.updated_at)

    def test_file_events_update_cache_state(self):
        source = self.path / "source"
        source.write_bytes(b"123")
        self.events.on_any_event(FileCreatedEvent(str(source)))

        destination = self.path / "destination"
        source.rename(destination)
        self.events.on_any_event(FileMovedEvent(str(source), str(destination)))

        destination.unlink()
        self.events.on_any_event(FileDeletedEvent(str(destination)))

        snapshot = self.monitor.snapshot()
        self.assertEqual(snapshot.files, 0)
        self.assertEqual(snapshot.bytes, 0)
        self.assertEqual(snapshot.version, 4)

    def test_deleted_directory_removes_its_files(self):
        directory = self.path / "environment"
        directory.mkdir()
        (directory / "package.whl").write_bytes(b"123")
        self.monitor.scan()

        self.events.on_any_event(DirDeletedEvent(str(directory)))

        self.assertEqual(self.monitor.snapshot().files, 0)

    def test_observer_receives_real_filesystem_events(self):
        self.monitor.start()
        try:
            (self.path / "artifact").write_bytes(b"123")
            deadline = time.monotonic() + 2
            while self.monitor.snapshot().files != 1 and time.monotonic() < deadline:
                time.sleep(0.01)

            snapshot = self.monitor.snapshot()
            self.assertEqual(snapshot.files, 1)
            self.assertEqual(snapshot.bytes, 3)
        finally:
            self.monitor.stop()


if __name__ == "__main__":
    unittest.main()
