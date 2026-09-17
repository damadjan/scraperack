import io
import json
import unittest
from datetime import datetime, timezone
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from ray_tasks import (
    CacheStatusClient,
    RayLogClient,
    RayTaskClient,
    add_cache_status,
    limit_task_history,
    parse_task_response,
    task_columns,
    task_duration,
    task_transaction,
    time_ago,
)


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class RayTaskClientTests(unittest.TestCase):
    def test_parses_ray_state_response_and_preserves_fields(self):
        snapshot = parse_task_response(
            {
                "result": True,
                "data": {
                    "result": {
                        "total": 2,
                        "result": [
                            {
                                "task_id": "task-1",
                                "state": "FINISHED",
                                "creation_time_ms": 0,
                                "required_resources": {"CPU": 1},
                            }
                        ],
                        "partial_failure_warning": "one node did not answer",
                    },
                },
            }
        )

        self.assertEqual(snapshot.total, 2)
        self.assertEqual(snapshot.warning, "one node did not answer")
        self.assertEqual(
            snapshot.tasks[0]["creation_time_ms"], "1970-01-01T00:00:00.000+00:00"
        )
        self.assertEqual(snapshot.tasks[0]["required_resources"], {"CPU": 1})

    def test_rejects_failed_or_unexpected_responses(self):
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            parse_task_response({"result": False, "msg": "unavailable"})
        with self.assertRaisesRegex(TypeError, "unexpected"):
            parse_task_response({"result": True, "data": {}})

    @patch("ray_tasks.urlopen")
    def test_fetches_detailed_tasks_from_internal_dashboard(self, urlopen):
        urlopen.return_value = Response(
            json.dumps(
                {
                    "result": True,
                    "data": {"result": {"total": 0, "result": []}},
                }
            ).encode()
        )

        snapshot = RayTaskClient("http://control-plane:8265/").fetch()

        request = urlopen.call_args.args[0]
        self.assertEqual(snapshot.tasks, [])
        self.assertIn("/api/v0/tasks?", request.full_url)
        self.assertIn("detail=1", request.full_url)
        self.assertIn("limit=10000", request.full_url)

    @patch("ray_tasks.urlopen")
    def test_fetches_only_non_terminal_tasks(self, urlopen):
        urlopen.return_value = Response(
            json.dumps(
                {"result": True, "data": {"result": {"total": 0, "result": []}}}
            ).encode()
        )

        RayTaskClient("http://control-plane:8265/").fetch_active()

        query = parse_qs(urlparse(urlopen.call_args.args[0].full_url).query)
        self.assertEqual(query["filter_keys"], ["state", "state"])
        self.assertEqual(query["filter_predicates"], ["!=", "!="])
        self.assertEqual(query["filter_values"], ["FINISHED", "FAILED"])

    @patch("ray_tasks.urlopen")
    def test_fetches_one_task_by_id(self, urlopen):
        urlopen.return_value = Response(
            json.dumps(
                {
                    "result": True,
                    "data": {
                        "result": {
                            "total": 1,
                            "result": [{"task_id": "task-1", "state": "FINISHED"}],
                        }
                    },
                }
            ).encode()
        )

        task = RayTaskClient("http://control-plane:8265/").fetch_task("task-1")

        query = parse_qs(urlparse(urlopen.call_args.args[0].full_url).query)
        self.assertEqual(task["state"], "FINISHED")
        self.assertEqual(query["filter_keys"], ["task_id"])
        self.assertEqual(query["filter_values"], ["task-1"])
        self.assertEqual(query["limit"], ["1"])

    @patch("ray_tasks.urlopen")
    def test_fetches_cache_statuses_in_one_request(self, urlopen):
        urlopen.return_value = Response(
            json.dumps(
                {
                    "task-1": {
                        "function_cache": "hit",
                        "working_dir_cache": "miss",
                    }
                }
            ).encode()
        )

        statuses = CacheStatusClient("http://gateway:8080/").fetch()

        self.assertEqual(statuses["task-1"]["function_cache"], "hit")
        self.assertEqual(
            urlopen.call_args.args[0],
            "http://gateway:8080/invocations/cache-status",
        )

    @patch("ray_tasks.urlopen")
    def test_fetches_task_log_by_attempt_and_stream(self, urlopen):
        urlopen.return_value = Response(b"hello from task\n")

        log = RayLogClient("http://control-plane:8265/").fetch(
            "task-1", attempt_number=2, suffix="err"
        )

        query = parse_qs(urlparse(urlopen.call_args.args[0]).query)
        self.assertEqual(log, "hello from task\n")
        self.assertEqual(query["task_id"], ["task-1"])
        self.assertEqual(query["attempt_number"], ["2"])
        self.assertEqual(query["suffix"], ["err"])
        self.assertEqual(query["lines"], ["1000"])

    def test_adds_cache_statuses_to_matching_tasks(self):
        tasks = add_cache_status(
            [{"task_id": "task-1"}, {"task_id": "older-task"}],
            {
                "task-1": {
                    "function_cache": "hit",
                    "working_dir_cache": "miss",
                }
            },
        )

        self.assertEqual(tasks[0]["function_cache"], "hit")
        self.assertEqual(tasks[0]["working_dir_cache"], "miss")
        self.assertEqual(tasks[1]["function_cache"], "unknown")

    def test_columns_only_include_compact_invocation_summary(self):
        columns = task_columns()

        self.assertEqual(
            [column["field"] for column in columns],
            [
                "name",
                "state",
                "node",
                "function_cache",
                "working_dir_cache",
                "duration",
                "created",
            ],
        )
        self.assertEqual(columns[-1]["sort"], "desc")

    def test_transaction_only_contains_changed_rows(self):
        unchanged = {"task_id": "one", "state": "FINISHED"}
        changed = {"task_id": "two", "state": "RUNNING"}
        removed = {"task_id": "three", "state": "FINISHED"}
        added = {"task_id": "four", "state": "PENDING"}

        current, transaction = task_transaction(
            {
                "one": unchanged,
                "two": {"task_id": "two", "state": "PENDING"},
                "three": removed,
            },
            [unchanged, changed, added],
        )

        self.assertEqual(set(current), {"one", "two", "four"})
        self.assertEqual(transaction["add"], [added])
        self.assertEqual(transaction["update"], [changed])
        self.assertEqual(transaction["remove"], [removed])

    def test_history_limit_keeps_active_and_newest_terminal_tasks(self):
        tasks = limit_task_history(
            [
                {"task_id": "active", "state": "RUNNING"},
                {
                    "task_id": "old",
                    "state": "FINISHED",
                    "creation_time_ms": "2026-01-01T00:00:00+00:00",
                },
                {
                    "task_id": "new",
                    "state": "FAILED",
                    "creation_time_ms": "2026-02-01T00:00:00+00:00",
                },
                {
                    "task_id": "middle",
                    "state": "FINISHED",
                    "creation_time_ms": "2026-01-15T00:00:00+00:00",
                },
            ],
            3,
        )

        self.assertEqual(
            {task["task_id"] for task in tasks}, {"active", "new", "middle"}
        )

    def test_time_ago_preserves_unexpected_values(self):
        self.assertEqual(time_ago("unknown"), "unknown")

    def test_formats_running_and_finished_task_durations(self):
        now = datetime(2026, 1, 1, 0, 1, 2, tzinfo=timezone.utc)

        running = task_duration("2026-01-01T00:00:00+00:00", now=now)
        finished = task_duration(
            "2026-01-01T00:00:00+00:00", "2026-01-01T00:02:03+00:00"
        )

        self.assertEqual(running, (62_000, "1m 2s"))
        self.assertEqual(finished, (123_000, "2m 3s"))

    def test_task_without_start_time_has_no_duration(self):
        self.assertEqual(task_duration(None), (None, "—"))


if __name__ == "__main__":
    unittest.main()
