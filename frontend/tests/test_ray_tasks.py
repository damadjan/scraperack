import io
import json
import unittest
from unittest.mock import patch

from ray_tasks import (
    CacheStatusClient,
    RayTaskClient,
    add_cache_status,
    parse_task_response,
    task_columns,
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
        self.assertEqual(snapshot.tasks[0]["required_resources"], '{"CPU":1}')

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
        started = "1970-01-01T00:00:00.000+00:00"
        columns = task_columns(
            [
                {
                    "task_id": "one",
                    "future_ray_field": "value",
                    "start_time_ms": started,
                }
            ]
        )

        self.assertEqual(
            [column["field"] for column in columns],
            [
                "name",
                "state",
                "node",
                "function_cache",
                "working_dir_cache",
                "start_time_ms",
            ],
        )
        self.assertEqual(columns[-1]["refData"][started], time_ago(started))

    def test_time_ago_preserves_unexpected_values(self):
        self.assertEqual(time_ago("unknown"), "unknown")


if __name__ == "__main__":
    unittest.main()
