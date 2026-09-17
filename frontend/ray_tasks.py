import json
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

TASK_FIELDS = (
    "task_id",
    "name",
    "state",
    "type",
    "func_or_class_name",
    "job_id",
    "node_id",
    "worker_pid",
    "attempt_number",
    "creation_time_ms",
    "start_time_ms",
    "end_time_ms",
    "error_type",
    "error_message",
    "required_resources",
    "runtime_env_info",
    "actor_id",
    "worker_id",
    "parent_task_id",
    "placement_group_id",
    "language",
    "is_debugger_paused",
    "call_site",
    "events",
    "task_log_info",
    "profiling_data",
    "fallback_strategy",
    "label_selector",
)
TIME_FIELDS = {"creation_time_ms", "start_time_ms", "end_time_ms"}


@dataclass(frozen=True)
class TaskSnapshot:
    tasks: list[dict]
    total: int
    warning: str | None


class RayTaskClient:
    def __init__(self, dashboard_url, limit=10_000, timeout=5):
        self.url = dashboard_url.rstrip("/") + "/api/v0/tasks"
        self.limit = limit
        self.timeout = timeout

    def fetch(self):
        query = urlencode({"detail": 1, "limit": self.limit})
        request = Request(f"{self.url}?{query}", headers={"Accept": "application/json"})
        with urlopen(request, timeout=self.timeout) as response:
            return parse_task_response(json.load(response))


def parse_task_response(payload):
    if payload.get("result") is False:
        raise RuntimeError(payload.get("msg") or "Ray rejected the task query")

    data = payload.get("data")
    if not isinstance(data, dict):
        raise TypeError("Ray returned an unexpected task response")

    result = data.get("result")
    if isinstance(result, list):
        tasks = result
        details = {}
    elif isinstance(result, dict) and (
        isinstance(result.get("result"), list) or result.get("result") is None
    ):
        tasks = result["result"] or []
        details = result
    else:
        raise TypeError("Ray returned an unexpected task response")

    warning = details.get("partial_failure_warning")
    warnings = details.get("warnings")
    if not warning and warnings:
        warning = (
            "; ".join(map(str, warnings))
            if isinstance(warnings, list)
            else str(warnings)
        )

    total = details.get("total", len(tasks))
    return TaskSnapshot(
        tasks=[display_task(task) for task in tasks],
        total=total,
        warning=warning,
    )


def display_task(task):
    displayed = {}
    for field, value in task.items():
        if field in TIME_FIELDS and value is not None:
            value = datetime.fromtimestamp(value / 1000, timezone.utc).isoformat(
                timespec="milliseconds"
            )
        elif isinstance(value, (dict, list)):
            value = json.dumps(value, sort_keys=True, separators=(",", ":"))
        displayed[field] = value
    return displayed


def task_columns(tasks):
    fields = set().union(*(task.keys() for task in tasks)) if tasks else set()
    ordered = [field for field in TASK_FIELDS if field in fields or not tasks]
    ordered.extend(sorted(fields - set(ordered)))
    return [
        {
            "field": field,
            "headerName": field.replace("_", " ").title(),
            "minWidth": 130,
        }
        for field in ordered
    ]
