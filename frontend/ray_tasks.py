import json
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ago import human

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
CACHE_RENDERER = """params => {
    const badge = document.createElement('span');
    const value = (params.value || 'unknown').toUpperCase();
    const colors = {
        HIT: '#22c55e',
        MISS: '#f59e0b',
        DISABLED: '#a3a3a3',
        NONE: '#a3a3a3',
        UNKNOWN: '#a3a3a3',
    };
    badge.className = 'cache-status';
    badge.style.color = colors[value];
    badge.textContent = value === 'NONE' ? '—' : value;
    return badge;
}"""
TABLE_COLUMNS = (
    {
        "field": "name",
        "headerName": "Invocation",
        "flex": 1.5,
        "minWidth": 160,
    },
    {
        "field": "state",
        "headerName": "State",
        "width": 140,
        "maxWidth": 140,
        ":cellRenderer": """params => {
            const badge = document.createElement('span');
            const colors = {
                FINISHED: '#22c55e',
                RUNNING: '#38bdf8',
                FAILED: '#ef4444',
                PENDING: '#f59e0b',
            };
            badge.className = 'invocation-state';
            badge.style.color = colors[params.value] || '#a3a3a3';
            badge.textContent = params.value || 'UNKNOWN';
            return badge;
        }""",
        "cellStyle": {"display": "flex", "alignItems": "center"},
    },
    {
        "field": "node",
        "headerName": "Node",
        "flex": 1.2,
        "minWidth": 220,
        "tooltipField": "node_id",
    },
    {
        "field": "function_cache",
        "headerName": "Function Cache",
        "width": 160,
        "maxWidth": 160,
        ":cellRenderer": CACHE_RENDERER,
        "cellStyle": {"display": "flex", "alignItems": "center"},
    },
    {
        "field": "working_dir_cache",
        "headerName": "Working Dir Cache",
        "width": 180,
        "maxWidth": 180,
        ":cellRenderer": CACHE_RENDERER,
        "cellStyle": {"display": "flex", "alignItems": "center"},
    },
    {
        "field": "start_time_ms",
        "headerName": "Started",
        "flex": 1.5,
        "minWidth": 180,
    },
)


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


class CacheStatusClient:
    def __init__(self, gateway_url, timeout=5):
        self.url = gateway_url.rstrip("/") + "/invocations/cache-status"
        self.timeout = timeout

    def fetch(self):
        with urlopen(self.url, timeout=self.timeout) as response:
            result = json.load(response)
        if not isinstance(result, dict):
            raise TypeError("ScrapeRack returned unexpected cache status data")
        return result


def add_cache_status(tasks, statuses):
    return [
        task
        | statuses.get(
            task.get("task_id"),
            {"function_cache": "unknown", "working_dir_cache": "unknown"},
        )
        for task in tasks
    ]


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


def time_ago(value):
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        return human(datetime.now(timezone.utc) - timestamp, precision=1)
    except (AttributeError, ValueError):
        return value


def task_columns(tasks):
    columns = [column.copy() for column in TABLE_COLUMNS]
    columns[-1]["refData"] = {
        task["start_time_ms"]: time_ago(task["start_time_ms"])
        for task in tasks
        if task.get("start_time_ms")
    }
    return columns
