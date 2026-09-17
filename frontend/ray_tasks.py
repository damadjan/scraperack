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
TERMINAL_STATES = {"FINISHED", "FAILED"}
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
        "field": "function_name",
        "headerName": "Name",
        "flex": 1.3,
        "minWidth": 140,
    },
    {
        "field": "project",
        "headerName": "Project",
        "flex": 1.3,
        "minWidth": 140,
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
        "field": "duration",
        "headerName": "Duration",
        "width": 130,
        "maxWidth": 130,
        ":comparator": """(a, b, nodeA, nodeB) =>
            (nodeA.data.duration_ms || 0) - (nodeB.data.duration_ms || 0)""",
    },
    {
        "field": "created",
        "headerName": "Created",
        "flex": 1.5,
        "minWidth": 180,
        "sort": "desc",
        "sortIndex": 0,
        ":comparator": """(a, b, nodeA, nodeB) =>
            Date.parse(nodeA.data.creation_time_ms) -
            Date.parse(nodeB.data.creation_time_ms)""",
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

    def fetch(self, filters=(), limit=None):
        query = [("detail", 1), ("limit", limit or self.limit)]
        for key, predicate, value in filters:
            query.extend(
                (
                    ("filter_keys", key),
                    ("filter_predicates", predicate),
                    ("filter_values", value),
                )
            )
        request = Request(
            f"{self.url}?{urlencode(query)}", headers={"Accept": "application/json"}
        )
        with urlopen(request, timeout=self.timeout) as response:
            return parse_task_response(json.load(response))

    def fetch_active(self):
        return self.fetch((("state", "!=", "FINISHED"), ("state", "!=", "FAILED")))

    def fetch_task(self, task_id):
        tasks = self.fetch((("task_id", "=", task_id),), limit=1).tasks
        return tasks[0] if tasks else None


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


class RayLogClient:
    def __init__(self, dashboard_url, timeout=5):
        self.url = dashboard_url.rstrip("/") + "/api/v0/logs/file"
        self.timeout = timeout

    def fetch(self, task_id, attempt_number=0, suffix="out", lines=1000):
        query = urlencode(
            {
                "task_id": task_id,
                "attempt_number": attempt_number,
                "suffix": suffix,
                "lines": lines,
            }
        )
        with urlopen(f"{self.url}?{query}", timeout=self.timeout) as response:
            return response.read().decode(errors="replace")


def add_cache_status(tasks, statuses):
    displayed = []
    for task in tasks:
        combined = task | statuses.get(
            task.get("task_id"),
            {"function_cache": "unknown", "working_dir_cache": "unknown"},
        )
        function_name = combined.get("function_name")
        project = combined.get("project")
        ray_name = task.get("name") or task.get("func_or_class_name")
        if not function_name and isinstance(ray_name, str):
            if "/" in ray_name:
                fallback_project, function_name = ray_name.split("/", 1)
                project = project or fallback_project
            else:
                function_name = ray_name
        displayed.append(
            combined
            | {
                "function_name": function_name or "—",
                "project": project or "—",
            }
        )
    return displayed


def task_transaction(previous, tasks):
    current = {task["task_id"]: task for task in tasks}
    return current, {
        "add": [task for task_id, task in current.items() if task_id not in previous],
        "update": [
            task
            for task_id, task in current.items()
            if task_id in previous and task != previous[task_id]
        ],
        "remove": [
            task for task_id, task in previous.items() if task_id not in current
        ],
    }


def limit_task_history(tasks, limit):
    active = [task for task in tasks if task.get("state") not in TERMINAL_STATES]
    terminal = sorted(
        (task for task in tasks if task.get("state") in TERMINAL_STATES),
        key=lambda task: task.get("creation_time_ms") or "",
        reverse=True,
    )
    return active + terminal[: max(0, limit - len(active))]


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


def task_duration(start, end=None, now=None):
    if not start:
        return None, "—"
    try:
        started = datetime.fromisoformat(start.replace("Z", "+00:00"))
        finished = (
            datetime.fromisoformat(end.replace("Z", "+00:00"))
            if end
            else now or datetime.now(timezone.utc)
        )
        milliseconds = max(0, round((finished - started).total_seconds() * 1000))
    except (AttributeError, ValueError):
        return None, "—"

    seconds = milliseconds / 1000
    if seconds < 1:
        return milliseconds, f"{milliseconds} ms"
    if seconds < 60:
        return milliseconds, f"{seconds:.1f} s"
    if seconds < 3600:
        minutes, seconds = divmod(round(seconds), 60)
        return milliseconds, f"{minutes}m {seconds}s"
    hours, remainder = divmod(round(seconds), 3600)
    minutes = remainder // 60
    return milliseconds, f"{hours}h {minutes}m"


def task_columns():
    return [column.copy() for column in TABLE_COLUMNS]
