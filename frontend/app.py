import json
import os
from asyncio import gather
from datetime import datetime, timezone
from time import monotonic

from cache_monitor import CacheMonitor
from nicegui import run, ui
from ray_nodes import RayNodeClient, node_columns
from ray_tasks import (
    TERMINAL_STATES,
    CacheStatusClient,
    RayLogClient,
    RayTaskClient,
    add_cache_status,
    limit_task_history,
    task_columns,
    task_duration,
    task_transaction,
    time_ago,
)

CACHES = {
    "Working directories": CacheMonitor(
        os.getenv("SCRAPERACK_WORKING_DIR_CACHE", "/cache/working-dirs")
    ),
    "Pip downloads": CacheMonitor(os.getenv("PIP_CACHE_DIR", "/cache/pip")),
}
RAY_DASHBOARD_URL = os.getenv("RAY_DASHBOARD_URL", "http://control-plane:8265")
TASKS = RayTaskClient(RAY_DASHBOARD_URL)
LOGS = RayLogClient(RAY_DASHBOARD_URL)
CACHE_STATUS = CacheStatusClient(
    os.getenv("SCRAPERACK_GATEWAY_URL", "http://127.0.0.1:42800")
)
NODES = RayNodeClient(RAY_DASHBOARD_URL)
node_names = {}
node_names_updated_at = 0.0
task_rows = {}
last_full_task_refresh = 0.0
FULL_TASK_REFRESH_SECONDS = 60


def size(value):
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    amount = float(value)
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            return f"{amount:.0f} {unit}" if unit == "B" else f"{amount:.1f} {unit}"
        amount /= 1024


def changed(value):
    if value is None:
        return "No changes observed"
    return f"Last change {value.astimezone().strftime('%H:%M:%S')}"


ui.dark_mode().enable()
ui.colors(primary="#22c55e")
ui.add_css("""
.nicegui-content { padding: 0 !important; }
.cache-card { background: #171717; border: 1px solid #303030; box-shadow: none; }
.invocation-state {
    display: inline-flex;
    align-items: center;
    border: 1px solid currentColor;
    border-radius: 9999px;
    padding: 2px 8px;
    font-size: 12px;
    font-weight: 500;
    line-height: 1.25;
}
.cache-status {
    display: inline-flex;
    align-items: center;
    border: 1px solid currentColor;
    border-radius: 9999px;
    padding: 2px 8px;
    font-size: 12px;
    font-weight: 500;
    line-height: 1.25;
}
.node-meter {
    position: relative;
    flex: 1 1 auto;
    min-width: 0;
    width: 100%;
    height: 22px;
    overflow: hidden;
    border: 1px solid #2563a9;
    border-radius: 4px;
    background: #262626;
}
.node-meter-fill { height: 100%; background: #0f4c8a; }
.node-meter-label {
    position: absolute;
    inset: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    color: #f5f5f5;
    font-size: 12px;
}
.node-meter-cell .ag-cell-wrapper,
.node-meter-cell .ag-cell-value { width: 100%; min-width: 0; }
.log-panel {
    min-height: 220px;
    max-height: 420px;
    overflow: auto;
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    border: 1px solid #303030;
    border-radius: 6px;
    background: #0f0f0f;
    padding: 16px;
    color: #e5e5e5;
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    font-size: 13px;
}
.data-panel {
    overflow: auto;
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    border: 1px solid #303030;
    border-radius: 6px;
    background: #0f0f0f;
    padding: 12px;
    color: #d4d4d4;
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    font-size: 12px;
}
.detail-card { background: #171717; border: 1px solid #303030; box-shadow: none; }
.modal-header {
    background: #262626;
    border-bottom: 1px solid #404040;
}
""")

with (
    ui.header()
    .classes("p-0 text-gray-200")
    .style(
        "height: 48px; background: #171717; border-bottom: 1px solid #303030; box-shadow: none"
    ),
    ui.row().classes("h-full w-full items-center gap-5 px-4"),
):
    ui.label("ScrapeRack").classes("text-lg font-medium")
    with ui.tabs().props("dense").classes("h-12") as tabs:
        invocations_tab = ui.tab("Invocations").props("no-caps")
        cluster_tab = ui.tab("Cluster").props("no-caps")
        caches_tab = ui.tab("Caches").props("no-caps")
    ui.space()
    with ui.row().classes("items-center gap-2"):
        ui.icon("circle", size="10px").classes("text-green-500")
        ui.label("Live").classes("text-xs text-gray-400")

labels = {}
versions = {name: -1 for name in CACHES}

with (
    ui.dialog() as invocation_dialog,
    ui.card().classes("w-[90vw] max-w-5xl h-[80vh] overflow-hidden p-0"),
):
    invocation_content = ui.column().classes(
        "w-full h-full min-h-0 gap-3 overflow-hidden"
    )

with (
    ui.tab_panels(tabs, value=invocations_tab)
    .classes("w-full bg-neutral-950")
    .style("height: calc(100vh - 48px)")
):
    with (
        ui.tab_panel(caches_tab).classes("p-6"),
        ui.column().classes("w-full max-w-6xl mx-auto gap-5"),
    ):
        with ui.column().classes("gap-1"):
            ui.label("Caches").classes("text-2xl font-medium")
            ui.label("Live storage used by ScrapeRack's persistent caches").classes(
                "text-sm text-gray-400"
            )
        with ui.row().classes("w-full gap-4 items-stretch flex-wrap"):
            for name in CACHES:
                with ui.card().classes("cache-card grow basis-80 gap-3 p-5"):
                    with ui.row().classes("w-full items-center"):
                        ui.label(name).classes("text-lg font-medium")
                        ui.space()
                        ui.icon("visibility", size="18px").classes(
                            "text-green-500"
                        ).tooltip("Watched through filesystem events")
                    total = ui.label("0 B").classes("text-3xl font-medium")
                    files = ui.label("0 files").classes("text-sm text-gray-300")
                    activity = ui.label("No changes observed").classes(
                        "text-xs text-gray-500"
                    )
                    labels[name] = (total, files, activity)

    with ui.tab_panel(invocations_tab).classes("p-0 h-full relative"):
        task_error = ui.label().classes(
            "absolute z-10 m-4 rounded bg-red-950 px-3 py-2 text-sm text-red-300"
        )
        task_error.set_visibility(False)
        task_grid = (
            ui.aggrid(
                {
                    ":getRowId": "params => params.data.task_id",
                    "columnDefs": task_columns(),
                    "rowData": [],
                    "defaultColDef": {
                        "sortable": True,
                        "filter": True,
                        "floatingFilter": True,
                        "resizable": True,
                    },
                    "pagination": True,
                    "paginationPageSize": 50,
                    "paginationPageSizeSelector": [25, 50, 100],
                    "animateRows": False,
                    "enableCellTextSelection": True,
                }
            )
            .classes("w-full h-full")
            .style("height: 100%")
        )

    with ui.tab_panel(cluster_tab).classes("p-0 h-full relative"):
        with ui.row().classes(
            "w-full h-[52px] items-center gap-3 border-b border-neutral-800 px-4"
        ):
            cluster_status = ui.label("Connecting").classes(
                "invocation-state text-gray-400"
            )
            cluster_nodes = ui.label().classes("text-sm text-gray-400")
            ui.space()
            cluster_error = ui.label().classes("text-sm text-red-400")
        cluster_grid = (
            ui.aggrid(
                {
                    "columnDefs": node_columns(),
                    "rowData": [],
                    "defaultColDef": {
                        "sortable": True,
                        "filter": True,
                        "resizable": True,
                    },
                    "animateRows": False,
                    "enableCellTextSelection": True,
                }
            )
            .classes("w-full")
            .style("height: calc(100% - 52px)")
        )


def decoded(value):
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def displayed(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, indent=2, sort_keys=True, default=str)
    return str(value)


def millisecond_time(value):
    try:
        return datetime.fromtimestamp(float(value) / 1000, timezone.utc).isoformat(
            timespec="milliseconds"
        )
    except (TypeError, ValueError, OSError):
        return str(value)


def elapsed(milliseconds):
    try:
        milliseconds = max(0, float(milliseconds))
    except (TypeError, ValueError):
        return "—"
    if milliseconds < 1:
        return f"{milliseconds:.3f} ms"
    if milliseconds < 1000:
        return f"{milliseconds:.2f} ms"
    return f"{milliseconds / 1000:.2f} s"


def modal_fields(task, fields):
    shown = False
    for field in fields:
        value = task.get(field)
        if value in (None, ""):
            continue
        shown = True
        with ui.row().classes(
            "w-full items-start gap-4 border-b border-neutral-800 py-2 flex-nowrap"
        ):
            ui.label(field.replace("_", " ").title()).classes(
                "w-48 shrink-0 text-sm text-gray-400"
            )
            ui.label(displayed(value)).classes(
                "grow min-w-0 whitespace-pre-wrap break-all text-sm"
            )
    if not shown:
        ui.label("No data available").classes("text-sm text-gray-500")


def detail_card(title, value, caption=None):
    with ui.card().classes("detail-card grow min-w-40 gap-1 p-3"):
        ui.label(title).classes("text-xs uppercase tracking-wide text-gray-500")
        ui.label(str(value)).classes("text-base font-medium break-all")
        if caption:
            ui.label(caption).classes("text-xs text-gray-500 break-all")


def environment_panel(task):
    info = decoded(task.get("runtime_env_info"))
    info = info if isinstance(info, dict) else {}
    config = decoded(info.get("runtime_env_config"))
    config = config if isinstance(config, dict) else {}
    environment = decoded(info.get("serialized_runtime_env"))
    environment = environment if isinstance(environment, dict) else {}
    uris = decoded(info.get("uris"))
    uris = uris if isinstance(uris, dict) else {}
    pip = decoded(environment.get("pip"))
    pip = pip if isinstance(pip, dict) else {}
    packages = pip.get("packages") or []
    working_dir = environment.get("working_dir") or uris.get("working_dir_uri")

    with ui.row().classes("w-full gap-3 mb-2"):
        detail_card("Function cache", task.get("function_cache", "—"))
        detail_card("Working dir cache", task.get("working_dir_cache", "—"))

    ui.label("Python packages").classes("text-xs uppercase tracking-wide text-gray-500")
    with ui.row().classes("w-full gap-2"):
        if packages:
            for package in packages:
                ui.badge(str(package)).props("outline color=blue-grey-4")
        else:
            ui.label("No pip packages requested").classes("text-sm text-gray-500")

    ui.separator().classes("my-2")
    ui.label("Working directory").classes(
        "text-xs uppercase tracking-wide text-gray-500"
    )
    if working_dir:
        archive = str(working_dir).rsplit("/", 1)[-1]
        detail_card("Archive", archive, str(working_dir))
    else:
        ui.label("No working directory").classes("text-sm text-gray-500")

    with ui.row().classes("w-full gap-3 mt-2"):
        detail_card("Eager install", config.get("eager_install", "—"))
        timeout = config.get("setup_timeout_seconds")
        detail_card("Setup timeout", f"{timeout} s" if timeout is not None else "—")
        detail_card("Pip check", pip.get("pip_check", "—"))
    with ui.row().classes("w-full gap-3"):
        detail_card("Ray commit", environment.get("_ray_commit", "—"))
        options = ", ".join(map(str, pip.get("pip_install_options") or []))
        detail_card("Pip options", options or "None")
        detail_card("Python modules", len(uris.get("py_modules_uris") or []))

    remaining = {
        key: value
        for key, value in info.items()
        if key not in {"runtime_env_config", "serialized_runtime_env", "uris"}
    }
    if remaining:
        ui.label("Additional environment data").classes(
            "text-xs uppercase tracking-wide text-gray-500 mt-2"
        )
        ui.label(displayed(remaining)).classes("data-panel w-full")
    with ui.expansion("Complete environment data", icon="data_object").classes(
        "w-full mt-2"
    ):
        ui.label(displayed(info)).classes("data-panel w-full")


def timeline_panel(task):
    events = decoded(task.get("events"))
    if not isinstance(events, list) or not events:
        ui.label("No state timeline available").classes("text-sm text-gray-500")
        return
    for index, event in enumerate(events):
        if not isinstance(event, dict):
            ui.label(displayed(event)).classes("data-panel w-full")
            continue
        created = event.get("created_ms")
        next_created = (
            events[index + 1].get("created_ms")
            if index + 1 < len(events) and isinstance(events[index + 1], dict)
            else None
        )
        with ui.row().classes("w-full items-center gap-3 flex-nowrap py-2"):
            ui.icon("circle", size="11px").classes(
                "text-green-500" if index == len(events) - 1 else "text-blue-400"
            )
            ui.badge(event.get("state") or "UNKNOWN").props("outline")
            ui.label(millisecond_time(created)).classes("grow text-sm text-gray-300")
            ui.label(
                elapsed(next_created - created)
                if next_created is not None and created is not None
                else "—"
            ).classes("w-24 text-right text-sm text-gray-400")


def profiling_panel(task):
    with ui.row().classes("w-full gap-3 mb-3"):
        detail_card(
            "Created",
            task.get("created", "—"),
            task.get("creation_time_ms"),
        )
        detail_card("Started", task.get("start_time_ms", "—"))
        detail_card("Finished", task.get("end_time_ms", "—"))
        detail_card("Duration", task.get("duration", "—"))

    ui.label("State timeline").classes("text-xs uppercase tracking-wide text-gray-500")
    timeline_panel(task)
    ui.separator().classes("my-3")
    ui.label("Worker profiling").classes(
        "text-xs uppercase tracking-wide text-gray-500"
    )
    profile = decoded(task.get("profiling_data"))
    if not isinstance(profile, dict):
        ui.label("No profiling information available").classes("text-sm text-gray-500")
        return
    modal_fields(profile, ("component_type", "component_id", "node_ip_address"))
    events = profile.get("events") or []
    durations = [
        max(0, event.get("end_time", 0) - event.get("start_time", 0))
        for event in events
        if isinstance(event, dict)
    ]
    maximum = max(durations, default=1)
    ui.label("Execution phases").classes(
        "text-xs uppercase tracking-wide text-gray-500 mt-3"
    )
    if not events:
        ui.label("No profiling events available").classes("text-sm text-gray-500")
    for event in events:
        if not isinstance(event, dict):
            continue
        duration = max(0, event.get("end_time", 0) - event.get("start_time", 0))
        with ui.card().classes("detail-card w-full gap-2 p-3"):
            with ui.row().classes("w-full items-center gap-3"):
                ui.label(event.get("event_name") or "Unnamed event").classes(
                    "grow font-medium"
                )
                ui.label(elapsed(duration)).classes("text-sm text-gray-400")
            ui.linear_progress(value=duration / maximum).props("rounded size=6px")
            ui.label(
                f"{millisecond_time(event.get('start_time'))} → "
                f"{millisecond_time(event.get('end_time'))}"
            ).classes("text-xs text-gray-500")
            extra = event.get("extra_data")
            if extra:
                ui.label(displayed(extra)).classes("data-panel w-full")


def show_invocation(event):
    task = event.args.get("data") or {}
    if not task:
        return
    invocation_content.clear()
    with invocation_content:
        with ui.row().classes("modal-header w-full items-center flex-nowrap px-4 py-3"):
            with ui.row().classes("grow min-w-0 items-center gap-3 flex-nowrap"):
                ui.label(
                    task.get("name") or task.get("func_or_class_name") or "Invocation"
                ).classes("text-xl font-medium min-w-0 truncate")
                if task.get("state"):
                    ui.badge(task["state"]).props("outline").classes("shrink-0")
            ui.button(icon="close", on_click=invocation_dialog.close).props(
                "flat round dense"
            )
        with (
            ui.tabs()
            .props("dense")
            .classes("w-full border-b border-neutral-800") as tabs
        ):
            overview_tab = ui.tab("Overview").props("no-caps")
            environment_tab = ui.tab("Environment").props("no-caps")
            profiling_tab = ui.tab("Profiling").props("no-caps")
            logs_tab = ui.tab("Logs").props("no-caps")
            result_tab = ui.tab("Result").props("no-caps")
            exception_tab = ui.tab("Exception").props("no-caps")
        with ui.tab_panels(tabs, value=overview_tab).classes(
            "w-full grow min-h-0 bg-transparent"
        ):
            with ui.tab_panel(overview_tab).classes("p-2 h-full overflow-y-auto"):
                modal_fields(
                    task,
                    (
                        "task_id",
                        "name",
                        "state",
                        "node",
                        "required_resources",
                        "actor_id",
                        "placement_group_id",
                        "is_debugger_paused",
                        "call_site",
                    ),
                )
            with ui.tab_panel(environment_tab).classes("p-2 h-full overflow-y-auto"):
                environment_panel(task)
            with ui.tab_panel(profiling_tab).classes("p-2 h-full overflow-y-auto"):
                profiling_panel(task)
            with ui.tab_panel(logs_tab).classes("p-2 h-full overflow-y-auto"):
                with ui.row().classes("w-full items-center"):
                    log_status = ui.label("Open this tab to load task logs").classes(
                        "text-sm text-gray-400"
                    )
                    ui.space()
                    refresh_logs = ui.button("Refresh", icon="refresh").props(
                        "flat dense no-caps"
                    )
                ui.label("stdout").classes("text-sm font-medium text-gray-300")
                stdout = ui.label().classes("log-panel w-full")
                ui.label("stderr").classes("text-sm font-medium text-gray-300 mt-2")
                stderr = ui.label().classes("log-panel w-full")
                with ui.expansion("Log metadata", icon="description").classes(
                    "w-full mt-2"
                ):
                    modal_fields(task, ("task_log_info",))
            with ui.tab_panel(result_tab).classes("p-6 h-full"):
                ui.icon("info", size="32px").classes("text-blue-400")
                ui.label("Results are not retained").classes("text-lg font-medium")
                ui.label(
                    "Return values are delivered directly to the SDK and are not stored "
                    "by ScrapeRack."
                ).classes("max-w-2xl text-sm text-gray-400")
            with ui.tab_panel(exception_tab).classes("p-2 h-full overflow-y-auto"):
                modal_fields(task, ("error_type", "error_message"))

        logs_loaded = False

        async def load_logs(force=False):
            nonlocal logs_loaded
            if logs_loaded and not force:
                return
            log_status.text = "Loading logs..."
            results = await gather(
                run.io_bound(
                    LOGS.fetch,
                    task["task_id"],
                    task.get("attempt_number") or 0,
                    "out",
                ),
                run.io_bound(
                    LOGS.fetch,
                    task["task_id"],
                    task.get("attempt_number") or 0,
                    "err",
                ),
                return_exceptions=True,
            )
            messages = []
            for label, result in zip((stdout, stderr), results, strict=True):
                if isinstance(result, BaseException):
                    label.text = str(result)
                    label.classes(add="text-red-300")
                    messages.append("Some logs could not be loaded")
                else:
                    label.text = result.rstrip() or "No output"
                    label.classes(remove="text-red-300")
            logs_loaded = True
            log_status.text = messages[0] if messages else "Logs loaded"

        async def load_selected_tab(event):
            if event.value in (logs_tab, logs_tab._props["name"]):
                await load_logs()

        async def reload_logs():
            await load_logs(force=True)

        tabs.on_value_change(load_selected_tab)
        refresh_logs.on_click(reload_logs)
    invocation_dialog.open()


task_grid.on("cellClicked", show_invocation, ["data"])


def refresh():
    for name, monitor in CACHES.items():
        snapshot = monitor.snapshot()
        if snapshot.version == versions[name]:
            continue
        versions[name] = snapshot.version
        total, files, activity = labels[name]
        total.text = size(snapshot.bytes)
        files.text = f"{snapshot.files:,} file{'s' if snapshot.files != 1 else ''}"
        activity.text = changed(snapshot.updated_at)


ui.timer(0.5, refresh)

task_refresh_running = False
cluster_refresh_running = False


async def refresh_tasks():
    global task_refresh_running
    if task_refresh_running:
        return
    task_refresh_running = True
    try:
        await update_tasks()
    finally:
        task_refresh_running = False


async def update_tasks():
    global last_full_task_refresh, task_rows
    await refresh_node_names()
    full_refresh = (
        not last_full_task_refresh
        or monotonic() - last_full_task_refresh >= FULL_TASK_REFRESH_SECONDS
    )
    try:
        snapshot = await run.io_bound(
            TASKS.fetch if full_refresh else TASKS.fetch_active
        )
    except Exception as error:  # noqa: BLE001 - keep the dashboard alive if Ray is down
        task_error.text = str(error)
        task_error.set_visibility(True)
        return
    if full_refresh:
        last_full_task_refresh = monotonic()

    incoming = list(snapshot.tasks)
    final_warning = ""
    if (
        not full_refresh
        and not snapshot.warning
        and len(snapshot.tasks) >= snapshot.total
    ):
        previous_active = {
            task_id
            for task_id, task in task_rows.items()
            if task.get("state") not in TERMINAL_STATES
        }
        active_ids = {task["task_id"] for task in incoming}
        ended = previous_active - active_ids
        if ended:
            results = await gather(
                *(run.io_bound(TASKS.fetch_task, task_id) for task_id in ended),
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, BaseException):
                    final_warning = f"Final task state unavailable: {result}"
                elif result:
                    incoming.append(result)

    cache_warning = ""
    try:
        statuses = await run.io_bound(CACHE_STATUS.fetch)
    except Exception as error:  # noqa: BLE001 - Ray data remains useful without it
        statuses = {}
        cache_warning = f"Cache status unavailable: {error}"

    displayed = []
    for task in incoming:
        duration_ms, duration = task_duration(
            task.get("start_time_ms"), task.get("end_time_ms")
        )
        displayed.append(
            task
            | {
                "node": node_label(task.get("node_id")),
                "duration_ms": duration_ms,
                "duration": duration,
                "created": time_ago(task.get("creation_time_ms")),
            }
        )
    incoming = add_cache_status(displayed, statuses)
    merged = task_rows | {task["task_id"]: task for task in incoming}
    tasks = limit_task_history(merged.values(), TASKS.limit)
    task_rows, transaction = task_transaction(task_rows, tasks)
    with task_grid.props.suspend_updates():
        task_grid.options["rowData"] = list(task_rows.values())
    if any(transaction.values()):
        task_grid.run_grid_method("applyTransaction", transaction)
    shown = len(snapshot.tasks)
    scope = "tasks" if full_refresh else "active tasks"
    task_error.text = (
        snapshot.warning
        or final_warning
        or cache_warning
        or (
            f"Ray returned {shown:,} of {snapshot.total:,} {scope}"
            if shown < snapshot.total
            else ""
        )
    )
    task_error.set_visibility(bool(task_error.text))


def remember_nodes(snapshot):
    global node_names_updated_at
    node_names.clear()
    node_names.update(
        {node["node_id"]: node["node"] for node in snapshot.nodes if node["node_id"]}
    )
    node_names_updated_at = monotonic()


async def refresh_node_names():
    if monotonic() - node_names_updated_at < 30:
        return
    try:
        remember_nodes(await run.io_bound(NODES.fetch))
    except Exception:  # noqa: BLE001, S110 - names are optional display metadata
        pass


def node_label(node_id):
    if not node_id:
        return "Unassigned"
    name = node_names.get(node_id)
    return name or node_id[:12]


async def refresh_cluster():
    global cluster_refresh_running
    if cluster_refresh_running:
        return
    cluster_refresh_running = True
    try:
        snapshot = await run.io_bound(NODES.fetch)
    except Exception as error:  # noqa: BLE001 - keep the dashboard alive if Ray is down
        cluster_status.text = "Unavailable"
        cluster_status.style("color: #ef4444")
        cluster_error.text = str(error)
        return
    finally:
        cluster_refresh_running = False

    remember_nodes(snapshot)
    total = len(snapshot.nodes)
    healthy = bool(total) and snapshot.alive == total
    cluster_status.text = "Healthy" if healthy else "Degraded"
    cluster_status.style(f"color: {'#22c55e' if healthy else '#f59e0b'}")
    cluster_nodes.text = f"{snapshot.alive} / {total} nodes alive"
    cluster_error.text = ""
    if cluster_grid.options["rowData"] != snapshot.nodes:
        cluster_grid.options["rowData"] = snapshot.nodes
        cluster_grid.update()


async def refresh_visible_data():
    if tabs.value in (invocations_tab, invocations_tab._props["name"]):
        await refresh_tasks()
    elif tabs.value in (cluster_tab, cluster_tab._props["name"]):
        await refresh_cluster()


tabs.on_value_change(lambda _: refresh_visible_data())
ui.timer(1.0, refresh_visible_data)

if __name__ in {"__main__", "__mp_main__"}:
    for cache in CACHES.values():
        cache.start()
    try:
        ui.run(
            host="0.0.0.0",
            port=9987,
            title="ScrapeRack",
            favicon="🕸️",
            reload=False,
        )
    finally:
        for cache in CACHES.values():
            cache.stop()
