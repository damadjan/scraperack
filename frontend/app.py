import os
from time import monotonic

from cache_monitor import CacheMonitor
from nicegui import run, ui
from ray_nodes import RayNodeClient, node_columns
from ray_tasks import (
    TASK_FIELDS,
    CacheStatusClient,
    RayTaskClient,
    add_cache_status,
    task_columns,
    task_transaction,
    time_ago,
)

CACHES = {
    "Working directories": CacheMonitor(
        os.getenv("SCRAPERACK_WORKING_DIR_CACHE", "/cache/working-dirs")
    ),
    "Pip downloads": CacheMonitor(os.getenv("PIP_CACHE_DIR", "/cache/pip")),
}
TASKS = RayTaskClient(os.getenv("RAY_DASHBOARD_URL", "http://control-plane:8265"))
CACHE_STATUS = CacheStatusClient(
    os.getenv("SCRAPERACK_GATEWAY_URL", "http://127.0.0.1:42800")
)
NODES = RayNodeClient(os.getenv("RAY_DASHBOARD_URL", "http://control-plane:8265"))
node_names = {}
node_names_updated_at = 0.0
task_rows = {}


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
    ui.card().classes("w-[90vw] max-w-5xl h-[80vh] overflow-hidden"),
):
    invocation_content = ui.column().classes(
        "w-full h-full min-h-0 gap-4 overflow-y-auto overflow-x-hidden"
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


def show_invocation(event):
    task = event.args.get("data") or {}
    if not task:
        return
    fields = [field for field in TASK_FIELDS if field in task]
    fields.extend(sorted(set(task) - set(fields)))
    invocation_content.clear()
    with invocation_content:
        with ui.row().classes("w-full items-center flex-nowrap"):
            ui.label(
                task.get("name") or task.get("func_or_class_name") or "Invocation"
            ).classes("text-xl font-medium grow min-w-0 truncate")
            if task.get("state"):
                ui.badge(task["state"]).props("outline")
            ui.button(icon="close", on_click=invocation_dialog.close).props(
                "flat round dense"
            )
        for field in fields:
            value = task[field]
            if value in (None, ""):
                continue
            with ui.row().classes(
                "w-full items-start gap-4 border-b border-neutral-800 py-2 flex-nowrap"
            ):
                ui.label(field.replace("_", " ").title()).classes(
                    "w-48 shrink-0 text-sm text-gray-400"
                )
                ui.label(str(value)).classes(
                    "grow min-w-0 whitespace-pre-wrap break-all text-sm"
                )
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
    global task_refresh_running, task_rows
    if task_refresh_running:
        return
    task_refresh_running = True
    await refresh_node_names()
    try:
        snapshot = await run.io_bound(TASKS.fetch)
    except Exception as error:  # noqa: BLE001 - keep the dashboard alive if Ray is down
        task_error.text = str(error)
        task_error.set_visibility(True)
        return
    finally:
        task_refresh_running = False

    cache_warning = ""
    try:
        statuses = await run.io_bound(CACHE_STATUS.fetch)
    except Exception as error:  # noqa: BLE001 - Ray data remains useful without it
        statuses = {}
        cache_warning = f"Cache status unavailable: {error}"

    tasks = add_cache_status(
        [
            task
            | {
                "node": node_label(task.get("node_id")),
                "started": time_ago(task.get("start_time_ms")),
            }
            for task in snapshot.tasks
        ],
        statuses,
    )
    task_rows, transaction = task_transaction(task_rows, tasks)
    with task_grid.props.suspend_updates():
        task_grid.options["rowData"] = list(task_rows.values())
    if any(transaction.values()):
        task_grid.run_grid_method("applyTransaction", transaction)
    shown = len(snapshot.tasks)
    task_error.text = (
        snapshot.warning
        or cache_warning
        or (
            f"Ray returned {shown:,} of {snapshot.total:,} tasks"
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
