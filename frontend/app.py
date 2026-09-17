import os
from datetime import datetime

from nicegui import run, ui

from cache_monitor import CacheMonitor
from ray_tasks import RayTaskClient, task_columns

CACHES = {
    "Working directories": CacheMonitor(
        os.getenv("SCRAPERACK_WORKING_DIR_CACHE", "/cache/working-dirs")
    ),
    "Pip downloads": CacheMonitor(os.getenv("PIP_CACHE_DIR", "/cache/pip")),
}
TASKS = RayTaskClient(os.getenv("RAY_DASHBOARD_URL", "http://control-plane:8265"))


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
        overview_tab = ui.tab("Overview").props("no-caps")
        tasks_tab = ui.tab("Tasks").props("no-caps")
    ui.space()
    with ui.row().classes("items-center gap-2"):
        ui.icon("circle", size="10px").classes("text-green-500")
        ui.label("Live").classes("text-xs text-gray-400")

labels = {}
versions = {name: -1 for name in CACHES}

with ui.tab_panels(tabs, value=overview_tab).classes("w-full bg-neutral-950"):
    with (
        ui.tab_panel(overview_tab).classes("p-6"),
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

    with (
        ui.tab_panel(tasks_tab).classes("p-6"),
        ui.column().classes("w-full gap-4"),
    ):
        with ui.row().classes("w-full items-end"):
            with ui.column().classes("gap-1"):
                ui.label("Tasks").classes("text-2xl font-medium")
                ui.label("Ray task state, refreshed every second").classes(
                    "text-sm text-gray-400"
                )
            ui.space()
            task_status = ui.label("Connecting to Ray").classes("text-xs text-gray-400")
        task_error = ui.label().classes("text-sm text-red-400")
        task_error.set_visibility(False)
        task_grid = (
            ui.aggrid(
                {
                    "columnDefs": task_columns([]),
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
            .classes("w-full")
            .style("height: calc(100vh - 190px)")
        )


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


async def refresh_tasks():
    if tabs.value != tasks_tab:
        return
    try:
        snapshot = await run.io_bound(TASKS.fetch)
    except Exception as error:  # noqa: BLE001 - keep the dashboard alive if Ray is down
        task_status.text = "Ray unavailable"
        task_error.text = str(error)
        task_error.set_visibility(True)
        return

    task_grid.options["columnDefs"] = task_columns(snapshot.tasks)
    task_grid.options["rowData"] = snapshot.tasks
    task_grid.update()
    shown = len(snapshot.tasks)
    task_status.text = (
        f"{shown:,} task{'s' if shown != 1 else ''} · "
        f"updated {datetime.now().astimezone().strftime('%H:%M:%S')}"
    )
    task_error.text = snapshot.warning or (
        f"Ray returned {shown:,} of {snapshot.total:,} tasks"
        if shown < snapshot.total
        else ""
    )
    task_error.set_visibility(bool(task_error.text))


ui.timer(1.0, refresh_tasks)

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
