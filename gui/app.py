import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo, available_timezones

import httpx
from ago import human
from nicegui import app, ui

CONTROLLER = os.getenv("SCRAPERACK_URL", "http://127.0.0.1:7070").rstrip("/")
client = httpx.AsyncClient(base_url=CONTROLLER, timeout=10)
app.on_shutdown(client.aclose)
ui.add_css(Path(__file__).with_name("style.css").read_text(), shared=True)
ui.add_head_html('''<script>
function capacityRenderer(params) {
  const value = params.value || {label: '—', percent: 0}
  const percent = Math.max(0, Math.min(100, value.percent))
  return `<div class="cell-progress"><span>${value.label}</span><div><i style="width:${percent}%"></i></div></div>`
}
function statusRenderer(params) {
  const status = params.value || 'unknown'
  return `<span class="status-pill status-${status}">${status.replaceAll('_', ' ')}</span>`
}
function portsRenderer(params) {
  const ports = params.value || []
  if (!ports.length) return '<span class="muted">—</span>'
  const escape = value => String(value).replace(/[&<>"']/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]))
  const address = params.data.port_host
  const urlHost = address.includes(':') ? `[${address}]` : address
  return `<div class="port-list">${ports.map(port => {
    const endpoint = `${escape(address)}:${port.host_port}`
    const published = port.protocol === 'tcp' ? `<a href="http://${urlHost}:${port.host_port}" target="_blank">${endpoint}</a>` : `<code>${endpoint}</code>`
    return `<div class="port-map"><span>${escape(port.name)}</span>${published}<b>→</b><code>${port.guest_port}/${escape(port.protocol)}</code></div>`
  }).join('')}</div>`
}
</script>''', shared=True)


async def request(method, path, **kwargs):
    response = await client.request(method, path, **kwargs)
    if response.is_error:
        try:
            message = response.json()["error"]
        except Exception:
            message = response.text or response.reason_phrase
        raise RuntimeError(message)
    return None if response.status_code == 204 else response.json()


def notify_error(error):
    ui.notify(str(error), type="negative", position="top-right")


def stamp(value):
    zone = app.storage.general.get("timezone", "UTC")
    return datetime.fromtimestamp(value / 1000, ZoneInfo(zone)).strftime("%Y-%m-%d %H:%M:%S %Z") if value else "—"


def ago(value):
    delta = datetime.now(timezone.utc) - datetime.fromtimestamp(value / 1000, timezone.utc)
    return human(delta, precision=1)


def size(value):
    for unit in ("B", "KiB", "MiB", "GiB"):
        if value < 1024 or unit == "GiB":
            return f"{value:.1f} {unit}"
        value /= 1024


def parse_ports(value):
    result = []
    for item in filter(None, (item.strip() for item in value.split(","))):
        mapping, _, protocol = item.partition("/")
        protocol = protocol or "tcp"
        if protocol not in ("tcp", "udp"):
            raise ValueError(f"invalid port protocol: {protocol}")
        name, separator, mapping = mapping.partition("=")
        if not separator:
            mapping = name
            name = ""
        values = mapping.split(":")
        if len(values) == 1:
            host_port, guest_port = None, int(values[0])
        elif len(values) == 2:
            host_port, guest_port = map(int, values)
        else:
            raise ValueError(f"invalid port mapping: {item}")
        if not 0 < guest_port < 65536 or host_port is not None and not 0 < host_port < 65536:
            raise ValueError(f"invalid port number: {item}")
        result.append({"name": name or f"{guest_port}/{protocol}", "guest_port": guest_port,
                       "host_port": host_port, "protocol": protocol})
    return result


def shell(active):
    ui.colors(primary="#14b8a6")
    ui.dark_mode().enable()
    drawer = ui.left_drawer(value=True).props("width=220 breakpoint=800").classes("compact-drawer")
    with drawer:
        with ui.row().classes("items-center gap-2 px-2 py-2"):
            ui.icon("developer_board", size="28px").classes("text-cyan-300")
            with ui.column().classes("gap-0"):
                ui.label("SCRAPERACK").classes("text-lg font-semibold text-white tracking-wide")
                ui.label("MicroVM cluster").classes("text-xs muted")
        ui.separator().classes("mb-1")
        groups = [
            (None, [("dashboard", "/", "home", "Home")]),
            ("Execution", [
                ("environments", "/environments", "science", "Environments"),
                ("executions", "/executions", "terminal", "Executions"),
                ("runtime-images", "/runtime-images", "extension", "Runtime Images"),
            ]),
            ("Infrastructure", [
                ("vms", "/vms", "memory", "MicroVMs"),
                ("pools", "/pools", "hub", "VM Pools"),
                ("vm-images", "/vm-images", "layers", "VM Images"),
                ("nodes", "/nodes", "dns", "Nodes"),
            ]),
            (None, [("settings", "/settings", "settings", "Settings")]),
        ]
        for group, links in groups:
            if group:
                ui.label(group).classes("nav-group")
            for key, path, icon, label in links:
                ui.button(label, icon=icon, on_click=lambda path=path: ui.navigate.to(path)) \
                    .props("flat no-caps").classes(f"nav-link {'nav-active' if key == active else ''}")
    content = ui.column().classes("content gap-2")
    with content:
        ui.button(icon="menu", on_click=drawer.toggle).props("flat round").classes("mobile-menu")
    return content


def summary(icon, label, value, detail):
    with ui.card().classes("summary-card"):
        with ui.row().classes("items-start justify-between w-full"):
            with ui.column().classes("gap-2"):
                ui.label(label).classes("text-sm muted")
                value_label = ui.label(value).classes("metric")
                detail_label = ui.label(detail).classes("text-xs muted")
            ui.icon(icon, size="30px").classes("text-teal-600")
    return value_label, detail_label


def table(columns, rows=None, selection=False, row_key=None, full_height=False):
    options = {
        "columnDefs": columns,
        "rowData": rows or [],
        "defaultColDef": {"sortable": True, "filter": True, "resizable": True, "flex": 1, "minWidth": 100},
        "pagination": True,
        "paginationPageSize": 10,
        "paginationPageSizeSelector": [10, 25, 50],
    }
    if full_height:
        options["paginationAutoPageSize"] = True
        options["paginationPageSizeSelector"] = False
    else:
        options["domLayout"] = "autoHeight"
    if selection:
        options["rowSelection"] = {"mode": "singleRow", "checkboxes": False, "enableClickSelection": True}
    if row_key:
        options[":getRowId"] = f"(params) => params.data.{row_key}"
    grid = ui.aggrid(options, auto_size_columns=False).classes(
        f'data-grid {"execution-grid" if full_height else ""}'
    )
    return grid.classes("clickable-grid") if selection else grid


def set_rows(element, rows):
    if element.options["rowData"] != rows:
        element.options["rowData"] = rows
        element.update()


async def execution_details(event):
    run = event.args.get("data")
    if not run:
        return
    try:
        run, environments = await asyncio.gather(
            request("GET", f'/v1/executions/{run["id"]}'),
            request("GET", "/v1/environments"),
        )
    except Exception as error:
        notify_error(error)
        return
    environment_names = {environment["id"]: environment["name"] for environment in environments}
    end = run["finished_at_ms"] or int(time.time() * 1000)
    duration = f'{(end - run["started_at_ms"]) / 1000:.2f}s' if run["started_at_ms"] else "—"
    with ui.dialog() as dialog, ui.card().classes("execution-dialog"):
        with ui.row().classes("items-start w-full gap-2"):
            with ui.column().classes("gap-1 grow"):
                ui.label("Execution details").classes("dialog-eyebrow")
                ui.label(run["id"]).classes("dialog-title mono")
            ui.label(run["status"].replace("_", " ").title()).classes(f'status-pill status-{run["status"]}')
            ui.button(icon="close", on_click=dialog.close).props("flat round")
        with ui.element("div").classes("detail-grid"):
            for label, value, mono in (
                ("Environment", environment_names.get(run.get("environment"), "—"), False),
                ("Node", run["node"] or "Unassigned", True),
                ("Duration", duration, False), ("Created", stamp(run["created_at_ms"]), False),
            ):
                with ui.element("div").classes("detail-card"):
                    ui.label(label).classes("detail-label")
                    ui.label(value).classes(f'detail-value {"mono" if mono else ""}')
        metrics = run.get("metrics")
        if metrics and metrics.get("samples"):
            ui.label("Lifetime utilization").classes("section-title")
            with ui.element("div").classes("metric-charts"):
                for title, field, color in (
                    ("CPU usage", "cpu_percent", "#22d3ee"),
                    ("RAM usage", "memory_percent", "#2dd4bf"),
                ):
                    data = [[point["elapsed_ms"] / 1000, round(point[field], 2)]
                            for point in metrics["samples"]]
                    ui.echart({
                        "backgroundColor": "transparent",
                        "title": {"text": title, "textStyle": {"color": "#ededed", "fontSize": 13}},
                        "tooltip": {"trigger": "axis"},
                        "grid": {"left": 48, "right": 18, "top": 42, "bottom": 38},
                        "xAxis": {"type": "value", "name": "seconds", "axisLine": {"lineStyle": {"color": "#777"}}},
                        "yAxis": {"type": "value", "min": 0, "max": 100,
                                  "axisLabel": {"formatter": "{value}%"}},
                        "series": [{"type": "line", "data": data, "showSymbol": False,
                                    "lineStyle": {"color": color, "width": 2},
                                    "areaStyle": {"color": color, "opacity": .12}}],
                    }).classes("metric-chart")
        ui.label("Lifecycle").classes("section-title")
        with ui.element("div").classes("timeline"):
            for label, value in (
                ("Created", run["created_at_ms"]),
                ("Started", run["started_at_ms"]),
                ("Finished", run["finished_at_ms"]),
            ):
                with ui.element("div").classes("timeline-item"):
                    ui.icon("check_circle" if value else "radio_button_unchecked", size="18px")
                    with ui.column().classes("gap-0"):
                        ui.label(label).classes("detail-label")
                        ui.label(stamp(value)).classes("text-sm")
        with ui.element("div").classes("payload-grid"):
            for label, value in (("Payload", run["payload"]), ("Result", run["result"])):
                with ui.element("div").classes("data-block"):
                    ui.label(label).classes("section-title")
                    ui.code(json.dumps(value, indent=2, ensure_ascii=False) if value is not None else "—").classes("json-code")
        if run["error"]:
            with ui.element("div").classes("error-block"):
                ui.label("Error").classes("section-title")
                ui.label(run["error"]).classes("mono whitespace-pre-wrap")
    dialog.open()


@ui.page("/")
async def dashboard():
    with shell("dashboard"):
        with ui.element("div").classes("summary-grid"):
            node_value, node_detail = summary("dns", "Nodes", "—", "Loading")
            image_value, image_detail = summary("layers", "Images", "—", "Loading")
            pool_value, pool_detail = summary("science", "Environments", "—", "Loading")
            run_value, run_detail = summary("terminal", "Executions", "—", "Loading")
        with ui.card().classes("panel"):
            with ui.row().classes("items-center w-full"):
                ui.label("Cluster capacity").classes("section-title")
                ui.space()
                capacity = ui.label().classes("text-sm muted")
            cpu = ui.linear_progress(value=0, show_value=False).props("rounded color=teal")
            cpu_text = ui.label().classes("text-xs muted")
            memory = ui.linear_progress(value=0, show_value=False).props("rounded color=cyan")
            memory_text = ui.label().classes("text-xs muted")
    async def refresh():
        try:
            nodes, images, environments, executions = await asyncio.gather(
                request("GET", "/v1/nodes"), request("GET", "/v1/images"),
                request("GET", "/v1/environments"), request("GET", "/v1/executions"),
            )
            ready_nodes = sum(node["status"] == "ready" for node in nodes)
            ready_images = sum(image["ready"] for image in images)
            active = sum(run["status"] in ("queued", "assigned", "running") for run in executions)
            for label, value in ((node_value, len(nodes)), (image_value, len(images)),
                                 (pool_value, len(environments)), (run_value, len(executions))):
                label.text = str(value)
            node_detail.text = f"{ready_nodes} ready"
            image_detail.text = f"{ready_images} distributed"
            pool_detail.text = "Execution environments"
            run_detail.text = f"{active} active"
            total_cpu = sum(node["cpu_millis"] for node in nodes)
            used_cpu = sum(node["used_cpu_millis"] for node in nodes)
            total_mem = sum(node["memory_mib"] for node in nodes)
            used_mem = sum(node["used_memory_mib"] for node in nodes)
            cpu.value = used_cpu / total_cpu if total_cpu else 0
            memory.value = used_mem / total_mem if total_mem else 0
            cpu_text.text = f"CPU  {used_cpu / 1000:g} / {total_cpu / 1000:g} cores"
            memory_text.text = f"Memory  {used_mem} / {total_mem} MiB"
            capacity.text = f"{len(nodes)} node{'s' if len(nodes) != 1 else ''}"
        except Exception as error:
            node_detail.text = f"Unavailable: {error}"

    await refresh()
    ui.timer(.5, refresh)


@ui.page("/nodes")
async def nodes_page():
    with shell("nodes"):
        with ui.row().classes("items-center w-full"):
            ui.label("Cluster nodes").classes("section-title")
            ui.space()
            ui.label("Live · 1 second").classes("text-xs muted")
        nodes = table([
            {"headerName": "Name", "field": "name"},
            {"headerName": "Status", "field": "status", "maxWidth": 120, ":cellRenderer": "statusRenderer"},
            {"headerName": "CPU", "field": "cpu", "minWidth": 210, ":cellRenderer": "capacityRenderer"},
            {"headerName": "Memory", "field": "memory", "minWidth": 230, ":cellRenderer": "capacityRenderer"},
            {"headerName": "Images", "field": "image_count", "maxWidth": 100},
            {"headerName": "Env caches", "field": "environment_count", "maxWidth": 120},
            {"headerName": "Idle VMs", "field": "idle_count", "maxWidth": 110},
            {"headerName": "Last heartbeat", "field": "last_seen", "minWidth": 190},
        ])

    async def refresh():
        try:
            data = await request("GET", "/v1/nodes")
            set_rows(nodes, [
                {**node,
                 "cpu": {"label": f'{node["used_cpu_millis"] / 1000:g} / {node["cpu_millis"] / 1000:g} cores',
                         "percent": node["used_cpu_millis"] / node["cpu_millis"] * 100 if node["cpu_millis"] else 0},
                 "memory": {"label": f'{node["used_memory_mib"]} / {node["memory_mib"]} MiB',
                            "percent": node["used_memory_mib"] / node["memory_mib"] * 100 if node["memory_mib"] else 0},
                 "image_count": len(node["images"]),
                 "environment_count": len(node.get("environments", [])),
                 "idle_count": sum(node.get("idle_instances", {}).values()),
                 "last_seen": stamp(node["last_seen_ms"])}
                for node in data
            ])
        except Exception as error:
            notify_error(error)

    await refresh()
    ui.timer(1, refresh)


async def image_catalog(kind, active, title):
    import_status_key = f"{kind}_image_import_status"
    with shell(active):
        with ui.card().classes("panel"):
            ui.label("Import Docker image").classes("section-title")
            ui.label(f"Pull a linux/amd64 image and convert it to a {title.lower()[:-1]}.") \
                .classes("text-sm muted")
            with ui.row().classes("items-end w-full"):
                docker_reference = ui.input("Docker image", placeholder="ghcr.io/acme/browser:latest") \
                    .classes("grow")
                docker_name = ui.input("Image name", placeholder="browser").classes("w-64")
                docker_username = ui.input("Username", placeholder="Optional").classes("w-52")
                docker_password = ui.input("Password", placeholder="Optional").props("type=password") \
                    .classes("w-52")
                import_button = ui.button("Import", icon="download").props("no-caps")
            import_status = ui.label(app.storage.general.get(import_status_key, "")) \
                .classes("text-sm muted")
        with ui.row().classes("items-center w-full"):
            ui.label(title).classes("section-title")
            ui.space()
            delete_button = ui.button("Delete", icon="delete", color="negative").props("outline no-caps")
        images = table([
            {"headerName": "Name", "field": "name"},
            {"headerName": "Compatible", "field": "compatible", "maxWidth": 130},
            {"headerName": "Executor", "field": "display_executor", "minWidth": 170},
            {"headerName": "Distributed", "field": "ready", "maxWidth": 140},
            {"headerName": "Size", "field": "display_size", "maxWidth": 120},
            {"headerName": "SHA-256", "field": "short_hash", "cellClass": "mono"},
            {"headerName": "ID", "field": "id", "cellClass": "mono", "minWidth": 260},
        ], selection=True)

    async def refresh():
        data = await request("GET", "/v1/images")
        data = [image for image in data if image.get("kind") == kind]
        set_rows(images, [{**image, "display_executor": image.get("executor") or "—",
                           "display_size": size(image["size"]), "short_hash": image["sha256"][:16]}
                          for image in data])

    async def do_import():
        reference = docker_reference.value.strip()
        if not reference:
            return ui.notify("Docker image is required", type="warning")
        image_name = docker_name.value.strip() or reference.rsplit("/", 1)[-1].replace(":", "-")
        username = docker_username.value.strip()
        password = docker_password.value
        if bool(username) != bool(password):
            return ui.notify("Username and password must be provided together", type="warning")
        import_button.disable()
        app.storage.general[import_status_key] = f"Importing {reference}…"
        import_status.set_text(app.storage.general[import_status_key])
        try:
            await request("POST", "/v1/images/import",
                          json={"name": image_name, "reference": reference, "kind": kind,
                                "username": username or None, "password": password or None}, timeout=None)
            app.storage.general[import_status_key] = f"Imported {image_name} successfully"
            if not import_status.is_deleted:
                import_status.set_text(app.storage.general[import_status_key])
                docker_reference.value = ""
                docker_name.value = ""
                docker_username.value = ""
                docker_password.value = ""
                await refresh()
        except Exception as error:
            app.storage.general[import_status_key] = f"Import failed: {error}"
            if not import_status.is_deleted:
                import_status.set_text(app.storage.general[import_status_key])
        finally:
            if not import_button.is_deleted:
                import_button.enable()

    async def do_delete():
        selected = await images.get_selected_row()
        if not selected:
            return ui.notify("Select an image first", type="warning")
        try:
            await request("DELETE", f'/v1/images/{selected["id"]}')
            await refresh()
        except Exception as error:
            notify_error(error)

    import_button.on_click(do_import)
    delete_button.on_click(do_delete)
    try:
        await refresh()
    except Exception as error:
        notify_error(error)


@ui.page("/vm-images")
async def vm_images_page():
    await image_catalog("micro_vm", "vm-images", "VM Images")


@ui.page("/runtime-images")
async def runtime_images_page():
    await image_catalog("runtime", "runtime-images", "Runtime Images")


@ui.page("/images")
async def images_redirect():
    ui.navigate.to("/vm-images")


@ui.page("/pools")
async def pools_page():
    with shell("pools"):
        images_data = []
        try:
            images_data = await request("GET", "/v1/images")
        except Exception as error:
            notify_error(error)
        with ui.card().classes("panel"):
            ui.label("Create VM pool").classes("section-title")
            with ui.row().classes("w-full items-end gap-2"):
                name = ui.input("Name").classes("w-48")
                image = ui.select({item["id"]: item["name"] for item in images_data
                                   if item["ready"] and item.get("kind") == "micro_vm"},
                                  label="Image").classes("w-64")
                cpu = ui.number("CPU cores", value=1, min=0.01, step=0.05).classes("w-32")
                memory = ui.number("Memory MiB", value=256, min=64, step=64).classes("w-32")
                disk = ui.number("Disk MiB", value=1024, min=64, step=64).classes("w-32")
                timeout = ui.number("Timeout sec", value=30, min=1).classes("w-32")
                idle_timeout = ui.number("Idle timeout sec", value=30, min=0).classes("w-36")
                warm = ui.number("Warm instances", value=0, min=0).classes("w-36")
                maximum = ui.number("Max instances", value=8, min=1).classes("w-32")
                reuse = ui.switch("Reuse VMs", value=True)
                networking = ui.switch("Networking", value=True)
                ports = ui.input("Ports", placeholder="vnc=5900, cdp=9222").classes("w-72")
                create_button = ui.button("Create", icon="add").props("unelevated no-caps color=teal")
        with ui.row().classes("items-center w-full"):
            ui.label("VM Pools").classes("section-title")
            ui.space()
            delete_button = ui.button("Delete", icon="delete", color="negative").props("outline no-caps")
        pools = table([
            {"headerName": "Name", "field": "name", "minWidth": 140},
            {"headerName": "Image", "field": "image_name", "minWidth": 140},
            {"headerName": "CPU", "field": "cpu", "maxWidth": 90},
            {"headerName": "Memory MiB", "field": "memory_mib", "maxWidth": 125},
            {"headerName": "Disk MiB", "field": "disk_mib", "maxWidth": 110},
            {"headerName": "Timeout", "field": "timeout_secs", "maxWidth": 100},
            {"headerName": "Idle timeout", "field": "idle_timeout_secs", "maxWidth": 120},
            {"headerName": "Warm", "field": "warm_instances", "maxWidth": 90},
            {"headerName": "Reuse", "field": "reuse", "maxWidth": 90},
            {"headerName": "Network", "field": "network", "maxWidth": 100},
            {"headerName": "Ports", "field": "display_ports", "minWidth": 180},
            {"headerName": "Max", "field": "max_instances", "maxWidth": 80},
        ], selection=True)

    async def refresh():
        data = [pool for pool in await request("GET", "/v1/pools") if not pool.get("managed_by")]
        image_names = {item["id"]: item["name"] for item in images_data}
        set_rows(pools, [{**pool, "image_name": image_names.get(pool["image"], pool["image"][:8]),
                          "cpu": pool["cpu_millis"] / 1000,
                          "display_ports": ", ".join(
                              f'{port["name"]}={port["host_port"] or "*"}:{port["guest_port"]}/{port["protocol"]}'
                              for port in pool.get("ports", [])) or "—"} for pool in data])

    async def create():
        if not name.value or not image.value:
            return ui.notify("Name and image are required", type="warning")
        try:
            await request("POST", "/v1/pools", json={
                "name": name.value, "image": image.value, "warm_instances": int(warm.value),
                "max_instances": int(maximum.value), "cpu_millis": round(float(cpu.value) * 1000),
                "memory_mib": int(memory.value), "disk_mib": int(disk.value),
                "timeout_secs": int(timeout.value), "idle_timeout_secs": int(idle_timeout.value),
                "reuse": reuse.value, "network": networking.value,
                "ports": parse_ports(ports.value),
            })
            ui.notify(f"Created pool {name.value}", type="positive")
            name.value = ""
            await refresh()
        except Exception as error:
            notify_error(error)

    async def delete_selected():
        selected = await pools.get_selected_row()
        if not selected:
            return ui.notify("Select a pool first", type="warning")
        try:
            await request("DELETE", f'/v1/pools/{quote(selected["name"], safe="")}')
            await refresh()
        except Exception as error:
            notify_error(error)

    create_button.on_click(create)
    delete_button.on_click(delete_selected)
    try:
        await refresh()
    except Exception as error:
        notify_error(error)


@ui.page("/environments")
async def environments_page():
    with shell("environments"):
        images_data = await request("GET", "/v1/images")
        runtime_images = [image for image in images_data
                          if image.get("kind") == "runtime" and image["ready"]]
        with ui.card().classes("panel"):
            ui.label("Create execution environment").classes("section-title")
            ui.label("Packages are built once, distributed as an immutable artifact, and cached on every node.") \
                .classes("text-sm muted")
            with ui.row().classes("w-full items-end gap-2"):
                name = ui.input("Name").classes("w-48")
                image = ui.select({item["id"]: f'{item["name"]} · {item.get("executor")}'
                                   for item in runtime_images}, label="Runtime image").classes("w-72")
                cpu = ui.number("CPU cores", value=1, min=0.01, step=0.05).classes("w-32")
                memory = ui.number("Memory MiB", value=512, step=64).classes("w-32")
                disk = ui.number("Disk MiB", value=1024, min=64, step=64).classes("w-32")
                warm = ui.number("Warm instances", value=0, min=0).classes("w-36")
                maximum = ui.number("Max instances", value=8, min=1).classes("w-32")
                timeout = ui.number("Timeout sec", value=300, min=1).classes("w-32")
                idle_timeout = ui.number("Idle timeout sec", value=30, min=0).classes("w-36")
                reuse = ui.switch("Reuse VMs", value=True)
                networking = ui.switch("Networking", value=True)
            packages = ui.textarea("Python packages", placeholder="httpx==0.28.1\nbeautifulsoup4==4.14.2") \
                .props("autogrow").classes("w-full")
            preload = ui.textarea("Preload modules", placeholder="httpx\nbs4") \
                .props("autogrow").classes("w-full")
            create_button = ui.button("Create environment", icon="add").props("unelevated no-caps color=teal")
        with ui.row().classes("items-center w-full"):
            ui.label("Execution Environments").classes("section-title")
            ui.space()
            ui.label("Click an environment to edit").classes("text-xs muted")
        environments = table([
            {"headerName": "Name", "field": "name", "minWidth": 150},
            {"headerName": "Runtime", "field": "image_name", "minWidth": 150},
            {"headerName": "Packages", "field": "package_count", "maxWidth": 110},
            {"headerName": "Preloaded", "field": "preload_count", "maxWidth": 110},
            {"headerName": "Cached artifact", "field": "cached", "maxWidth": 150},
            {"headerName": "Distributed", "field": "ready", "maxWidth": 130},
            {"headerName": "CPU", "field": "cpu", "maxWidth": 90},
            {"headerName": "Memory MiB", "field": "memory_mib", "maxWidth": 125},
            {"headerName": "Warm", "field": "warm", "headerTooltip": "Configured / ready", "maxWidth": 90},
            {"headerName": "Max", "field": "max_instances", "maxWidth": 80},
            {"headerName": "Network", "field": "network", "maxWidth": 100},
            {"headerName": "ID", "field": "id", "cellClass": "mono", "minWidth": 260},
        ], selection=True, row_key="id")

    known = {}
    initialized = False

    async def refresh():
        nonlocal known, initialized
        data, vms = await asyncio.gather(
            request("GET", "/v1/environments"), request("GET", "/v1/vms")
        )
        image_names = {item["id"]: item["name"] for item in images_data}
        ready = {}
        for vm in vms:
            if vm["status"] == "idle":
                ready[vm["pool"]] = ready.get(vm["pool"], 0) + 1
        rows = [{
            **environment,
            "image_name": image_names.get(environment["image"], environment["image"][:8]),
            "package_count": len(environment["packages"]),
            "preload_count": len(environment.get("preload", [])),
            "cached": size(environment["size"]) if environment["artifact"] else "Base runtime",
            "cpu": environment["cpu_millis"] / 1000,
            "warm": f'{environment["warm_instances"]}/{ready.get(environment["pool"], 0)}',
        } for environment in data]
        current = {row["id"]: row for row in rows}
        if not initialized:
            set_rows(environments, rows)
            known = current
            initialized = True
            return
        transaction = {
            "add": [row for key, row in current.items() if key not in known],
            "update": [row for key, row in current.items() if key in known and row != known[key]],
            "remove": [row for key, row in known.items() if key not in current],
        }
        with environments.props.suspend_updates():
            environments.options["rowData"] = rows
        if any(transaction.values()):
            environments.run_grid_method("applyTransaction", transaction)
        known = current

    async def create():
        if not name.value or not image.value:
            return ui.notify("Name and runtime image are required", type="warning")
        requested = [item.strip() for item in packages.value.replace(",", "\n").splitlines() if item.strip()]
        preloaded = [item.strip() for item in preload.value.replace(",", "\n").splitlines() if item.strip()]
        create_button.disable()
        try:
            await request("POST", "/v1/environments", json={
                "name": name.value, "image": image.value, "packages": requested, "preload": preloaded,
                "warm_instances": int(warm.value), "max_instances": int(maximum.value),
                "cpu_millis": round(float(cpu.value) * 1000), "memory_mib": int(memory.value),
                "disk_mib": int(disk.value), "timeout_secs": int(timeout.value),
                "idle_timeout_secs": int(idle_timeout.value), "reuse": reuse.value,
                "network": networking.value,
            }, timeout=None)
            ui.notify(f"Created environment {name.value}", type="positive")
            name.value = ""
            packages.value = ""
            preload.value = ""
            await refresh()
        except Exception as error:
            notify_error(error)
        finally:
            create_button.enable()

    async def edit_environment(event):
        current = event.args.get("data")
        if not current:
            return
        try:
            runs = await request("GET", "/v1/executions?limit=1000")
        except Exception as error:
            notify_error(error)
            runs = []
        measured = [run for run in runs if run.get("environment") == current["id"]
                    and (run.get("metrics") or {}).get("samples")]
        with ui.dialog() as dialog, ui.card().classes("execution-dialog"):
            with ui.row().classes("items-center w-full"):
                ui.label("Edit execution environment").classes("section-title")
                ui.space()
                ui.button(icon="close", on_click=dialog.close).props("flat round")
            ui.label(f"Actual utilization · {len(measured)} recent measured executions") \
                .classes("text-sm muted")
            if measured:
                with ui.element("div").classes("detail-grid"):
                    for label, value in (
                        ("Average CPU usage",
                         f'{sum(run["metrics"]["average_cpu_percent"] for run in measured) / len(measured):.1f}%'),
                        ("Average RAM usage",
                         f'{sum(run["metrics"]["average_memory_percent"] for run in measured) / len(measured):.1f}%'),
                    ):
                        with ui.element("div").classes("detail-card"):
                            ui.label(label).classes("detail-label")
                            ui.label(value).classes("detail-value")
            else:
                ui.label("Metrics will appear after new executions complete.").classes("text-sm muted")
            with ui.row().classes("w-full items-end gap-2"):
                edit_name = ui.input("Name", value=current["name"]).classes("w-48")
                edit_image = ui.select(
                    {item["id"]: f'{item["name"]} · {item.get("executor")}'
                     for item in runtime_images},
                    label="Runtime image", value=current["image"],
                ).classes("w-72")
                edit_cpu = ui.number("CPU cores", value=current["cpu_millis"] / 1000,
                                     min=0.01, step=0.05).classes("w-32")
                edit_memory = ui.number("Memory MiB", value=current["memory_mib"],
                                        step=64).classes("w-32")
                edit_disk = ui.number("Disk MiB", value=current["disk_mib"],
                                      min=64, step=64).classes("w-32")
            with ui.row().classes("w-full items-end gap-2"):
                edit_warm = ui.number("Warm instances", value=current["warm_instances"],
                                      min=0).classes("w-36")
                edit_maximum = ui.number("Max instances", value=current["max_instances"],
                                         min=1).classes("w-32")
                edit_timeout = ui.number("Timeout sec", value=current["timeout_secs"],
                                         min=1).classes("w-32")
                edit_idle_timeout = ui.number("Idle timeout sec", value=current["idle_timeout_secs"],
                                              min=0).classes("w-36")
                edit_reuse = ui.switch("Reuse VMs", value=current["reuse"])
                edit_networking = ui.switch("Networking", value=current["network"])
            edit_packages = ui.textarea(
                "Python packages", value="\n".join(current["packages"]),
                placeholder="httpx==0.28.1\nbeautifulsoup4==4.14.2",
            ).props("autogrow").classes("w-full")
            edit_preload = ui.textarea(
                "Preload modules", value="\n".join(current.get("preload", [])),
                placeholder="httpx\nbs4",
            ).props("autogrow").classes("w-full")
            status = ui.label().classes("text-sm muted")
            with ui.row().classes("w-full items-center"):
                delete_button = ui.button("Delete", icon="delete", color="negative") \
                    .props("outline no-caps")
                ui.space()
                restart_button = ui.button("Restart", icon="restart_alt") \
                    .props("outline no-caps color=orange")
                ui.button("Cancel", on_click=dialog.close).props("flat no-caps")
                save_button = ui.button("Save changes", icon="save").props("unelevated no-caps color=teal")

        async def save_environment():
            if not edit_name.value or not edit_image.value:
                return ui.notify("Name and runtime image are required", type="warning")
            requested = [item.strip() for item in edit_packages.value.replace(",", "\n").splitlines()
                         if item.strip()]
            preloaded = [item.strip() for item in edit_preload.value.replace(",", "\n").splitlines()
                         if item.strip()]
            save_button.disable()
            status.set_text("Applying changes…")
            try:
                await request("PUT", f'/v1/environments/{current["id"]}', json={
                    "name": edit_name.value, "image": edit_image.value, "packages": requested,
                    "preload": preloaded,
                    "warm_instances": int(edit_warm.value),
                    "max_instances": int(edit_maximum.value) if edit_maximum.value is not None else None,
                    "cpu_millis": round(float(edit_cpu.value) * 1000),
                    "memory_mib": int(edit_memory.value), "disk_mib": int(edit_disk.value),
                    "timeout_secs": int(edit_timeout.value),
                    "idle_timeout_secs": int(edit_idle_timeout.value), "reuse": edit_reuse.value,
                    "network": edit_networking.value,
                }, timeout=None)
                dialog.close()
                ui.notify(f"Updated environment {edit_name.value}", type="positive")
                await refresh()
            except Exception as error:
                status.set_text(f"Update failed: {error}")
                save_button.enable()

        async def restart_environment():
            restart_button.disable()
            status.set_text("Restarting VM pool…")
            try:
                await request("POST", f'/v1/environments/{current["id"]}/restart')
                dialog.close()
                ui.notify(f'Restarted environment {current["name"]}', type="positive")
                await refresh()
            except Exception as error:
                status.set_text(f"Restart failed: {error}")
                restart_button.enable()

        async def delete_environment():
            delete_button.disable()
            status.set_text("Deleting environment…")
            try:
                await request("DELETE", f'/v1/environments/{current["id"]}')
                dialog.close()
                ui.notify(f'Deleted environment {current["name"]}', type="positive")
                await refresh()
            except Exception as error:
                status.set_text(f"Delete failed: {error}")
                delete_button.enable()

        save_button.on_click(save_environment)
        restart_button.on_click(restart_environment)
        delete_button.on_click(delete_environment)
        dialog.open()

    create_button.on_click(create)
    environments.classes("clickable-grid").on("cellClicked", edit_environment, ["data"])
    await refresh()
    ui.timer(1, refresh)


@ui.page("/executions")
async def executions_page():
    with shell("executions"):
        with ui.row().classes("items-center w-full"):
            ui.label("Execution history").classes("section-title")
            ui.space()
            ui.label("Click an execution for full details").classes("text-xs muted")
        executions = table([
            {"headerName": "ID", "field": "short", "maxWidth": 130, "cellClass": "mono"},
            {"headerName": "Environment", "field": "environment_name"},
            {"headerName": "Status", "field": "status", "maxWidth": 140, ":cellRenderer": "statusRenderer"},
            {"headerName": "Node", "field": "short_node", "cellClass": "mono"},
            {"headerName": "Created", "field": "created", "tooltipField": "created_exact"},
            {"headerName": "Duration", "field": "duration", "maxWidth": 120},
        ], row_key="id", full_height=True)
        executions.classes("clickable-grid").on("cellClicked", execution_details, ["data"])

    known = {}
    initialized = False

    async def refresh():
        nonlocal known, initialized
        data, environments_data = await asyncio.gather(
            request("GET", "/v1/executions?limit=1000"), request("GET", "/v1/environments")
        )
        environment_names = {environment["id"]: environment["name"]
                             for environment in environments_data}
        data.sort(key=lambda item: item["created_at_ms"], reverse=True)
        rows = []
        for run in data:
            end = run["finished_at_ms"] or int(time.time() * 1000)
            duration = f'{(end - run["started_at_ms"]) / 1000:.2f}s' if run["started_at_ms"] else "—"
            rows.append({**run, "short": run["id"][:8], "short_node": (run["node"] or "—")[:8],
                         "environment_name": environment_names.get(run.get("environment"), "—"),
                         "created": ago(run["created_at_ms"]), "created_exact": stamp(run["created_at_ms"]),
                         "duration": duration})
        current = {row["id"]: row for row in rows}
        if not initialized:
            set_rows(executions, rows)
            known = current
            initialized = True
            return
        transaction = {
            "add": [row for row in rows if row["id"] not in known],
            "update": [row for row in rows if row["id"] in known and row != known[row["id"]]],
            "remove": [row for key, row in known.items() if key not in current],
            "addIndex": 0,
        }
        with executions.props.suspend_updates():
            executions.options["rowData"] = rows
        if transaction["add"] or transaction["update"] or transaction["remove"]:
            executions.run_grid_method("applyTransaction", transaction)
        known = current

    try:
        await refresh()
    except Exception as error:
        notify_error(error)
    ui.timer(.5, refresh)


@ui.page("/vms")
async def vms_page():
    with shell("vms"):
        with ui.row().classes("items-center w-full"):
            ui.label("MicroVMs").classes("section-title")
            ui.space()
            ui.label("Live inventory · 1 second").classes("text-xs muted")
        vms = table([
            {"headerName": "ID", "field": "short", "maxWidth": 130, "cellClass": "mono"},
            {"headerName": "Status", "field": "status", "maxWidth": 120, ":cellRenderer": "statusRenderer"},
            {"headerName": "Resource", "field": "resource", "minWidth": 150},
            {"headerName": "Environment", "field": "environment_name", "minWidth": 150},
            {"headerName": "Pool", "field": "pool"},
            {"headerName": "Node", "field": "node_name"},
            {"headerName": "Guest IP", "field": "guest_ip", "cellClass": "mono", "minWidth": 130},
            {"headerName": "Published ports", "field": "published_ports", ":cellRenderer": "portsRenderer",
             "minWidth": 360, "autoHeight": True, "sortable": False, "filter": False},
            {"headerName": "CPU", "field": "cpu", "maxWidth": 110},
            {"headerName": "Memory", "field": "memory", "maxWidth": 130},
            {"headerName": "PID", "field": "pid", "maxWidth": 100},
            {"headerName": "Execution", "field": "short_execution", "cellClass": "mono"},
        ], row_key="id")

    known = {}
    initialized = False

    async def refresh():
        nonlocal known, initialized
        try:
            data, environments_data = await asyncio.gather(
                request("GET", "/v1/vms"), request("GET", "/v1/environments")
            )
            environment_names = {environment["id"]: environment["name"]
                                 for environment in environments_data}
            rows = [{
                "id": vm["id"],
                "short": vm["id"][:8],
                "status": vm["status"],
                "resource": "Execution environment" if vm.get("environment") else "VM pool",
                "environment_name": environment_names.get(vm.get("environment"), "—"),
                "pool": vm["pool"],
                "node_name": vm["node_name"],
                "guest_ip": vm.get("guest_ip") or "—",
                "published_ports": vm.get("ports", []),
                "port_host": vm.get("node_address") or vm["node_name"],
                "cpu": f'{vm["cpu_millis"] / 1000:g} cores',
                "memory": f'{vm["memory_mib"]} MiB',
                "pid": vm["pid"] or "—",
                "short_execution": (vm["execution"] or "—")[:8],
            } for vm in data]
            current = {row["id"]: row for row in rows}
            if not initialized:
                set_rows(vms, rows)
                known = current
                initialized = True
                return
            transaction = {
                "add": [row for key, row in current.items() if key not in known],
                "update": [row for key, row in current.items() if key in known and row != known[key]],
                "remove": [row for key, row in known.items() if key not in current],
            }
            with vms.props.suspend_updates():
                vms.options["rowData"] = rows
            if any(transaction.values()):
                vms.run_grid_method("applyTransaction", transaction)
            known = current
        except Exception as error:
            notify_error(error)

    await refresh()
    ui.timer(1, refresh)


@ui.page("/settings")
async def settings_page():
    with shell("settings"):
        with ui.card().classes("panel"):
            ui.label("Display timezone").classes("section-title")
            ui.label("All absolute dates and times in the GUI use this timezone.").classes("text-sm muted")
            zone = ui.select(sorted(available_timezones()), value=app.storage.general.get("timezone", "UTC"),
                             label="Timezone", with_input=True).classes("w-80")
            clock = ui.label().classes("text-sm mono")

            def update_clock():
                clock.text = datetime.now(ZoneInfo(zone.value)).strftime("%Y-%m-%d %H:%M:%S %Z")

            def save_timezone():
                app.storage.general["timezone"] = zone.value
                update_clock()
                ui.notify(f"Timezone set to {zone.value}", type="positive")

            zone.on_value_change(save_timezone)
            update_clock()
            ui.timer(1, update_clock)
        with ui.card().classes("panel"):
            ui.label("Controller").classes("section-title")
            ui.label(CONTROLLER).classes("mono")
            status = ui.label("Checking…").classes("text-sm muted")
            try:
                health = await request("GET", "/health")
                status.text = "Connected" if health.get("ok") else "Unexpected response"
                status.classes("status-ready")
            except Exception as error:
                status.text = f"Unavailable: {error}"
                status.classes("status-error")
            ui.separator()
            ui.label("The controller URL is set with SCRAPERACK_URL when the GUI service starts.") \
                .classes("text-sm muted")


ui.run(host="0.0.0.0", port=int(os.getenv("SCRAPERACK_GUI_PORT", "8080")),
       title="Scraperack", favicon="⚡", reload=False, show=False)
