import asyncio
import json
import os
import shlex
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo, available_timezones

import httpx
from ago import human
from fastapi import Request
from fastapi.responses import Response
from nicegui import app, ui

CONTROLLER = os.getenv("SCRAPERACK_URL", "http://127.0.0.1:7070").rstrip("/")
PUBLIC_CONTROLLER = os.getenv("SCRAPERACK_PUBLIC_URL", "")
client = httpx.AsyncClient(base_url=CONTROLLER, timeout=10)
app.on_shutdown(client.aclose)
HOP_HEADERS = {"connection", "keep-alive", "transfer-encoding", "content-length", "host",
               "content-encoding"}


@app.api_route("/plugins/{name}/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
               include_in_schema=False)
async def plugin_proxy(name: str, path: str, request: Request):
    """Serve plugin pages and APIs through the controller from the GUI's own origin."""
    headers = {key: value for key, value in request.headers.items() if key.lower() not in HOP_HEADERS}
    response = await client.request(
        request.method, f"/plugins/{quote(name, safe='')}/{path}",
        params=request.query_params, content=await request.body(), headers=headers, timeout=None,
    )
    return Response(response.content, status_code=response.status_code,
                    headers={key: value for key, value in response.headers.items()
                             if key.lower() not in HOP_HEADERS})
ui.add_css(Path(__file__).with_name("style.css").read_text(), shared=True)
ui.add_head_html('''<script>
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


def shell(active, plugins=()):
    """Page layout; ``plugins`` lists installed plugins for their sidebar entries."""
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
        plugin_links = [(f'plugin-{plugin["name"]}', f'/plugin/{quote(plugin["name"], safe="")}',
                         "widgets", plugin["name"].replace("-", " ").title())
                        for plugin in plugins if plugin.get("gui") and plugin.get("enabled")]
        groups = [
            (None, [("dashboard", "/", "home", "Home")]),
            ("Execution", [
                ("executions", "/executions", "terminal", "Executions"),
            ]),
            ("Plugins", [*plugin_links, ("plugins", "/plugins", "extension", "Manage plugins")]),
            ("Virtualization", [
                ("vms", "/vms", "memory", "MicroVMs"),
                ("pools", "/pools", "hub", "VM Pools"),
                ("images", "/images", "layers", "Images"),
            ]),
            ("Infrastructure", [
                ("nodes", "/nodes", "dns", "Nodes"),
                ("settings", "/settings", "settings", "Settings"),
            ]),
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


async def layout(active):
    try:
        plugins = await request("GET", "/v1/plugins")
    except Exception:
        plugins = []
    return shell(active, plugins)


async def pool_names():
    """Map pool names to the names their owners show for them."""
    return {pool["name"]: pool.get("display_name") or pool["name"]
            for pool in await request("GET", "/v1/pools")}


def owner_label(managed_by):
    if not managed_by:
        return "VM pool"
    return managed_by.removeprefix("plugin:").replace("-", " ").title() + " plugin"


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
        f'data-grid {"full-height-grid" if full_height else ""}'
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
        run, names, payload, logs = await asyncio.gather(
            request("GET", f'/v1/executions/{run["id"]}'),
            pool_names(),
            request("GET", f'/v1/executions/{run["id"]}/payload'),
            request("GET", f'/v1/executions/{run["id"]}/logs'),
        )
        outcome = {"value": None}
        if run["status"] in ("succeeded", "failed", "timed_out"):
            outcome = await request("GET", f'/v1/executions/{run["id"]}/result')
    except Exception as error:
        notify_error(error)
        return
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
                ("Pool", names.get(run["pool"], run["pool"]), False),
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
            for label, value in (("Payload", payload), ("Result", outcome["value"]),
                                 ("stdout", logs["stdout"]), ("stderr", logs["stderr"])):
                with ui.element("div").classes("data-block"):
                    ui.label(label).classes("section-title")
                    text = value if isinstance(value, str) else json.dumps(value, indent=2, ensure_ascii=False)
                    ui.code(text if value is not None else "—").classes("json-code")
        if run["error"]:
            with ui.element("div").classes("error-block"):
                ui.label("Error").classes("section-title")
                ui.label(run["error"]).classes("mono whitespace-pre-wrap")
    dialog.open()


@ui.page("/")
async def dashboard():
    with await layout("dashboard"):
        with ui.element("div").classes("summary-grid"):
            node_value, node_detail = summary("dns", "Nodes", "—", "Loading")
            image_value, image_detail = summary("layers", "Images", "—", "Loading")
            pool_value, pool_detail = summary("hub", "Pools", "—", "Loading")
            run_value, run_detail = summary("terminal", "Executions", "—", "Loading")
        with ui.card().classes("panel"):
            with ui.row().classes("items-center w-full"):
                ui.label("Cluster allocation").classes("section-title")
                ui.space()
                capacity = ui.label().classes("text-sm muted")
            cpu_text = ui.label().classes("text-sm")
            memory_text = ui.label().classes("text-sm")
    async def refresh():
        try:
            nodes, images, pools, executions = await asyncio.gather(
                request("GET", "/v1/nodes"), request("GET", "/v1/images"),
                request("GET", "/v1/pools"), request("GET", "/v1/executions"),
            )
            ready_nodes = sum(node["status"] == "ready" for node in nodes)
            ready_images = sum(image["ready"] for image in images)
            active = sum(run["status"] in ("queued", "assigned", "running") for run in executions)
            for label, value in ((node_value, len(nodes)), (image_value, len(images)),
                                 (pool_value, len(pools)), (run_value, len(executions))):
                label.text = str(value)
            node_detail.text = f"{ready_nodes} ready"
            image_detail.text = f"{ready_images} distributed"
            pool_detail.text = f'{sum(bool(pool.get("managed_by")) for pool in pools)} managed by plugins'
            run_detail.text = f"{active} active"
            total_cpu = sum(node["cpu_millis"] for node in nodes)
            used_cpu = sum(node["used_cpu_millis"] for node in nodes)
            total_mem = sum(node["memory_mib"] for node in nodes)
            used_mem = sum(node["used_memory_mib"] for node in nodes)
            cpu_percent = used_cpu / total_cpu * 100 if total_cpu else 0
            memory_percent = used_mem / total_mem * 100 if total_mem else 0
            cpu_text.text = f"CPU  {used_cpu / 1000:g} / {total_cpu / 1000:g} cores · {cpu_percent:.0f}%"
            memory_text.text = f"RAM  {used_mem} / {total_mem} MiB · {memory_percent:.0f}%"
            capacity.text = f"{len(nodes)} node{'s' if len(nodes) != 1 else ''}"
        except Exception as error:
            node_detail.text = f"Unavailable: {error}"

    await refresh()
    ui.timer(.5, refresh)


@ui.page("/nodes")
async def nodes_page():
    with await layout("nodes"):
        with ui.row().classes("items-center w-full"):
            ui.label("Cluster nodes").classes("section-title")
            ui.space()
            ui.label("Live · 1 second").classes("text-xs muted")
            revoke_button = ui.button("Revoke", icon="link_off", color="negative") \
                .props("outline no-caps")
        nodes = table([
            {"headerName": "Name", "field": "name"},
            {"headerName": "Status", "field": "status", "maxWidth": 120, ":cellRenderer": "statusRenderer"},
            {"headerName": "CPU allocation", "field": "cpu", "minWidth": 210},
            {"headerName": "RAM allocation", "field": "memory", "minWidth": 230},
            {"headerName": "Images", "field": "image_count", "maxWidth": 100},
            {"headerName": "Disks", "field": "disk_count", "maxWidth": 100},
            {"headerName": "Idle VMs", "field": "idle_count", "maxWidth": 110},
            {"headerName": "Concurrent VM starts", "field": "vm_start_concurrency", "minWidth": 190},
            {"headerName": "Last heartbeat", "field": "last_seen", "minWidth": 190},
        ], selection=True, row_key="id", full_height=True)

    known = {}
    initialized = False

    async def refresh():
        nonlocal known, initialized
        try:
            data = await request("GET", "/v1/nodes")
            rows = [
                {"id": node["id"], "name": node["name"], "status": node["status"],
                 "cpu_millis": node["cpu_millis"],
                 "cpu": f'{node["used_cpu_millis"] / 1000:g} / {node["cpu_millis"] / 1000:g} cores · '
                        f'{(node["used_cpu_millis"] / node["cpu_millis"] * 100 if node["cpu_millis"] else 0):.0f}%',
                 "memory": f'{node["used_memory_mib"]} / {node["memory_mib"]} MiB · '
                           f'{(node["used_memory_mib"] / node["memory_mib"] * 100 if node["memory_mib"] else 0):.0f}%',
                 "image_count": len(node["images"]),
                 "disk_count": len(node.get("disks", [])),
                 "idle_count": sum(node.get("idle_instances", {}).values()),
                 "vm_start_concurrency": node.get("vm_start_concurrency")
                                         or max(1, node["cpu_millis"] // 2000),
                 "last_seen": stamp(node["last_seen_ms"])}
                for node in data
            ]
            current = {row["id"]: row for row in rows}
            if not initialized:
                set_rows(nodes, rows)
                known = current
                initialized = True
                return
            transaction = {
                "add": [row for key, row in current.items() if key not in known],
                "update": [row for key, row in current.items() if key in known and row != known[key]],
                "remove": [row for key, row in known.items() if key not in current],
            }
            with nodes.props.suspend_updates():
                nodes.options["rowData"] = rows
            if any(transaction.values()):
                nodes.run_grid_method("applyTransaction", transaction)
            known = current
        except Exception as error:
            notify_error(error)

    async def settings(event):
        selected = event.args.get("data")
        if not selected:
            return
        with ui.dialog() as dialog, ui.card().classes("execution-dialog"):
            with ui.row().classes("items-center w-full"):
                ui.label(f'{selected["name"]} settings').classes("section-title")
                ui.space()
                ui.button(icon="close", on_click=dialog.close).props("flat round")
            concurrency = ui.number(
                "Concurrent VM starts", value=selected["vm_start_concurrency"], min=1,
                step=1, precision=0,
            ).classes("w-64")
            ui.label("Limits simultaneous cold and warm microVM starts on this node.") \
                .classes("text-sm muted")

            async def save_settings():
                try:
                    await request("PUT", f'/v1/nodes/{selected["id"]}', json={
                        "vm_start_concurrency": int(concurrency.value),
                    })
                    dialog.close()
                    ui.notify(f'Updated {selected["name"]}', type="positive")
                    await refresh()
                except Exception as error:
                    notify_error(error)

            with ui.row().classes("w-full justify-end"):
                ui.button("Cancel", on_click=dialog.close).props("flat no-caps")
                ui.button("Save", on_click=save_settings).props("no-caps")
        dialog.open()

    async def revoke():
        selected = await nodes.get_selected_row()
        if not selected:
            return ui.notify("Select a node first", type="warning")
        try:
            await request("DELETE", f'/v1/nodes/{selected["id"]}')
            ui.notify(f'Revoked {selected["name"]}', type="positive")
            await refresh()
        except Exception as error:
            notify_error(error)

    revoke_button.on_click(revoke)
    nodes.classes("clickable-grid").on("cellClicked", settings, ["data"])
    await refresh()
    ui.timer(1, refresh)


@ui.page("/images")
async def images_page():
    import_status_key = "image_import_status"
    with await layout("images"):
        with ui.card().classes("panel"):
            ui.label("Import Docker image").classes("section-title")
            ui.label("Pull a linux/amd64 image and convert it to a microVM root filesystem. "
                     "The image needs /usr/bin/python3 for the guest agent.") \
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
            ui.label("Images").classes("section-title")
            ui.space()
            delete_button = ui.button("Delete", icon="delete", color="negative").props("outline no-caps")
        images = table([
            {"headerName": "Name", "field": "name"},
            {"headerName": "Compatible", "field": "compatible", "maxWidth": 130},
            {"headerName": "Distributed", "field": "ready", "maxWidth": 140},
            {"headerName": "Size", "field": "display_size", "maxWidth": 120},
            {"headerName": "SHA-256", "field": "short_hash", "cellClass": "mono"},
            {"headerName": "ID", "field": "id", "cellClass": "mono", "minWidth": 260},
        ], selection=True)

    async def refresh():
        data = await request("GET", "/v1/images")
        set_rows(images, [{**image, "display_size": size(image["size"]),
                           "short_hash": image["sha256"][:16]} for image in data])

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
                          json={"name": image_name, "reference": reference,
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
async def vm_images_redirect():
    ui.navigate.to("/images")


@ui.page("/runtime-images")
async def runtime_images_redirect():
    ui.navigate.to("/images")


@ui.page("/pools")
async def pools_page():
    with await layout("pools"):
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
                                   if item["ready"]}, label="Image").classes("w-64")
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


async def read_upload(event):
    """Return (name, bytes) from a NiceGUI upload event across NiceGUI versions."""
    file = getattr(event, "file", None)
    if file is not None:
        return file.name, await file.read()
    return event.name, event.content.read()


def install_dialog(on_installed):
    state = {"upload": None}
    with ui.dialog().props("persistent") as dialog, ui.card().classes("execution-dialog"):
        with ui.row().classes("items-center w-full"):
            ui.label("Install plugin").classes("section-title")
            ui.space()
            close_button = ui.button(icon="close", on_click=dialog.close).props("flat round")
        step = ui.label("1. Choose the plugin's .tar.gz archive").classes("text-sm muted")
        review = ui.column().classes("w-full gap-1")
        log = ui.code("").classes("json-code w-full")
        log.set_visibility(False)
        with ui.row().classes("w-full items-center"):
            status = ui.label().classes("text-sm muted")
            ui.space()
            install_button = ui.button("Install", icon="download_done").props("unelevated no-caps color=teal")
            install_button.set_visibility(False)

        async def uploaded(event):
            name, data = await read_upload(event)
            status.set_text(f"Checking {name}…")
            review.clear()
            try:
                staged = await request("POST", "/v1/plugins/uploads", content=data,
                                       headers={"Content-Type": "application/gzip"}, timeout=None)
            except Exception as error:
                status.set_text(f"This archive cannot be installed: {error}")
                return
            state["upload"] = staged["upload"]
            manifest = staged["manifest"]
            installed = staged.get("installed_version")
            step.set_text("2. Review the plugin, then install it")
            with review:
                with ui.element("div").classes("detail-grid"):
                    for label, value in (
                        ("Plugin", manifest["name"]),
                        ("Version", f'{installed} → {manifest["version"]}' if installed
                         else manifest["version"]),
                        ("Adds GUI page", "Yes" if manifest.get("gui") else "No"),
                        ("Files for microVMs", manifest.get("guest") or "None"),
                    ):
                        with ui.element("div").classes("detail-card"):
                            ui.label(label).classes("detail-label")
                            ui.label(value).classes("detail-value")
                if manifest.get("description"):
                    ui.label(manifest["description"]).classes("text-sm")
                ui.label(f'Runs: {" ".join(manifest["run"])}').classes("text-xs mono muted")
                if manifest.get("install"):
                    ui.label(f'Install step: {" ".join(manifest["install"])}').classes("text-xs mono muted")
                ui.label("Plugins run with the controller's permissions. Only install plugins you trust.") \
                    .classes("text-xs muted")
            if not staged.get("install_enabled", True):
                status.set_text("Plugin installation is disabled on this controller.")
                return
            status.set_text("Ready to install" if not installed else
                            ("Reinstall the same version" if installed == manifest["version"]
                             else f"Update from {installed}"))
            install_button.set_visibility(True)

        async def install():
            install_button.disable()
            close_button.disable()
            upload.disable()
            status.set_text("Installing… this can take a few minutes if dependencies are downloaded")
            try:
                result = await request("POST", f'/v1/plugins/uploads/{state["upload"]}/install',
                                       timeout=None)
                status.set_text(f'Installed {result["plugin"]["name"]} {result["plugin"]["version"]}')
                log.set_content(result.get("log") or "")
                install_button.set_visibility(False)
                await on_installed()
            except Exception as error:
                status.set_text("Install failed")
                log.set_content(str(error))
            finally:
                log.set_visibility(True)
                close_button.enable()
                upload.enable()
                install_button.enable()

        upload = ui.upload(on_upload=uploaded, auto_upload=True, max_files=1,
                           label="Drop a plugin .tar.gz here or click to browse") \
            .props('accept=".tar.gz,.tgz,application/gzip" flat bordered').classes("w-full")
        upload.move(target_index=2)
        install_button.on_click(install)
    return dialog


@ui.page("/plugins")
async def plugins_page():
    with await layout("plugins"):
        with ui.row().classes("items-center w-full"):
            ui.label("Plugins").classes("section-title")
            ui.space()
            install_button = ui.button("Install plugin", icon="add").props("unelevated no-caps color=teal")
        ui.label("Plugins add workload types to this cluster. Click a plugin to manage it.") \
            .classes("text-sm muted")
        plugins = table([
            {"headerName": "Plugin", "field": "name", "minWidth": 140},
            {"headerName": "Version", "field": "version", "maxWidth": 120},
            {"headerName": "Status", "field": "status", "maxWidth": 130, ":cellRenderer": "statusRenderer"},
            {"headerName": "Enabled", "field": "enabled_text", "maxWidth": 110},
            {"headerName": "Previous", "field": "previous", "maxWidth": 120},
            {"headerName": "Installed", "field": "installed"},
            {"headerName": "Description", "field": "description", "minWidth": 260},
        ], selection=True, row_key="name")

    async def refresh():
        data = await request("GET", "/v1/plugins")
        set_rows(plugins, [{**plugin, "previous": plugin.get("previous_version") or "—",
                            "enabled_text": "Yes" if plugin["enabled"] else "No",
                            "installed": stamp(plugin["installed_at_ms"])} for plugin in data])

    async def installed():
        await refresh()
        ui.navigate.reload()

    async def manage(event):
        plugin = event.args.get("data")
        if not plugin:
            return
        name = plugin["name"]
        with ui.dialog() as dialog, ui.card().classes("execution-dialog"):
            with ui.row().classes("items-center w-full"):
                ui.label(f'{name} {plugin["version"]}').classes("section-title")
                ui.label(plugin["status"]).classes(f'status-pill status-{plugin["status"]}')
                ui.space()
                ui.button(icon="close", on_click=dialog.close).props("flat round")
            if plugin.get("message"):
                ui.label(plugin["message"]).classes("mono text-sm")
            ui.label("Recent log").classes("section-title")
            log = ui.code("Loading…").classes("json-code w-full")
            keep_data = ui.checkbox("Keep the plugin's data when removing it", value=True)
            status = ui.label().classes("text-sm muted")
            with ui.row().classes("w-full items-center"):
                remove_button = ui.button("Remove", icon="delete", color="negative").props("outline no-caps")
                ui.space()
                if plugin.get("previous_version"):
                    rollback_button = ui.button(f'Roll back to {plugin["previous_version"]}',
                                                icon="history").props("outline no-caps color=orange")
                else:
                    rollback_button = None
                toggle_button = ui.button("Disable" if plugin["enabled"] else "Enable",
                                          icon="pause" if plugin["enabled"] else "play_arrow") \
                    .props("unelevated no-caps color=teal")

        async def act(method, path, done):
            status.set_text("Working…")
            try:
                await request(method, path, timeout=None)
                dialog.close()
                ui.notify(done, type="positive")
                await installed()
            except Exception as error:
                status.set_text(str(error))

        toggle_button.on_click(lambda: act(
            "POST", f'/v1/plugins/{quote(name, safe="")}/{"disable" if plugin["enabled"] else "enable"}',
            f'{"Disabled" if plugin["enabled"] else "Enabled"} {name}'))
        if rollback_button:
            rollback_button.on_click(lambda: act(
                "POST", f'/v1/plugins/{quote(name, safe="")}/rollback', f"Rolled back {name}"))
        remove_button.on_click(lambda: act(
            "DELETE", f'/v1/plugins/{quote(name, safe="")}?keep_data={str(keep_data.value).lower()}',
            f"Removed {name}"))
        dialog.open()
        try:
            response = await client.get(f'/v1/plugins/{quote(name, safe="")}/log')
            log.set_content(response.text or "(empty)")
        except Exception as error:
            log.set_content(f"Unavailable: {error}")

    install_button.on_click(lambda: install_dialog(installed).open())
    plugins.classes("clickable-grid").on("cellClicked", manage, ["data"])
    try:
        await refresh()
    except Exception as error:
        notify_error(error)
    ui.timer(2, refresh)


@ui.page("/plugin/{name}")
async def plugin_page(name: str):
    with await layout(f"plugin-{name}"):
        ui.element("iframe").props(f'src="/plugins/{quote(name, safe="")}/gui/"') \
            .classes("plugin-frame")


@ui.page("/executions")
async def executions_page():
    with await layout("executions"):
        with ui.row().classes("items-center w-full"):
            ui.label("Execution history").classes("section-title")
            ui.space()
            ui.label("Click an execution for full details").classes("text-xs muted")
        executions = table([
            {"headerName": "ID", "field": "short", "maxWidth": 130, "cellClass": "mono"},
            {"headerName": "Pool", "field": "pool_name"},
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
        data, names = await asyncio.gather(
            request("GET", "/v1/executions?limit=1000"), pool_names()
        )
        data.sort(key=lambda item: item["created_at_ms"], reverse=True)
        rows = []
        for run in data:
            end = run["finished_at_ms"] or int(time.time() * 1000)
            duration = f'{(end - run["started_at_ms"]) / 1000:.2f}s' if run["started_at_ms"] else "—"
            rows.append({**run, "short": run["id"][:8], "short_node": (run["node"] or "—")[:8],
                         "pool_name": names.get(run["pool"], run["pool"]),
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
    with await layout("vms"):
        with ui.row().classes("items-center w-full"):
            ui.label("MicroVMs").classes("section-title")
            ui.space()
            ui.label("Live inventory · 1 second").classes("text-xs muted")
        vms = table([
            {"headerName": "ID", "field": "short", "maxWidth": 130, "cellClass": "mono"},
            {"headerName": "Status", "field": "status", "maxWidth": 120, ":cellRenderer": "statusRenderer"},
            {"headerName": "Owner", "field": "resource", "minWidth": 150},
            {"headerName": "Pool", "field": "pool_name", "minWidth": 150},
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
            data, pools = await asyncio.gather(
                request("GET", "/v1/vms"), request("GET", "/v1/pools")
            )
            pools = {pool["name"]: pool for pool in pools}
            rows = [{
                "id": vm["id"],
                "short": vm["id"][:8],
                "status": vm["status"],
                "resource": owner_label(pools.get(vm["pool"], {}).get("managed_by")),
                "pool_name": pools.get(vm["pool"], {}).get("display_name") or vm["pool"],
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
    with await layout("settings"):
        storage = await request("GET", "/v1/settings/storage")
        with ui.card().classes("panel"):
            ui.label("Execution storage").classes("section-title")
            ui.label("Completed executions are a temporary buffer and may be evicted early when full.") \
                .classes("text-sm muted")
            with ui.row().classes("items-end w-full"):
                retention = ui.number("Retention", value=storage["retention_hours"],
                                      min=1, max=24, suffix="hours").classes("w-48")
                limit = ui.number("Maximum storage", value=storage["max_storage_bytes"] / 1024**3,
                                  min=.1, step=.1, suffix="GiB").classes("w-56")
                save_storage = ui.button("Save", icon="save").props("unelevated no-caps color=teal")
            usage = ui.label(
                f'{size(storage["used_storage_bytes"])} used of {size(storage["max_storage_bytes"])}'
            ).classes("text-sm muted")

            async def update_storage():
                save_storage.disable()
                try:
                    updated = await request("PUT", "/v1/settings/storage", json={
                        "retention_hours": int(retention.value),
                        "max_storage_bytes": round(float(limit.value) * 1024**3),
                    })
                    usage.text = (
                        f'{size(updated["used_storage_bytes"])} used of '
                        f'{size(updated["max_storage_bytes"])}'
                    )
                    ui.notify("Storage settings saved", type="positive")
                except Exception as error:
                    notify_error(error)
                finally:
                    save_storage.enable()

            save_storage.on_click(update_storage)
        with ui.card().classes("panel"):
            ui.label("Join nodes").classes("section-title")
            ui.label("Generate a single-use command for an existing ScrapeRack installation.") \
                .classes("text-sm muted")
            with ui.row().classes("items-end w-full"):
                controller_url = ui.input(
                    "Controller URL",
                    value=app.storage.general.get("public_controller", PUBLIC_CONTROLLER),
                    placeholder="http://scraperack.example.com:7070",
                ).classes("grow")
                node_name = ui.input("Node name", placeholder="Defaults to its hostname").classes("w-64")
                generate = ui.button("Generate command", icon="add_link").props(
                    "unelevated no-caps color=teal"
                )
            command = ui.textarea("Join command").props("readonly autogrow").classes("w-full")
            command.set_visibility(False)
            with ui.row().classes("items-center w-full"):
                join_status = ui.label().classes("text-sm muted")
                ui.space()
                copy = ui.button("Copy", icon="content_copy").props("outline no-caps")
                copy.set_visibility(False)
            current = {"id": None, "expires": 0}

            async def create_token():
                url = controller_url.value.strip().rstrip("/")
                if not url.startswith(("http://", "https://")):
                    return ui.notify("Enter a complete HTTP or HTTPS controller URL", type="warning")
                generate.disable()
                try:
                    token = await request("POST", "/v1/join-tokens")
                    parts = ["sudo scraperack join", "--controller", shlex.quote(url),
                             "--token", shlex.quote(token["token"])]
                    if node_name.value.strip():
                        parts.extend(("--name", shlex.quote(node_name.value.strip())))
                    command.value = " ".join(parts)
                    command.set_visibility(True)
                    copy.set_visibility(True)
                    current.update(id=token["id"], expires=token["expires_at_ms"])
                    app.storage.general["public_controller"] = url
                except Exception as error:
                    notify_error(error)
                finally:
                    generate.enable()

            async def token_status():
                if not current["id"]:
                    return
                try:
                    token = await request("GET", f'/v1/join-tokens/{current["id"]}')
                    if token["status"] == "pending":
                        remaining = max(0, (token["expires_at_ms"] - int(time.time() * 1000)) // 1000)
                        join_status.text = f'Pending · expires in {remaining // 60}:{remaining % 60:02d}'
                    else:
                        join_status.text = token["status"].title()
                        current["id"] = None
                except Exception as error:
                    join_status.text = f"Unavailable: {error}"

            generate.on_click(create_token)
            copy.on_click(lambda: ui.clipboard.write(command.value))
            ui.timer(1, token_status)
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
