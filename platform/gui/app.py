import asyncio
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
function progressRenderer(params) {
  const row = params.data
  if (row.kind !== 'import') return ''
  const escape = value => String(value).replace(/[&<>"']/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]))
  const detail = `<small class="${row.status === 'failed' ? 'status-error' : ''}">${escape(row.detail)}</small>`
  if (row.status !== 'importing') return `<div class="progress-cell">${detail}</div>`
  const bar = row.progress == null
    ? '<div class="progress-bar indeterminate"><span></span></div>'
    : `<div class="progress-bar"><span style="width:${(row.progress * 100).toFixed(1)}%"></span></div>`
  return `<div class="progress-cell">${bar}${detail}</div>`
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


def _stroke_icon(body):
    return ('<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" '
            f'stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">{body}</svg>')


NAV_ICONS = {
    "home": _stroke_icon('<path d="M3.5 10.5 12 3.5l8.5 7"/>'
                         '<path d="M5.5 9v10.5a1 1 0 0 0 1 1H10v-5.5h4v5.5h3.5a1 1 0 0 0 1-1V9"/>'),
    "plugins": _stroke_icon('<rect x="3.5" y="3.5" width="7" height="7" rx="1.75"/>'
                            '<rect x="3.5" y="13.5" width="7" height="7" rx="1.75"/>'
                            '<rect x="13.5" y="13.5" width="7" height="7" rx="1.75"/>'
                            '<path d="M17 3.5v7M13.5 7h7"/>'),
    "vms": _stroke_icon('<rect x="6" y="6" width="12" height="12" rx="2"/>'
                        '<rect x="9.5" y="9.5" width="5" height="5" rx="1"/>'
                        '<path d="M9.5 2.5V6M14.5 2.5V6M9.5 18v3.5M14.5 18v3.5'
                        'M2.5 9.5H6M2.5 14.5H6M18 9.5h3.5M18 14.5h3.5"/>'),
    "pools": _stroke_icon('<rect x="2.5" y="3.5" width="8" height="7" rx="1.75"/>'
                          '<rect x="13.5" y="3.5" width="8" height="7" rx="1.75"/>'
                          '<rect x="8" y="13.5" width="8" height="7" rx="1.75"/>'
                          '<path d="M6.5 10.5v1.5a1 1 0 0 0 1 1M17.5 10.5v1.5a1 1 0 0 1-1 1"/>'),
    "images": _stroke_icon('<path d="M12 3 3 7.75l9 4.75 9-4.75z"/>'
                           '<path d="m3 12 9 4.75L21 12"/><path d="m3 16.25 9 4.75 9-4.75"/>'),
    "nodes": _stroke_icon('<rect x="3" y="3.5" width="18" height="7" rx="2"/>'
                          '<rect x="3" y="13.5" width="18" height="7" rx="2"/>'
                          '<path d="M7 7h.01M7 17h.01M11 7h6M11 17h6"/>'),
    "settings": _stroke_icon('<path d="M4 6.5h8.5M16.5 6.5H20M4 12h2.5M10.5 12H20M4 17.5h10M18 17.5h2"/>'
                             '<circle cx="14.5" cy="6.5" r="2"/><circle cx="8.5" cy="12" r="2"/>'
                             '<circle cx="16" cy="17.5" r="2"/>'),
    "plugin": _stroke_icon('<path d="M20.5 7.5 12 3 3.5 7.5v9L12 21l8.5-4.5z"/>'
                           '<path d="M3.5 7.5 12 12l8.5-4.5M12 12v9"/>'),
}
# The Python logo, as published by the Python Software Foundation.
PLUGIN_ICONS = {
    "python": '''<svg viewBox="0.21 -0.077 110 110" aria-hidden="true">
<linearGradient id="py-blue" gradientUnits="userSpaceOnUse" x1="63.8159" y1="56.6829" x2="118.4934" y2="1.8225"
 gradientTransform="matrix(1 0 0 -1 -53.2974 66.4321)"><stop offset="0" stop-color="#387EB8"/>
<stop offset="1" stop-color="#366994"/></linearGradient>
<path fill="url(#py-blue)" d="M55.023-0.077c-25.971,0-26.25,10.081-26.25,12.156c0,3.148,0,12.594,0,12.594h26.75v3.781c0,0-27.852,0-37.375,0c-7.949,0-17.938,4.833-17.938,26.25c0,19.673,7.792,27.281,15.656,27.281c2.335,0,9.344,0,9.344,0s0-9.765,0-13.125c0-5.491,2.721-15.656,15.406-15.656c15.91,0,19.971,0,26.531,0c3.902,0,14.906-1.696,14.906-14.406c0-13.452,0-17.89,0-24.219C82.054,11.426,81.515-0.077,55.023-0.077z M40.273,8.392c2.662,0,4.813,2.15,4.813,4.813c0,2.661-2.151,4.813-4.813,4.813s-4.813-2.151-4.813-4.813C35.46,10.542,37.611,8.392,40.273,8.392z"/>
<linearGradient id="py-yellow" gradientUnits="userSpaceOnUse" x1="97.0444" y1="21.6321" x2="155.6665" y2="-34.5308"
 gradientTransform="matrix(1 0 0 -1 -53.2974 66.4321)"><stop offset="0" stop-color="#FFE052"/>
<stop offset="1" stop-color="#FFC331"/></linearGradient>
<path fill="url(#py-yellow)" d="M55.397,109.923c25.959,0,26.282-10.271,26.282-12.156c0-3.148,0-12.594,0-12.594H54.897v-3.781c0,0,28.032,0,37.375,0c8.009,0,17.938-4.954,17.938-26.25c0-23.322-10.538-27.281-15.656-27.281c-2.336,0-9.344,0-9.344,0s0,10.216,0,13.125c0,5.491-2.631,15.656-15.406,15.656c-15.91,0-19.476,0-26.532,0c-3.892,0-14.906,1.896-14.906,14.406c0,14.475,0,18.265,0,24.219C28.366,100.497,31.562,109.923,55.397,109.923z M70.148,101.454c-2.662,0-4.813-2.151-4.813-4.813s2.15-4.813,4.813-4.813c2.661,0,4.813,2.151,4.813,4.813S72.809,101.454,70.148,101.454z"/>
</svg>''',
}


def shell(active, plugins=()):
    """Page layout; ``plugins`` lists installed plugins for their sidebar entries."""
    ui.colors(primary="#d946ef")
    ui.dark_mode().enable()
    drawer = ui.left_drawer(value=True).props("width=220 breakpoint=800").classes("compact-drawer")
    with drawer:
        with ui.row().classes("items-center gap-2 px-2 py-2"):
            ui.icon("developer_board", size="28px").classes("text-primary")
            with ui.column().classes("gap-0"):
                ui.label("SCRAPERACK").classes("text-lg font-semibold text-white tracking-wide")
                ui.label("MicroVM cluster").classes("text-xs muted")
        ui.separator().classes("mb-1")
        plugin_links = [(f'plugin-{plugin["name"]}', f'/plugin/{quote(plugin["name"], safe="")}',
                         PLUGIN_ICONS.get(plugin["name"], NAV_ICONS["plugin"]),
                         plugin["name"].replace("-", " ").title())
                        for plugin in plugins if plugin.get("gui") and plugin.get("enabled")]
        groups = [
            (None, [("dashboard", "/", NAV_ICONS["home"], "Home")]),
            ("Plugins", [*plugin_links, ("plugins", "/plugins", NAV_ICONS["plugins"], "Manage plugins")]),
            ("Virtualization", [
                ("vms", "/vms", NAV_ICONS["vms"], "MicroVMs"),
                ("pools", "/pools", NAV_ICONS["pools"], "VM Pools"),
                ("images", "/images", NAV_ICONS["images"], "Images"),
            ]),
            ("Infrastructure", [
                ("nodes", "/nodes", NAV_ICONS["nodes"], "Nodes"),
                ("settings", "/settings", NAV_ICONS["settings"], "Settings"),
            ]),
        ]
        for group, links in groups:
            if group:
                ui.label(group).classes("nav-group")
            for key, path, icon, label in links:
                with ui.button(on_click=lambda path=path: ui.navigate.to(path), color=None) \
                        .props("flat no-caps").classes(f"nav-link {'nav-active' if key == active else ''}"):
                    ui.html(icon, sanitize=False).classes("nav-icon")
                    ui.label(label).classes("nav-label")
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
            ui.icon(icon, size="30px").classes("text-primary")
    return value_label, detail_label


def table(columns, rows=None, selection=False, row_key=None):
    """A data grid that fills the rest of the page and pages as many rows as fit."""
    options = {
        "columnDefs": columns,
        "rowData": rows or [],
        "defaultColDef": {"sortable": True, "filter": True, "resizable": True, "flex": 1, "minWidth": 100},
        "pagination": True,
        "paginationAutoPageSize": True,
        "paginationPageSizeSelector": False,
    }
    if selection:
        options["rowSelection"] = {"mode": "singleRow", "checkboxes": False, "enableClickSelection": True}
    if row_key:
        options[":getRowId"] = f"(params) => params.data.{row_key}"
    grid = ui.aggrid(options, auto_size_columns=False).classes("data-grid")
    return grid.classes("clickable-grid") if selection else grid


def set_rows(element, rows):
    if element.options["rowData"] != rows:
        element.options["rowData"] = rows
        element.update()


@ui.page("/")
async def dashboard():
    with await layout("dashboard"):
        with ui.element("div").classes("summary-grid"):
            node_value, node_detail = summary("dns", "Nodes", "—", "Loading")
            image_value, image_detail = summary("layers", "Images", "—", "Loading")
            pool_value, pool_detail = summary("hub", "Pools", "—", "Loading")
            vm_value, vm_detail = summary("memory", "MicroVMs", "—", "Loading")
        with ui.card().classes("panel"):
            with ui.row().classes("items-center w-full"):
                ui.label("Cluster allocation").classes("section-title")
                ui.space()
                capacity = ui.label().classes("text-sm muted")
            cpu_text = ui.label().classes("text-sm")
            memory_text = ui.label().classes("text-sm")
    async def refresh():
        try:
            nodes, images, pools, vms = await asyncio.gather(
                request("GET", "/v1/nodes"), request("GET", "/v1/images"),
                request("GET", "/v1/pools"), request("GET", "/v1/vms"),
            )
            ready_nodes = sum(node["status"] == "ready" for node in nodes)
            ready_images = sum(image["ready"] for image in images)
            running = sum(vm["status"] == "running" for vm in vms)
            for label, value in ((node_value, len(nodes)), (image_value, len(images)),
                                 (pool_value, len(pools)), (vm_value, len(vms))):
                label.text = str(value)
            node_detail.text = f"{ready_nodes} ready"
            image_detail.text = f"{ready_images} distributed"
            pool_detail.text = f'{sum(bool(pool.get("managed_by")) for pool in pools)} managed by plugins'
            vm_detail.text = f"{running} running"
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
        ], row_key="id")

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
        with ui.dialog() as dialog, ui.card().classes("detail-dialog"):
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

            with ui.dialog() as confirm, ui.card():
                ui.label(f'Revoke {selected["name"]}?').classes("section-title")
                ui.label("This removes the node from the cluster.") \
                    .classes("text-sm muted")
                with ui.row().classes("w-full justify-end"):
                    ui.button("Cancel", on_click=confirm.close).props("flat no-caps")
                    ui.button("Revoke", color="negative", on_click=lambda: confirm.submit(True)) \
                        .props("no-caps")

            async def revoke():
                if not await confirm:
                    return
                try:
                    await request("DELETE", f'/v1/nodes/{selected["id"]}')
                    dialog.close()
                    ui.notify(f'Revoked {selected["name"]}', type="positive")
                    await refresh()
                except Exception as error:
                    notify_error(error)

            with ui.row().classes("w-full items-center"):
                ui.button("Revoke", icon="link_off", color="negative", on_click=revoke) \
                    .props("outline no-caps")
                ui.space()
                ui.button("Cancel", on_click=dialog.close).props("flat no-caps")
                ui.button("Save", on_click=save_settings).props("no-caps")
        dialog.open()

    nodes.classes("clickable-grid").on("cellClicked", settings, ["data"])
    await refresh()
    ui.timer(1, refresh)


def import_detail(job):
    """What an import row shows under its progress bar."""
    if job["status"] == "failed":
        return job.get("error") or "Import failed"
    detail = job["stage"]
    if job["stage"] == "Pulling" and job.get("downloaded"):
        detail += f' · {size(job["downloaded"])}'
        if job.get("total"):
            detail += f' / {size(job["total"])}'
    return detail


def image_status(image):
    if not image["compatible"]:
        return "incompatible"
    return "ready" if image["ready"] else "distributing"


def image_rows(images, imports):
    rows = []
    for job in imports:
        if job["status"] == "succeeded":
            continue
        rows.append({
            "row_id": job["id"], "kind": "import", "name": job["name"],
            "status": {"running": "importing"}.get(job["status"], job["status"]),
            "progress": job.get("progress"), "detail": import_detail(job),
            "display_size": "—", "short_hash": "—", "id": job["image"],
        })
    for image in images:
        rows.append({
            "row_id": image["id"], "kind": "image", "name": image["name"],
            "status": image_status(image), "progress": None, "detail": "",
            "display_size": size(image["size"]), "short_hash": image["sha256"][:16], "id": image["id"],
        })
    return rows


def detail_card(label, value, classes=""):
    with ui.element("div").classes("detail-card"):
        ui.label(label).classes("detail-label")
        ui.label(value or "—").classes(f"detail-value {classes}")


def log_block(title="Log"):
    with ui.row().classes("items-center w-full"):
        ui.label(title).classes("section-title")
        ui.space()
        copy = ui.button("Copy log", icon="content_copy").props("flat dense no-caps")
    log = ui.code("Loading…", language="plaintext").classes("json-code w-full import-log")

    def copy_log():
        ui.clipboard.write(log.content)
        ui.notify("Log copied")

    copy.on_click(copy_log)
    return log


async def confirm(title, message, action):
    with ui.dialog() as dialog, ui.card():
        ui.label(title).classes("section-title")
        ui.label(message).classes("text-sm muted")
        with ui.row().classes("w-full justify-end"):
            ui.button("Keep", on_click=dialog.close).props("flat no-caps")
            ui.button(action, color="negative", on_click=lambda: dialog.submit(True)).props("no-caps")
    return await dialog


@ui.page("/images")
async def images_page():
    with await layout("images"):
        with ui.row().classes("items-center w-full"):
            ui.label("Images").classes("section-title")
            ui.space()
            import_button = ui.button("Import image", icon="download").props("unelevated no-caps")
        images = table([
            {"headerName": "Name", "field": "name"},
            {"headerName": "Status", "field": "status", "maxWidth": 140, ":cellRenderer": "statusRenderer"},
            {"headerName": "Progress", "field": "progress", "minWidth": 220, ":cellRenderer": "progressRenderer",
             "sortable": False, "filter": False},
            {"headerName": "Size", "field": "display_size", "maxWidth": 120},
            {"headerName": "SHA-256", "field": "short_hash", "cellClass": "mono"},
            {"headerName": "ID", "field": "id", "cellClass": "mono", "minWidth": 260},
        ], row_key="row_id")
    images.classes("clickable-grid")
    # Dialogs opened from grid clicks must not be built inside the grid element.
    dialogs = ui.element("div")
    state = {"images": [], "imports": []}

    async def refresh():
        state["images"], state["imports"] = await asyncio.gather(
            request("GET", "/v1/images"), request("GET", "/v1/images/imports"))
        set_rows(images, image_rows(state["images"], state["imports"]))

    async def safe_refresh():
        try:
            await refresh()
        except Exception as error:
            notify_error(error)

    def open_import(reference="", name=""):
        with dialogs, ui.dialog() as dialog, ui.card().classes("detail-dialog import-dialog"):
            with ui.row().classes("items-center w-full"):
                ui.label("Import Docker image").classes("section-title")
                ui.space()
                ui.button(icon="close", on_click=dialog.close).props("flat round")
            ui.label("Pull a linux/amd64 image and convert it to a microVM root filesystem. "
                     "The image needs /usr/bin/python3 for the guest agent. "
                     "The import keeps running in the background.").classes("text-sm muted")
            docker_reference = ui.input("Docker image", value=reference,
                                        placeholder="ghcr.io/acme/browser:latest").classes("w-full")
            docker_name = ui.input("Image name", value=name, placeholder="browser").classes("w-full")
            with ui.row().classes("w-full no-wrap"):
                docker_username = ui.input("Username", placeholder="Optional").classes("grow")
                docker_password = ui.input("Password", placeholder="Optional") \
                    .props("type=password").classes("grow")
            status = ui.label().classes("text-sm status-error")
            with ui.row().classes("w-full justify-end"):
                ui.button("Cancel", on_click=dialog.close).props("flat no-caps")
                submit = ui.button("Import", icon="download").props("unelevated no-caps")

        async def start():
            image_reference = docker_reference.value.strip()
            if not image_reference:
                return status.set_text("Docker image is required")
            image_name = docker_name.value.strip() \
                or image_reference.rsplit("/", 1)[-1].replace(":", "-")
            username = docker_username.value.strip()
            password = docker_password.value
            if bool(username) != bool(password):
                return status.set_text("Username and password must be provided together")
            submit.disable()
            try:
                await request("POST", "/v1/images/imports",
                              json={"name": image_name, "reference": image_reference,
                                    "username": username or None, "password": password or None})
                dialog.close()
                ui.notify(f"Importing {image_name}", type="positive")
                await safe_refresh()
            except Exception as error:
                status.set_text(str(error))
                submit.enable()

        submit.on_click(start)
        dialog.on("hide", dialog.delete)
        dialog.open()

    def show_import(job_id):
        with dialogs, ui.dialog() as dialog, ui.card().classes("detail-dialog"):
            with ui.row().classes("items-center w-full"):
                with ui.column().classes("gap-0"):
                    ui.label("Image import").classes("dialog-eyebrow")
                    title = ui.label().classes("dialog-title")
                pill = ui.label().classes("status-pill")
                ui.space()
                ui.button(icon="close", on_click=dialog.close).props("flat round")
            stage = ui.label().classes("text-sm")
            bar = ui.linear_progress(value=0, show_value=False, size="8px", color="primary")
            error_box = ui.label().classes("error-block mono text-sm w-full")
            error_box.set_visibility(False)
            log = log_block()
            with ui.row().classes("w-full items-center"):
                remove = ui.button("Cancel import", icon="delete", color="negative").props("outline no-caps")
                ui.space()
                retry = ui.button("Retry", icon="refresh").props("outline no-caps")
                retry.set_visibility(False)
                ui.button("Close", on_click=dialog.close).props("flat no-caps")
            timer = ui.timer(1, lambda: load())
        job = {}

        async def load():
            try:
                job.update(await request("GET", f"/v1/images/imports/{job_id}"))
            except Exception as error:
                log.set_content(f"Unavailable: {error}")
                timer.cancel()
                return
            running = job["status"] == "running"
            status = "importing" if running else job["status"]
            title.set_text(f'{job["name"]} · {job["reference"]}')
            pill.set_text(status)
            pill.classes(replace=f"status-pill status-{status}")
            stage.set_text(import_detail(job) if running
                           else f'Failed while: {job["stage"]}' if job["status"] == "failed" else job["stage"])
            bar.set_visibility(running)
            if job.get("progress") is None:
                bar.props("indeterminate")
            else:
                bar.props(remove="indeterminate")
                bar.set_value(job["progress"])
            error_box.set_text(job.get("error") or "")
            error_box.set_visibility(bool(job.get("error")))
            if log.content != (job.get("log") or "(empty)"):
                log.set_content(job.get("log") or "(empty)")
            remove.set_text("Cancel import" if running else "Dismiss")
            retry.set_visibility(job["status"] == "failed")
            if not running:
                timer.cancel()

        async def do_remove():
            if job.get("status") == "running" and not await confirm(
                    f'Cancel importing {job["name"]}?', "The download and conversion stop.", "Cancel import"):
                return
            try:
                await request("DELETE", f"/v1/images/imports/{job_id}")
                dialog.close()
                await safe_refresh()
            except Exception as error:
                notify_error(error)

        def do_retry():
            dialog.close()
            open_import(job["reference"], job["name"])

        remove.on_click(do_remove)
        retry.on_click(do_retry)
        dialog.on("hide", dialog.delete)
        dialog.open()

    async def show_image(image_id):
        image = next((item for item in state["images"] if item["id"] == image_id), None)
        if not image:
            return
        job = next((item for item in state["imports"] if item["image"] == image_id), None)
        status = image_status(image)
        log = None
        with dialogs, ui.dialog() as dialog, ui.card().classes("detail-dialog"):
            with ui.row().classes("items-center w-full"):
                with ui.column().classes("gap-0"):
                    ui.label("Image").classes("dialog-eyebrow")
                    ui.label(image["name"]).classes("dialog-title")
                ui.label(status).classes(f"status-pill status-{status}")
                ui.space()
                ui.button(icon="close", on_click=dialog.close).props("flat round")
            with ui.element("div").classes("detail-grid w-full"):
                detail_card("Size", size(image["size"]))
                detail_card("Distributed", "On every node" if image["ready"] else "Syncing to nodes")
                detail_card("Source", job["reference"] if job else "", "mono")
                detail_card("ID", image["id"], "mono")
            with ui.element("div").classes("payload-grid"):
                detail_card("SHA-256", image["sha256"], "mono")
                if image.get("docker_source"):
                    detail_card("Docker image ID", image["docker_source"], "mono")
            if job:
                log = log_block("Import log")
            else:
                ui.label("No import log. Logs are kept until the controller restarts.") \
                    .classes("text-sm muted")
            with ui.row().classes("w-full items-center"):
                delete = ui.button("Delete", icon="delete", color="negative").props("outline no-caps")
                ui.space()
                ui.button("Close", on_click=dialog.close).props("flat no-caps")

        async def do_delete():
            if not await confirm(f'Delete {image["name"]}?',
                                 "The image is removed from every node.", "Delete"):
                return
            try:
                await request("DELETE", f'/v1/images/{image["id"]}')
                dialog.close()
                ui.notify(f'Deleted {image["name"]}', type="positive")
                await safe_refresh()
            except Exception as error:
                notify_error(error)

        delete.on_click(do_delete)
        dialog.on("hide", dialog.delete)
        dialog.open()
        if log:
            try:
                full = await request("GET", f'/v1/images/imports/{job["id"]}')
                log.set_content(full.get("log") or "(empty)")
            except Exception as error:
                log.set_content(f"Unavailable: {error}")

    async def open_row(event):
        row = event.args.get("data")
        if not row:
            return
        if row["kind"] == "import":
            show_import(row["row_id"])
        else:
            await show_image(row["row_id"])

    import_button.on_click(lambda: open_import())
    images.on("cellClicked", open_row, ["data"])
    await safe_refresh()
    ui.timer(1, refresh)


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
                create_button = ui.button("Create", icon="add").props("unelevated no-caps color=primary")
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
    with ui.dialog().props("persistent") as dialog, ui.card().classes("detail-dialog"):
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
            install_button = ui.button("Install", icon="download_done").props("unelevated no-caps color=primary")
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
            install_button = ui.button("Install plugin", icon="add").props("unelevated no-caps color=primary")
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
        with ui.dialog() as dialog, ui.card().classes("detail-dialog"):
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
                    .props("unelevated no-caps color=primary")

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
            {"headerName": "Task", "field": "short_task", "cellClass": "mono"},
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
                "short_task": (vm.get("task") or "—")[:8],
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
            ui.label("Task storage").classes("section-title")
            ui.label("Completed tasks are a temporary buffer and may be evicted early when full.") \
                .classes("text-sm muted")
            with ui.row().classes("items-end w-full"):
                retention = ui.number("Retention", value=storage["retention_hours"],
                                      min=1, max=24, suffix="hours").classes("w-48")
                limit = ui.number("Maximum storage", value=storage["max_storage_bytes"] / 1024**3,
                                  min=.1, step=.1, suffix="GiB").classes("w-56")
                save_storage = ui.button("Save", icon="save").props("unelevated no-caps color=primary")
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
            ui.label("Generate a single-use command for an existing Scraperack installation.") \
                .classes("text-sm muted")
            with ui.row().classes("items-end w-full"):
                controller_url = ui.input(
                    "Controller URL",
                    value=app.storage.general.get("public_controller", PUBLIC_CONTROLLER),
                    placeholder="http://scraperack.example.com:7070",
                ).classes("grow")
                node_name = ui.input("Node name", placeholder="Defaults to its hostname").classes("w-64")
                generate = ui.button("Generate command", icon="add_link").props(
                    "unelevated no-caps color=primary"
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
