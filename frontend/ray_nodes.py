import json
from dataclasses import dataclass
from urllib.request import Request, urlopen


def meter_renderer(label_field):
    return f"""params => {{
        const value = Math.max(0, Math.min(100, Number(params.value) || 0));
        const meter = document.createElement('div');
        meter.className = 'node-meter';
        const fill = document.createElement('div');
        fill.className = 'node-meter-fill';
        fill.style.width = `${{value}}%`;
        const label = document.createElement('span');
        label.className = 'node-meter-label';
        label.textContent = params.data.{label_field};
        meter.append(fill, label);
        return meter;
    }}"""


NODE_COLUMNS = (
    {"field": "node", "headerName": "Node", "flex": 1.4, "minWidth": 150},
    {
        "field": "state",
        "headerName": "State",
        "width": 130,
        "maxWidth": 130,
        ":cellRenderer": """params => {
            const badge = document.createElement('span');
            badge.className = 'invocation-state';
            badge.style.color = params.value === 'ALIVE' ? '#22c55e' : '#ef4444';
            badge.textContent = params.value || 'UNKNOWN';
            return badge;
        }""",
        "cellStyle": {"display": "flex", "alignItems": "center"},
    },
    {"field": "ip", "headerName": "IP", "flex": 1, "minWidth": 120},
    {
        "field": "cpu_percent",
        "headerName": "CPU",
        "flex": 1.2,
        "minWidth": 130,
        ":cellRenderer": meter_renderer("cpu_label"),
        "cellClass": "node-meter-cell",
        "cellStyle": {"display": "flex", "alignItems": "center"},
    },
    {
        "field": "memory_percent",
        "headerName": "Memory",
        "flex": 1.8,
        "minWidth": 180,
        ":cellRenderer": meter_renderer("memory_label"),
        "cellClass": "node-meter-cell",
        "cellStyle": {"display": "flex", "alignItems": "center"},
    },
    {
        "field": "object_store_percent",
        "headerName": "Object Store",
        "flex": 1.8,
        "minWidth": 180,
        ":cellRenderer": meter_renderer("object_store_label"),
        "cellClass": "node-meter-cell",
        "cellStyle": {"display": "flex", "alignItems": "center"},
    },
    {
        "field": "disk_percent",
        "headerName": "Disk",
        "flex": 1.8,
        "minWidth": 180,
        ":cellRenderer": meter_renderer("disk_label"),
        "cellClass": "node-meter-cell",
        "cellStyle": {"display": "flex", "alignItems": "center"},
    },
)


@dataclass(frozen=True)
class NodeSnapshot:
    nodes: list[dict]

    @property
    def alive(self):
        return sum(node["state"] == "ALIVE" for node in self.nodes)


class RayNodeClient:
    def __init__(self, dashboard_url, timeout=5):
        self.url = dashboard_url.rstrip("/") + "/nodes?view=summary"
        self.timeout = timeout

    def fetch(self):
        request = Request(self.url, headers={"Accept": "application/json"})
        with urlopen(request, timeout=self.timeout) as response:
            return parse_node_response(json.load(response))


def parse_node_response(payload):
    if payload.get("result") is False:
        raise RuntimeError(payload.get("msg") or "Ray rejected the node query")
    data = payload.get("data")
    summary = data.get("summary") if isinstance(data, dict) else None
    if not isinstance(summary, list):
        raise TypeError("Ray returned an unexpected node response")
    return NodeSnapshot([display_node(node) for node in summary])


def display_node(node):
    raylet = node.get("raylet") or {}
    memory = node.get("mem") or []
    memory_total = number(memory, 0)
    memory_used = number(memory, 3)
    memory_percent = number(memory, 2) or percent(memory_used, memory_total)
    store = raylet.get("storeStats") or {}
    object_used = numeric(store.get("objectStoreBytesUsed"))
    object_total = numeric(store.get("objectStoreBytesAvail"))
    root_disk = (node.get("disk") or {}).get("/") or {}
    disk_used = numeric(root_disk.get("used"))
    disk_total = numeric(root_disk.get("total"))
    disk_percent = numeric(root_disk.get("percent")) or percent(disk_used, disk_total)
    hostname = node.get("hostname") or raylet.get("nodeManagerHostname") or "Unknown"
    cpu = numeric(node.get("cpu"))
    return {
        "node_id": raylet.get("nodeId") or "",
        "name": hostname,
        "node": f"{hostname} (Head)" if raylet.get("isHeadNode") else hostname,
        "state": raylet.get("state") or "UNKNOWN",
        "ip": node.get("ip") or raylet.get("nodeManagerAddress") or "",
        "cpu_percent": cpu,
        "cpu_label": f"{cpu:.1f}%",
        "memory_percent": memory_percent,
        "memory_label": usage(memory_used, memory_total, memory_percent),
        "object_store_percent": percent(object_used, object_total),
        "object_store_label": usage(object_used, object_total),
        "disk_percent": disk_percent,
        "disk_label": usage(disk_used, disk_total, disk_percent),
    }


def node_columns():
    return [column.copy() for column in NODE_COLUMNS]


def numeric(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def number(values, index):
    return numeric(values[index]) if len(values) > index else 0.0


def percent(used, total):
    return used / total * 100 if total else 0.0


def usage(used, total, used_percent=None):
    used_percent = percent(used, total) if used_percent is None else used_percent
    return f"{bytes_label(used)} / {bytes_label(total)} ({used_percent:.1f}%)"


def bytes_label(value):
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    amount = float(value)
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            return f"{amount:.0f} {unit}" if unit == "B" else f"{amount:.1f} {unit}"
        amount /= 1024
