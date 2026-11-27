"""The homepage: the workers' hardware, then one funnel per plugin that reports capacity.

Each funnel runs left to right: what the plugin draws from (other plugins, its microVMs,
its own resources) narrows into what it delivers. Nothing is inferred from how full things
are and nothing is per project: a stage is the bottleneck when the plugin says so, when its
microVMs fail to start or can't be placed, or when it runs microVMs and a worker is
physically out of CPU, RAM or disk.
"""
from html import escape

FULL = .9  # a worker's CPU, RAM or disk at 90 % is out of room
ACRONYMS = {"mcp", "api", "vm", "vms"}


def title(name):
    return " ".join(word.upper() if word in ACRONYMS else word.title() for word in name.split("-"))


def size(value, unit="B"):
    for prefix in ("", "Ki", "Mi", "Gi", "Ti"):
        if value < 1024 or prefix == "Ti":
            return f"{value:,.0f} {prefix}{unit}" if not prefix else f"{value:,.1f} {prefix}{unit}"
        value /= 1024


def count(value, total=None, unit=None):
    text = f"{value:,}" if total is None else f"{value:,} <small>/ {total:,}</small>"
    return text + (f" <small>{escape(unit)}</small>" if unit else "")


def tone(share):
    return "hot" if share >= 1 else "warm" if share >= FULL else ""


class Stage:
    def __init__(self, label, value, fill, note="", link=None, kind=""):
        self.label, self.value, self.fill = label, value, fill
        self.note, self.link, self.kind = note, link, kind
        self.state = ""  # "ok" or "hot": a colour the plugin set


def worker_shares(node):
    """A worker's physical CPU, RAM and disk use as shares from 0 to 1."""
    usage, memory = node.get("host_usage") or {}, node.get("host_memory")
    shares = {}
    if usage.get("cpu_busy_permille") is not None:
        shares["CPU"] = usage["cpu_busy_permille"] / 1000
    if memory:
        shares["RAM"] = 1 - memory["available_kib"] / memory["total_kib"]
    if usage.get("disk_total_bytes"):
        shares["Disk"] = 1 - usage["disk_available_bytes"] / usage["disk_total_bytes"]
    return shares


def hardware_stage(nodes):
    """The tightest physical resource on any online worker."""
    worst = max(((share, what, node["name"]) for node in nodes if node["online"]
                 for what, share in worker_shares(node).items()), default=None)
    if worst is None:
        return Stage("Hardware", "—", 0, "No worker reports usage", kind="hardware")
    share, what, name = worst
    return Stage("Hardware", f"{share * 100:.0f}% <small>{what}</small>", min(share, 1),
                 f"Tightest: {escape(name)}", kind="hardware")


def vms_stage(pools):
    """All of a plugin's microVMs together; a pool that can't start or place them breaks it."""
    running = sum(pool["active"] for pool in pools)
    warm = sum(pool["idle"] for pool in pools)
    waiting = sum(pool["queued"] for pool in pools)
    notes = [f"{warm:,} warm"] + ([f"{waiting:,} waiting"] if waiting else [])
    stage = Stage("MicroVMs", count(running, unit="running"), 0, " · ".join(notes), kind="vms")
    for pool in pools:
        problem = (f'Fail to start: {pool["failing"]["error"]}' if pool.get("failing")
                   else pool["placement"])
        if problem:
            stage.note, stage.kind = escape(problem), "vms broken"
            break
    return stage


class Funnel:
    def __init__(self, name, report, pools):
        self.name, self.report, self.pools = name, report, pools
        self.stages, self.bottleneck = [], None

    def build(self, plugins, funnels, nodes):
        self.stages = [dependency_stage(dep, plugins.get(dep), funnels.get(dep))
                       for dep in self.report.get("dependencies", [])]
        vms = hardware = None
        if self.report.get("vms") or any(pool.get("failing") or pool["placement"] for pool in self.pools):
            vms = vms_stage(self.pools)
            self.stages.append(vms)
        if self.report.get("vms"):
            hardware = hardware_stage(nodes)
            self.stages.append(hardware)
        resources = [(resource, resource_stage(resource)) for resource in self.report.get("resources", [])]
        self.stages += [stage for _, stage in resources]
        declared = [stage for resource, stage in resources if resource.get("bottleneck")]
        if vms and vms.kind == "vms broken":
            self.bottleneck = vms
        elif declared:
            self.bottleneck = declared[0]
        elif hardware and hardware.fill >= FULL:  # config limits never count; physical ones do
            self.bottleneck = hardware

    def output(self):
        output = self.report.get("output")
        queued = self.report.get("queued")
        if output:
            target = output.get("target")
            note = f"{queued:,} waiting" if queued else ""
            stage = Stage(output["label"], count(output["ready"], target, output.get("unit")),
                          min(output["ready"] / target, 1) if target else 1, note, kind="output")
            stage.shortfall = max(0, 1 - output["ready"] / target) if target else 0
            return stage
        stage = Stage("Waiting", count(queued or 0), 0, kind="output")
        stage.shortfall = 0
        return stage


def dependency_stage(name, plugin, funnel):
    if plugin is None:
        return Stage(title(name), "missing", 0, "Plugin not installed", kind="plugin")
    if funnel is None:
        state = "Disabled" if not plugin["enabled"] else plugin["status"].title()
        return Stage(title(name), "down", 0, escape(state), kind="plugin")
    output = funnel.report.get("output")
    if output:
        value = count(output["ready"], output.get("target"), output.get("unit"))
        fill = min(output["ready"] / output["target"], 1) if output.get("target") else 1
        note = escape(output["label"])
    else:
        value, fill, note = "running", 1, ""
    return Stage(title(name), value, fill, note, link=f"home-{name}", kind="plugin")


def resource_stage(resource):
    used, total = resource["used"], resource.get("total")
    fill = min(used / total, 1) if total else 0
    stage = Stage(resource["label"], count(used, total, resource.get("unit")), fill,
                  escape(resource.get("note") or ""))
    stage.state = {"ok": "ok", "bad": "hot"}.get(resource.get("state"), "")
    if stage.state:
        stage.fill = 1  # the colour is the message: fill the whole stage
    return stage


def stage_html(stage, index, total, bottleneck):
    # The funnel narrows from 0 % inset on the left to 28 % at the output.
    top = 28 * index / total
    bottom = 28 * (index + 1) / total
    clip = f"polygon(0 {top:.1f}%,100% {bottom:.1f}%,100% {100 - bottom:.1f}%,0 {100 - top:.1f}%)"
    if stage.kind == "output":  # a shortfall against the target: half empty is already serious
        level = "hot" if stage.shortfall >= .5 else "warm" if stage.shortfall > 0 else "ok" if stage.fill else ""
    else:
        level = "hot" if stage is bottleneck else stage.state
    classes = f"home-stage {stage.kind} {level}" + (" bottleneck" if stage is bottleneck else "")
    label = escape(stage.label)
    if stage.link:
        label = f'<a href="#{escape(stage.link)}">{label}</a>'
    flag = '<em>Bottleneck</em>' if stage is bottleneck else ""
    note = f"<p>{stage.note}</p>" if stage.note else ""
    return (f'<div class="{classes}" style="--clip:{clip};--fill:{stage.fill * 100:.1f}%">'
            f'<div class="home-stage-body">{flag}<h4>{label}</h4>'
            f'<div class="home-stage-value">{stage.value}</div>{note}</div></div>')


def funnel_html(funnel):
    stages = funnel.stages + [funnel.output()]
    body = "".join(stage_html(stage, index, len(stages), funnel.bottleneck)
                   for index, stage in enumerate(stages))
    if funnel.bottleneck:
        verdict, level = f"Bottleneck: {escape(funnel.bottleneck.label)}", "hot"
    else:
        verdict, level = "Healthy", "ok"
    return (f'<article class="home-funnel" id="home-{escape(funnel.name)}"><header>'
            f'<h3>{escape(title(funnel.name))}</h3><span class="home-verdict {level}">{verdict}</span></header>'
            f'<div class="home-track" style="--stages:{len(stages)}">{body}</div></article>')


def meter(label, used, total, detail):
    share = used / total if total else 0
    return (f'<div class="home-meter {tone(share)}"><div><span>{label}</span>'
            f'<strong>{share * 100:.0f}%</strong></div>'
            f'<i style="--fill:{min(share, 1) * 100:.1f}%"></i><small>{detail}</small></div>')


def missing(label, why="Not reported"):
    return (f'<div class="home-meter muted"><div><span>{label}</span><strong>—</strong></div>'
            f'<i style="--fill:0%"></i><small>{why}</small></div>')


def node_html(node):
    name = escape(node["name"])
    if not node["online"]:
        return (f'<article class="home-node hot"><header><h3>{name}</h3><span>Offline</span></header>'
                f'<p class="home-empty">No heartbeat. Its VMs are not available.</p></article>')
    usage = node.get("host_usage") or {}
    memory = node.get("host_memory")
    busy = usage.get("cpu_busy_permille")
    cpus = node["cpu_millis"] / 1000
    meters = [
        meter("CPU", busy, 1000, f'{node["used_cpu_millis"] / 1000:g} / {cpus:g} vCPU reserved by VMs')
        if busy is not None else missing("CPU", "Update the node to report CPU use"),
    ]
    if memory:
        used = (memory["total_kib"] - memory["available_kib"]) * 1024
        meters.append(meter("RAM", used, memory["total_kib"] * 1024,
                            f'{size(memory["available_kib"] * 1024)} free · '
                            f'{size(node["used_memory_mib"] * 1048576)} reserved by VMs'))
    else:
        meters.append(missing("RAM"))
    disk_total, disk_free = usage.get("disk_total_bytes"), usage.get("disk_available_bytes")
    if disk_total:
        meters.append(meter("Disk", disk_total - disk_free, disk_total,
                            f"{size(disk_free)} free of {size(disk_total)}"))
    else:
        meters.append(missing("Disk", "Update the node to report disk use"))
    vms = f'{node["running"]:,} running · {node["idle"]:,} warm'
    level = "" if node["status"] == "ready" else "warm"
    return (f'<article class="home-node {level}"><header><h3>{name}</h3>'
            f'<span>{escape(node["status"].title())} · {vms}</span></header>'
            f'<div class="home-meters">{"".join(meters)}</div></article>')


def home_html(data):
    """The page as (hardware, plugins) HTML sections."""
    nodes, pools = data["nodes"], data["pools"]
    plugins = {plugin["name"]: plugin for plugin in data["plugins"]}
    owned = {}
    for pool in pools:
        owned.setdefault(pool["owner"], []).append(pool)
    # Only plugins that report capacity get a funnel, in the order the plugins ask for.
    funnels = {name: Funnel(name, plugin["report"], owned.get(name, []))
               for name, plugin in plugins.items()
               if plugin.get("report") and plugin["enabled"] and plugin["status"] in ("running", "starting")}
    order = sorted(funnels.values(), key=lambda funnel: (funnel.report.get("position", 1000), funnel.name))
    for funnel in order:
        funnel.build(plugins, funnels, nodes)

    hardware = "".join(map(node_html, nodes)) or '<p class="home-empty">No workers registered.</p>'
    body = "".join(map(funnel_html, order)) or '<p class="home-empty">No plugin reports capacity.</p>'
    return (f'<section><h2>Hardware</h2><div class="home-nodes">{hardware}</div></section>',
            f'<section><h2>Plugins</h2>{body}</section>')
