"""The homepage: one funnel per plugin that reports capacity, then the workers' hardware.

Each funnel runs left to right: what the plugin draws from (other plugins, its VM pools,
its own resources) narrows into what it delivers. The page doesn't guess what limits a
plugin from how full things are. A stage is the bottleneck when the plugin says so, when a
pool is broken, or when a busy pool's workers are physically out of CPU, RAM or disk.
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
    def __init__(self, label, value, fill, note="", hint="", link=None, kind=""):
        self.label, self.value, self.fill = label, value, fill
        self.note, self.hint, self.link, self.kind = note, hint, link, kind


def worker_limits(node):
    """What a worker is physically out of, as labels: CPU, RAM, Disk."""
    usage, memory = node.get("host_usage") or {}, node.get("host_memory")
    out = []
    if (usage.get("cpu_busy_permille") or 0) >= FULL * 1000:
        out.append("CPU")
    if memory and 1 - memory["available_kib"] / memory["total_kib"] >= FULL:
        out.append("RAM")
    if usage.get("disk_total_bytes") and 1 - usage["disk_available_bytes"] / usage["disk_total_bytes"] >= FULL:
        out.append("Disk")
    return out


class Funnel:
    def __init__(self, name, report, pools):
        self.name, self.report, self.pools = name, report, pools
        self.stages, self.bottleneck = [], None

    def build(self, plugins, funnels, starved):
        for dep in self.report.get("dependencies", []):
            self.stages.append(dependency_stage(dep, plugins.get(dep), funnels.get(dep)))
        pools = [(pool, pool_stage(pool)) for pool in self.pools]
        resources = [(resource, resource_stage(resource)) for resource in self.report.get("resources", [])]
        self.stages += [stage for _, stage in pools + resources]
        broken = [stage for _, stage in pools if stage.kind == "pool broken"]
        declared = [stage for resource, stage in resources if resource.get("bottleneck")]
        busy = sorted(((pool["active"] + pool["queued"], stage) for pool, stage in pools
                       if pool["active"] or pool["queued"]), key=lambda item: -item[0])
        if broken or declared:
            self.bottleneck = (broken + declared)[0]
        elif starved and busy:  # config limits never count; workers out of hardware do
            self.bottleneck = busy[0][1]
            self.bottleneck.hint = f"Workers out of room ({starved}): add a worker, or free RAM or disk"

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
    link = f"home-{name}"
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
    if funnel.bottleneck:
        note += f" · limited by {escape(funnel.bottleneck.label)}"
    return Stage(title(name), value, fill, note, link=link, kind="plugin")


def pool_stage(pool):
    active, limit, queued = pool["active"], pool["limit"], pool["queued"]
    notes = [f'{pool["idle"]:,} warm']
    if queued:
        notes.append(f"{queued:,} waiting")
    value = count(active, limit, "VMs")
    failing = pool.get("failing")
    if failing:
        where = "every worker" if failing["workers"] >= failing["of"] else f'{failing["workers"]} of {failing["of"]} workers'
        return Stage(pool["label"], value, 1, f'VMs fail to start on {where}: {escape(failing["error"])}',
                     "Its VMs crash on start: check the pool's image and the plugin's guest code", kind="pool broken")
    if pool["placement"]:
        return Stage(pool["label"], value, 1, escape(pool["placement"]),
                     "Fix the pool's image or disks, or add a bigger worker", kind="pool broken")
    return Stage(pool["label"], value, min(active / limit, 1) if limit else 0, " · ".join(notes), kind="pool")


def resource_stage(resource):
    used, total = resource["used"], resource.get("total")
    fill = min(used / total, 1) if total else 0
    return Stage(resource["label"], count(used, total, resource.get("unit")), fill,
                 escape(resource.get("note") or ""), resource.get("hint") or "")


def stage_html(stage, index, total, bottleneck):
    # The funnel narrows from 0 % inset on the left to 28 % at the output.
    top = 28 * index / total
    bottom = 28 * (index + 1) / total
    clip = f"polygon(0 {top:.1f}%,100% {bottom:.1f}%,100% {100 - bottom:.1f}%,0 {100 - top:.1f}%)"
    if stage.kind == "output":  # a shortfall against the target: half empty is already serious
        level = "hot" if stage.shortfall >= .5 else "warm" if stage.shortfall > 0 else "ok" if stage.fill else ""
    else:
        level = "hot" if stage is bottleneck else ""
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
    hint = ""
    if funnel.bottleneck:
        verdict, level = f"Bottleneck: {escape(funnel.bottleneck.label)}", "hot"
        if funnel.bottleneck.hint:
            hint = f'<p class="home-hint hot">{escape(funnel.bottleneck.hint)}</p>'
    else:
        verdict, level = "Healthy", "ok"
    return (f'<article class="home-funnel" id="home-{escape(funnel.name)}"><header>'
            f'<h3>{escape(title(funnel.name))}</h3>{hint}<span class="home-verdict {level}">{verdict}</span></header>'
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


def flow_order(funnels):
    """A plugin follows what it draws from: start from the plugins that draw on the most."""
    order, seen = [], set()

    def visit(name):
        if name in seen or name not in funnels:
            return
        seen.add(name)
        for dep in funnels[name].report.get("dependencies", []):
            visit(dep)
        order.append(funnels[name])

    for name in sorted(funnels, key=lambda name: (-len(funnels[name].report.get("dependencies", [])), name)):
        visit(name)
    return order


def home_html(data):
    """The page as (plugins, hardware) HTML sections."""
    nodes, pools = data["nodes"], data["pools"]
    plugins = {plugin["name"]: plugin for plugin in data["plugins"]}
    owned = {}
    for pool in pools:
        owned.setdefault(pool["owner"], []).append(pool)
    # Only plugins that report capacity get a funnel.
    funnels = {name: Funnel(name, plugin["report"], owned.get(name, []))
               for name, plugin in plugins.items()
               if plugin.get("report") and plugin["enabled"] and plugin["status"] in ("running", "starting")}
    starved = ", ".join(f'{node["name"]} {"/".join(out)}' for node in nodes
                        if node["online"] and (out := worker_limits(node)))
    order = flow_order(funnels)
    for funnel in order:  # dependencies first, so their bottleneck is known when others read it
        funnel.build(plugins, funnels, starved)

    body = "".join(map(funnel_html, order)) or '<p class="home-empty">No plugin reports capacity.</p>'
    hardware = "".join(map(node_html, nodes)) or '<p class="home-empty">No workers registered.</p>'
    return (f'<section><h2>Plugins</h2>{body}</section>',
            f'<section><h2>Hardware</h2><div class="home-nodes">{hardware}</div></section>')
