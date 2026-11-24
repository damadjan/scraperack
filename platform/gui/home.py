"""The homepage: what limits the platform right now, drawn as one funnel per plugin.

Each funnel runs left to right: what the plugin draws from (other plugins, its VM pools,
its own resources) narrows into what it delivers. Every stage has a strain from 0 (idle)
to 1 (exhausted); the most strained stage of a funnel is its bottleneck.
"""
from html import escape

WARM, HOT = .85, 1.0


def tone(strain):
    return "hot" if strain >= HOT else "warm" if strain >= WARM else ""


def title(name):
    return name.replace("-", " ").title()


def size(value, unit="B"):
    for prefix in ("", "Ki", "Mi", "Gi", "Ti"):
        if value < 1024 or prefix == "Ti":
            return f"{value:,.0f} {prefix}{unit}" if not prefix else f"{value:,.1f} {prefix}{unit}"
        value /= 1024


def count(value, total=None, unit=None):
    text = f"{value:,}" if total is None else f"{value:,} <small>/ {total:,}</small>"
    return text + (f" <small>{escape(unit)}</small>" if unit else "")


class Stage:
    def __init__(self, label, value, fill, strain, note="", hint="", link=None, kind=""):
        self.label, self.value, self.fill, self.strain = label, value, fill, strain
        self.note, self.hint, self.link, self.kind = note, hint, link, kind


def pool_stage(pool):
    active, limit, queued = pool["active"], pool["limit"], pool["queued"]
    notes = [f'{pool["idle"]:,} warm']
    if queued:
        notes.append(f"{queued:,} waiting")
    if pool["placement"]:
        return Stage(pool["label"], count(active, limit, "VMs"), 1, 1.5, pool["placement"],
                     "Fix the pool's image, disks or size, or add a worker", kind="pool")
    fill = active / limit if limit else 0
    if queued:  # demand the pool can't serve yet
        strain = HOT if limit and active >= limit else WARM
    else:  # busy but keeping up: a full pool with nothing waiting is not a bottleneck
        strain = min(fill, 1) * .8
    hint = "Raise the pool's VM limit or add workers" if strain >= WARM else ""
    return Stage(pool["label"], count(active, limit, "VMs"), min(fill, 1), strain, " · ".join(notes), hint,
                 kind="pool")


def resource_stage(resource):
    used, total = resource["used"], resource.get("total")
    if total is None:
        strain, fill = 0, 0
    elif total == 0:
        strain, fill = 1.5, 1  # no capacity at all
    else:
        strain, fill = used / total, used / total
    return Stage(resource["label"], count(used, total, resource.get("unit")), min(fill, 1), strain,
                 hint=resource.get("hint") or "")


def output_shortfall(output):
    target = output.get("target")
    if not target:
        return 0
    return max(0, 1 - output["ready"] / target)


class Funnel:
    def __init__(self, name, plugin, pools):
        self.name, self.plugin, self.pools = name, plugin, pools
        self.report = (plugin or {}).get("report") or {}
        self.stages, self.state, self.down = [], "", False
        if plugin is None:
            self.state, self.down = "Not installed", True
        elif not plugin["enabled"]:
            self.state, self.down = "Disabled", True
        elif plugin["status"] != "running":
            self.state, self.down = plugin["status"].title(), plugin["status"] != "starting"

    def build(self, funnels):
        """Stages from source to output; dependencies are read from the other funnels."""
        for dep in self.report.get("dependencies", []):
            other = funnels.get(dep)
            if other is None:
                self.stages.append(Stage(title(dep), "missing", 0, 1.5, "Plugin not installed",
                                         f"Install the {title(dep)} plugin", kind="plugin"))
                continue
            output = other.report.get("output")
            strain = other.worst()
            if other.down:
                value, fill, note, strain = "down", 0, other.state, 1.5
            elif output:
                value = count(output["ready"], output.get("target"), output.get("unit"))
                fill = output["ready"] / output["target"] if output.get("target") else 1
                note = escape(output["label"])
                strain = max(strain, output_shortfall(output))
            else:
                value, fill, note = escape(other.state or "running"), 1, "No output reported"
            bottleneck = other.bottleneck()
            hint = f"Scale {title(dep)}: {bottleneck.label}" if bottleneck and strain >= WARM else ""
            self.stages.append(Stage(title(dep), value, min(fill, 1), strain, note, hint,
                                     link=f"home-{dep}", kind="plugin"))
        self.stages += [pool_stage(pool) for pool in self.pools]
        self.stages += [resource_stage(item) for item in self.report.get("resources", [])]

    def own_stages(self):
        return [stage for stage in self.stages if stage.kind != "plugin"]

    def worst(self):
        """Strain of the plugin's own stages, for funnels that depend on it."""
        return max((stage.strain for stage in self.own_stages()), default=0)

    def bottleneck(self):
        stage = max(self.stages, key=lambda stage: stage.strain, default=None)
        return stage if stage and stage.strain >= WARM else None

    def output(self):
        output = self.report.get("output")
        queued = self.report.get("queued")
        if self.down:
            return Stage("Not delivering", escape(self.state), 0, 1.5, kind="output")
        if output:
            target = output.get("target")
            note = f"{queued:,} waiting" if queued else ""
            return Stage(output["label"], count(output["ready"], target, output.get("unit")),
                         min(output["ready"] / target, 1) if target else 1,
                         output_shortfall(output), note, kind="output")
        if queued is not None:
            return Stage("Waiting", count(queued), 0, WARM if queued else 0, kind="output")
        return Stage("Output", "—", 0, 0, "The plugin reports no output", kind="output")


def stage_html(stage, index, total, bottleneck):
    # The funnel narrows from 0 % inset on the left to 28 % at the output.
    top = 28 * index / total
    bottom = 28 * (index + 1) / total
    clip = f"polygon(0 {top:.1f}%,100% {bottom:.1f}%,100% {100 - bottom:.1f}%,0 {100 - top:.1f}%)"
    if stage.kind == "output":  # a shortfall, not a load: half empty is already serious
        level = "hot" if stage.strain >= .5 else "warm" if stage.strain > 0 else "ok" if stage.fill else ""
    else:
        level = tone(stage.strain)
    classes = f"home-stage {stage.kind} {level}" + (" bottleneck" if stage is bottleneck else "")
    label = escape(stage.label)
    if stage.link:
        label = f'<a href="#{escape(stage.link)}">{label}</a>'
    flag = '<em>Bottleneck</em>' if stage is bottleneck else ""
    note = f"<p>{stage.note}</p>" if stage.note else ""
    hint = f'<p class="home-hint">{escape(stage.hint)}</p>' if stage is bottleneck and stage.hint else ""
    return (f'<div class="{classes}" style="--clip:{clip};--fill:{stage.fill * 100:.1f}%">'
            f'<div class="home-stage-body">{flag}<h4>{label}</h4>'
            f'<div class="home-stage-value">{stage.value}</div>{note}{hint}</div></div>')


def funnel_html(funnel):
    output = funnel.output()
    bottleneck = funnel.bottleneck()
    if funnel.down:
        bottleneck = None
    stages = funnel.stages + [output]
    body = "".join(stage_html(stage, index, len(stages), bottleneck)
                   for index, stage in enumerate(stages))
    if funnel.down:
        verdict, level = f"{escape(funnel.state)}", "hot"
    elif bottleneck:
        verdict, level = f"Limited by {escape(bottleneck.label)}", tone(bottleneck.strain)
    elif output.strain:
        verdict, level = "Filling up", ""
    elif not funnel.report and funnel.plugin:
        verdict, level = "No telemetry", ""
    else:
        verdict, level = "Healthy", "ok"
    anchor = f' id="home-{escape(funnel.name)}"' if funnel.plugin else ""
    return (f'<article class="home-funnel"{anchor}><header><h3>{escape(title(funnel.name))}</h3>'
            f'<span class="home-verdict {level}">{verdict}</span></header>'
            f'<div class="home-track" style="--stages:{len(stages)}">{body}</div></article>')


def meter(label, used, total, detail, strain=None):
    share = used / total if total else 0
    strain = share if strain is None else strain
    return (f'<div class="home-meter {tone(strain)}"><div><span>{label}</span>'
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


def node_alerts(node):
    name = node["name"]
    if not node["online"]:
        yield 1.5, name, "Offline", "Bring the node back or remove it"
        return
    usage = node.get("host_usage") or {}
    memory = node.get("host_memory")
    if usage.get("cpu_busy_permille") is not None:
        yield usage["cpu_busy_permille"] / 1000, name, "CPU busy", "Add a worker or lower pool VM limits"
    if memory:
        yield 1 - memory["available_kib"] / memory["total_kib"], name, "RAM used", \
            "Add RAM or a worker, or fewer warm VMs"
    if usage.get("disk_total_bytes"):
        yield 1 - usage["disk_available_bytes"] / usage["disk_total_bytes"], name, "Disk used", \
            "Grow the disk or delete unused images and snapshots"


def home_html(data):
    """The page as (summary, hardware, funnels) HTML sections."""
    nodes, pools = data["nodes"], data["pools"]
    plugins = {plugin["name"]: plugin for plugin in data["plugins"]}
    owned = {}
    for pool in pools:
        owned.setdefault(pool["owner"], []).append(pool)
    funnels = {name: Funnel(name, plugin, owned.get(name, [])) for name, plugin in plugins.items()}
    # Build dependencies after their sources: a plugin's own stages are known before others read them.
    for funnel in funnels.values():
        funnel.build({})
    for funnel in funnels.values():
        funnel.stages = []
        funnel.build(funnels)
    orphans = [pool for pool in pools if pool["owner"] not in plugins]
    if orphans:
        funnels[""] = Funnel("VM pools", {"enabled": True, "status": "running", "report": None}, orphans)
        funnels[""].build({})

    # What to scale: every bottleneck, worst first.
    alerts = []
    for node in nodes:
        for strain, name, what, hint in node_alerts(node):
            if strain >= WARM:
                alerts.append((strain, name, what, hint))
    for name, funnel in funnels.items():
        if funnel.down and funnel.plugin:
            alerts.append((1.5, title(funnel.name), funnel.state, "Open Plugins to start it or read its log"))
            continue
        stage = funnel.bottleneck()
        if stage and stage.kind != "plugin":  # a dependency's own funnel already lists it
            alerts.append((stage.strain, title(funnel.name), stage.label, stage.hint))
    alerts.sort(key=lambda alert: -alert[0])
    if alerts:
        items = "".join(
            f'<li class="{tone(strain)}"><strong>{escape(who)}</strong><span>{escape(what)}</span>'
            f'<small>{escape(hint)}</small></li>' for strain, who, what, hint in alerts[:8])
        summary = f'<section class="home-scale"><h2>Scale next</h2><ol>{items}</ol></section>'
    else:
        summary = ('<section class="home-scale ok"><h2>Scale next</h2>'
                   '<p>Nothing is constrained. Every plugin and worker has headroom.</p></section>')

    hardware = "".join(map(node_html, nodes)) or '<p class="home-empty">No workers registered.</p>'
    hardware = f'<section><h2>Hardware</h2><div class="home-nodes">{hardware}</div></section>'
    order = sorted(funnels.values(), key=lambda funnel: (funnel.name == "VM pools", funnel.name))
    body = "".join(funnel_html(funnel) for funnel in order) or '<p class="home-empty">No plugins installed.</p>'
    return summary, hardware, f'<section><h2>Plugins</h2>{body}</section>'
