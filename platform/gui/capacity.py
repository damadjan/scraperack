from html import escape


def pressure(used, total):
    if total is None:
        return ""
    if used >= total:
        return "hot"
    return "warm" if used >= total * .85 else ""


def meter(used, total, idle=0, tone=""):
    if not total:
        return ""
    active = min(100, used / total * 100)
    warm = min(100 - active, idle / total * 100)
    return (f'<div class="cap-meter {tone}" aria-hidden="true">'
            f'<i style="width:{active:.2f}%"></i><i style="width:{warm:.2f}%"></i></div>')


def tile(label, value, detail, bar="", tone=""):
    return (f'<article class="cap-tile {tone}"><h3>{label}</h3>'
            f'<div class="cap-value">{value}</div>{bar}<p>{detail}</p></article>')


def ram(kib):
    return f"{kib / (1024 * 1024):,.1f} GiB" if kib >= 1024 * 1024 else f"{kib / 1024:,.0f} MiB"


def pool_signal(pool):
    if pool["placement"]:
        return "hot", pool["placement"]
    if pool["limit"] is not None and pool["active"] >= pool["limit"]:
        return "hot", "At VM limit"
    if pool["queued"]:
        return "warm", "Tasks waiting"
    if pressure(pool["active"], pool["limit"]) == "warm":
        return "warm", "Few slots free"
    return "", ""


def pool_tile(pool):
    active, idle, limit = pool["active"], pool["idle"], pool["limit"]
    tone, signal = pool_signal(pool)
    details = [f"{idle:,} warm", f'{pool["queued"]:,} queued']
    if signal:
        details.append(f'<strong>{escape(signal)}</strong>')
    value = f"{active:,} <small>/ {limit:,} slots</small>" if limit is not None else f"{active:,} <small>active · uncapped</small>"
    return tile(escape(pool["label"]), value, " · ".join(details), meter(active, limit, idle, tone), tone)


def resource_view(resource):
    used, total = resource["used"], resource["total"]
    unit = escape(resource["unit"] or "")
    tone = pressure(used, total)
    value = f"{used:,} / {total:,}" if total is not None else f"{used:,}"
    status = {"warm": "High usage"}.get(tone)
    if tone == "hot":
        status = "Over limit" if used > total else "At limit" if total else "No capacity"
    note = f"<p>{status}</p>" if status else ""
    return (f'<div class="cap-resource {tone}"><div><strong>{escape(resource["label"])}</strong>'
            f'<span>{value} {unit}</span></div>{meter(used, total, tone=tone)}{note}</div>')


def capacity_html(data):
    nodes, pools = data["nodes"], data["pools"]
    plugins = {plugin["name"]: plugin for plugin in data["plugins"]}
    live = [node for node in nodes if node["online"]]
    running = sum(node["running"] for node in live)
    idle = sum(node["idle"] for node in live)
    queued = sum(pool["queued"] for pool in pools)
    limited = sum(pool_signal(pool)[0] == "hot" for pool in pools)
    memory = [node["host_memory"] for node in live if node["host_memory"]]
    total_ram = sum(sample["total_kib"] for sample in memory)
    free_ram = sum(sample["available_kib"] for sample in memory)
    reserved_ram = 1024 * sum(node["used_memory_mib"] for node in live)
    vm_ram = 1024 * sum(node["memory_mib"] for node in live)
    cpu = sum(node["used_cpu_millis"] for node in live)
    cpu_limit = sum(node["cpu_millis"] for node in live)
    ram_tone = pressure(total_ram - free_ram, total_ram) if total_ram else ""
    cpu_tone = pressure(cpu, cpu_limit) if cpu_limit else ""
    ram_value = f'{ram(total_ram - free_ram)} <small>/ {ram(total_ram)} used</small>' if memory else "—"
    ram_detail = (f"{ram(free_ram)} available" if memory else "Memory telemetry unavailable")
    if memory and len(memory) < len(live):
        ram_detail += f" · {len(memory)}/{len(live)} workers reporting"
    if live:
        ram_detail += f"<br>{ram(reserved_ram)} / {ram(vm_ram)} reserved for VMs"
    overview = (
        tile("MicroVMs", f'{running:,} <small>/ {running + idle:,} running</small>',
             f"{idle:,} idle · {queued:,} queued<br>{limited:,} of {len(pools):,} pools blocked or at limit",
             meter(running, running + idle, idle), "hot" if limited else "warm" if queued else "")
        + tile("Worker RAM", ram_value, ram_detail, meter(total_ram - free_ram, total_ram, tone=ram_tone), ram_tone)
        + tile("vCPU allocation", f'{cpu / 1000:g} <small>/ {cpu_limit / 1000:g} reserved</small>' if live else "—",
               f"{len(live):,} / {len(nodes):,} workers online", meter(cpu, cpu_limit, tone=cpu_tone), cpu_tone)
    )

    def plugin_state(plugin):
        if plugin is None:
            return "hot", "Missing"
        if not plugin["enabled"]:
            return "", "Disabled"
        state = plugin["status"]
        return {"failed": "hot", "starting": "warm", "stopped": "hot"}.get(state, ""), escape(state.title())

    owned = {}
    for pool in pools:
        owned.setdefault(pool["owner"], []).append(pool)
    flows = []
    for name, plugin in plugins.items():
        tone, state = plugin_state(plugin)
        report = plugin["report"] or {}
        resources = "".join(map(resource_view, report.get("resources", [])))
        waiting = report.get("queued")
        queue = f'<p class="{"warm" if waiting else "muted"}">{waiting:,} queued</p>' if waiting is not None else ""
        dependencies = []
        for dep in report.get("dependencies", []):
            upstream = plugins.get(dep)
            dep_tone, dep_state = plugin_state(upstream)
            if upstream and not upstream["enabled"]:
                dep_tone = "hot"
            elif upstream and upstream["status"] == "running":
                snapshot = upstream["report"] or {}
                alerts = [pool_signal(pool)[0] for pool in owned.get(dep, [])]
                alerts += [pressure(item["used"], item["total"]) for item in snapshot.get("resources", [])]
                if "hot" in alerts:
                    dep_tone, dep_state = "hot", "Capacity constrained"
                elif "warm" in alerts or snapshot.get("queued"):
                    dep_tone, dep_state = "warm", "Under pressure"
                elif not upstream["report"]:
                    dep_state = "No plugin telemetry"
            label = escape(dep.replace("-", " ").title())
            target = f'<a href="#cap-{escape(dep)}">{label}</a>' if upstream else label
            dependencies.append(f'<div class="cap-dependency {dep_tone}"><strong>{target}</strong><span>{dep_state}</span></div>')
        inputs = []
        if name in owned:
            inputs.append(f'<div><h3>Managed VM pools</h3>{"".join(map(pool_tile, owned[name]))}</div>')
        if dependencies:
            inputs.append(f'<div><h3>Dependencies</h3>{"".join(dependencies)}</div>')
        upstream = (f'<div class="cap-inputs">{"".join(inputs)}</div>'
                    '<div class="cap-arrow" aria-hidden="true"></div>') if inputs else ""
        metrics = resources + queue if report else '<p class="cap-empty">No recent plugin telemetry</p>'
        label = escape(name.replace("-", " ").title())
        flows.append(
            f'<article class="cap-flow{"" if inputs else " cap-solo"}" id="cap-{escape(name)}">{upstream}'
            f'<div class="cap-plugin {tone}"><div class="cap-plugin-title"><h3>{label}</h3>'
            f'<span>{state}</span></div>{metrics}</div></article>'
        )

    standalone = "".join(pool_tile(pool) for pool in pools if pool["owner"] not in plugins)
    workers = []
    for node in nodes:
        sample = node["host_memory"]
        detail = "Offline"
        tone, bar = "hot", ""
        if node["online"]:
            detail, tone = escape(node["status"].title()), ""
            used_cpu, total_cpu = node["used_cpu_millis"], node["cpu_millis"]
            detail += f" · {used_cpu / 1000:g} / {total_cpu / 1000:g} vCPU reserved"
            if sample:
                total, available = sample["total_kib"], sample["available_kib"]
                tone = pressure(total - available, total)
                detail += f" · {ram(total - available)} / {ram(total)} RAM used · {ram(available)} available"
                bar = meter(total - available, total, tone=tone)
            else:
                detail += " · RAM telemetry unavailable"
            cpu_tone = pressure(used_cpu, total_cpu) if total_cpu else ""
            tone = "hot" if "hot" in (tone, cpu_tone) else tone or cpu_tone
        workers.append(tile(escape(node["name"]), "", detail, bar, tone))
    return (
        f'<div class="cap-overview">{overview}</div>',
        '<section class="cap-section"><h2>Plugin capacity</h2>'
        f'{"".join(flows) or "<p class=cap-empty>No plugins installed</p>"}</section>'
        + (f'<section class="cap-section"><h2>Standalone VM pools</h2><div class="cap-grid">{standalone}</div></section>' if standalone else ""),
        '<section class="cap-section"><h2>Workers</h2>'
        f'<div class="cap-grid">{"".join(workers) or "<p class=cap-empty>No workers registered</p>"}</div></section>',
    )
