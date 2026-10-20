from contextvars import ContextVar

_context = ContextVar("scraperack_context", default=None)


def _set_context(variables, secrets):
    return _context.set({"variables": variables, "secrets": secrets})


def _reset_context(token):
    _context.reset(token)


def secret(name):
    context = _context.get()
    if context is None:
        raise RuntimeError("no active ScrapeRack invocation")
    try:
        return context["secrets"][name]
    except KeyError:
        raise KeyError(f"unknown ScrapeRack secret: {name}") from None


def variable(name, default=None):
    context = _context.get()
    if context is None:
        raise RuntimeError("no active ScrapeRack invocation")
    return context["variables"].get(name, default)
