import time

from scraperack import Cluster


def resize():
    from PIL import Image

    start = time.perf_counter()
    result = Image.effect_noise((4096, 4096), 100).resize(
        (1024, 1024), Image.Resampling.LANCZOS,
    ).getextrema()
    return time.perf_counter() - start, result


cluster = Cluster(url="http://192.168.2.16:7070")
start = time.perf_counter()
execution = cluster.wait(cluster.run(environment="pillow", function=resize)["id"])
cluster.close()
if execution["error"]:
    raise RuntimeError(execution["error"])
elapsed, result = execution["result"]["value"]["data"]
duration = (execution["finished_at_ms"] - execution["started_at_ms"]) / 1000
print(f"{elapsed:.3f}s Pillow; {duration:.3f}s execution; "
      f"{time.perf_counter() - start:.3f}s total; extrema: {result}")
