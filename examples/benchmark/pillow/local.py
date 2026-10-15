import time

from PIL import Image


start = time.perf_counter()
result = Image.effect_noise((4096, 4096), 100).resize(
    (1024, 1024), Image.Resampling.LANCZOS,
).getextrema()
print(f"{time.perf_counter() - start:.3f}s; extrema: {result}")
