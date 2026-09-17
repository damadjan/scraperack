import hashlib
import urllib.error
import urllib.request

import cloudpickle


def prepare(address, target):
    function = cloudpickle.dumps(target)
    digest = hashlib.sha256(function).hexdigest()
    url = f"{address}/functions/{digest}"

    try:
        with urllib.request.urlopen(
            urllib.request.Request(url, method="HEAD")
        ) as response:
            if response.status == 204:
                return function
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise RuntimeError(
                f"Cannot check function cache: HTTP {error.code}"
            ) from error
        try:
            urllib.request.urlopen(
                urllib.request.Request(
                    url,
                    data=function,
                    headers={"content-type": "application/octet-stream"},
                    method="PUT",
                )
            ).close()
        except urllib.error.HTTPError as upload_error:
            raise RuntimeError(
                f"Cannot upload function: HTTP {upload_error.code}"
            ) from upload_error
        except urllib.error.URLError as upload_error:
            raise RuntimeError(
                f"Cannot reach ScrapeRack at {address}: {upload_error.reason}"
            ) from upload_error
    except urllib.error.URLError as error:
        raise RuntimeError(
            f"Cannot reach ScrapeRack at {address}: {error.reason}"
        ) from error

    return digest
