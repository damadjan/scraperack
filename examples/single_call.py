import socket

import scraperack


@scraperack.function
def identify(message):
    return {"message": message, "node": socket.gethostname()}


print(identify("hello from ScrapeRack"))
