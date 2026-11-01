"""Instrumented application for real process lifecycle tests."""

import asyncio
import os
from pathlib import Path

from wred import App

from wred_lifespan import lifespan_application

api = App()
resource = None
events = Path(os.environ["WRED_LIFESPAN_EVENTS"])


async def startup():
    global resource
    resource = (events / f"{os.getpid()}.resource").open("w")
    (events / f"{os.getpid()}.started").touch()


async def shutdown():
    if resource is not None:
        resource.close()
    (events / f"{os.getpid()}.stopped").touch()


@api.route("/")
async def hello(request):
    await asyncio.sleep(0.005)
    return {"pid": os.getpid(), "ready": resource is not None and not resource.closed}


app = lifespan_application(api, on_start=startup, on_shutdown=shutdown, hook_timeout=1)
