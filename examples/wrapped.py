"""Run with: uvicorn examples.wrapped:app --lifespan on"""

from wred import App

from wred_lifespan import lifespan_application

api = App()


async def startup():
    print("Starting up")


async def shutdown():
    print("Shutting down")


@api.route("/")
async def hello(request):
    return {"hello": "world"}


app = lifespan_application(api, on_start=startup, on_shutdown=shutdown)
