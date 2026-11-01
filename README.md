# wred-lifespan

Async startup and shutdown hooks for Wred, through one function:
`lifespan_application`. Requires Python 3.11+, Wred 0.14, and an asyncio ASGI
server such as Uvicorn.

Install from PyPI:

```sh
python -m pip install wred-lifespan
```

```python
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
```

Save the example as `app.py`. Register routes on `api` and serve `app`:

```sh
python -m pip install uvicorn
uvicorn app:app --lifespan on --workers 2
```

## Configuration

```python
app = lifespan_application(
    api,
    on_start=[connect_database, warm_cache],
    on_shutdown=[close_cache, close_database],
    hook_timeout=10.0,
)
```

Both hook arguments are optional and accept one async callable or an iterable
of async callables. Each takes no arguments. Hooks run sequentially in the
supplied order; place dependent resource cleanup before its dependencies.
Iterables are captured when wrapping the app and can be reused across lifespans.
Use `functools.partial` on an async function to bind arguments.

`hook_timeout` defaults to **30 seconds per hook** and must be positive and
finite. Each cleanup hook gets a fresh timeout, so a hung hook cannot consume
the next hook's budget. For N shutdown hooks, allow at least N × `hook_timeout`
for cleanup in your process supervisor's termination grace period, in addition
to time for draining requests. Startup failure can take the startup hooks' time
plus the entire cleanup budget before it is reported.

Synchronous hooks are rejected during configuration. Hooks must use
nonblocking async operations and cooperate with cancellation. Python's
[`asyncio.timeout`](https://docs.python.org/3.11/library/asyncio-task.html#asyncio.timeout)
cannot forcibly stop blocking code or a coroutine that indefinitely suppresses
cancellation. Set timeouts in external clients too. Moving blocking work to a
thread does not make that thread cancellable.

## Cleanup and failure behavior

- Startup completion is sent only after every startup hook finishes.
- Startup exception or timeout stops subsequent startup hooks and runs **all
  shutdown hooks** as rollback before sending `lifespan.startup.failed`.
- Shutdown attempts every hook even if an earlier hook fails or times out.
  It sends `lifespan.shutdown.failed` if any cleanup failed, including all
  ordinary errors in the diagnostic; otherwise it sends completion.
- Cancellation, a broken lifespan connection, or an unexpected message after
  startup begins also triggers cleanup. The original exception propagates;
  secondary cleanup errors are logged through the `wred_lifespan` logger.
- Cancellation during cleanup interrupts that hook; remaining hooks are still
  attempted before cancellation propagates. Interrupted hooks are not retried.
  Process-exit exceptions and forced process termination can prevent cleanup.

Startup and shutdown callbacks are independent, so the wrapper cannot infer
which acquisitions correspond to which cleanup. Cleanup hooks must tolerate
missing or partially initialized resources. For example, with HTTPX installed:

```python
import httpx

client = None


async def startup():
    global client
    client = httpx.AsyncClient(timeout=5.0)
    # If this fails, shutdown still closes the client.
    response = await client.get("https://example.com")
    response.raise_for_status()


async def shutdown():
    global client
    if client is not None:
        await client.aclose()
        client = None
```

Assign acquired resources before performing later operations that can fail.
An acquisition that fails before returning a resource must clean up its own
partial work. Prefer separate shutdown hooks for independent resources so a
failure closing one cannot skip closing another.

## Deployment

Hooks run once per lifespan connection, usually once per worker, in the event
loop serving that worker. They do not run at import or per request. Hook-owned
resources must belong to the correct worker/event loop; the wrapper does not
provide shared application state or cluster-wide coordination. Registering a
scheduled job in startup therefore starts one job per worker.

Wrap the application once. The wrapper owns the ASGI lifespan protocol and does
not compose another application's existing lifespan handler. HTTP and other
scopes are forwarded unchanged. The earlier `on_start` and `on_shutdown`
decorator exports have been removed; those names now appear only as keyword
arguments to `lifespan_application`.

The Linux integration test runs two real Uvicorn workers, concurrent HTTP
clients, worker replacement, SIGHUP restarts, and SIGTERM shutdown. It checks
that requests only see initialized resources and every gracefully stopped
worker executes cleanup. Uvicorn may close accepted idle connections during
restart; the test records these transport disconnects separately and permits
them only during restart. For deployments requiring uninterrupted requests,
drain traffic at the load balancer before terminating instances. Lifespan hooks
alone cannot guarantee zero downtime or cleanup after SIGKILL.

## Development

From a source checkout:

```sh
python -m pip install -e '.[test]'
python -m pytest -s
python -m pytest -m 'not integration'  # fast protocol tests only
python -m build
python benchmarks/request_overhead.py
```

The benchmark compares raw Wred with the wrapper using in-process HTTP ASGI
calls. It measures wrapper overhead, not network throughput or production
capacity. The worker test is a finite deployment check, not a long-running soak.
