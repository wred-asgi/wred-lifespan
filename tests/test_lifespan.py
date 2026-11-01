import asyncio

import httpx
import pytest
from wred import App

from wred_lifespan import lifespan_application


async def exchange(app, events=("lifespan.startup", "lifespan.shutdown")):
    """Drive the ASGI protocol and return messages sent by the application."""
    incoming = asyncio.Queue()
    outgoing = []
    for event in events:
        incoming.put_nowait({"type": event})

    async def send(message):
        outgoing.append(message)

    async with asyncio.timeout(2):
        await app({"type": "lifespan"}, incoming.get, send)
    return outgoing


async def test_wred_serves_requests_between_completed_hooks():
    api = App()
    events = []

    async def startup():
        await asyncio.sleep(0)
        events.append("ready")

    async def shutdown():
        await asyncio.sleep(0)
        events.append("closed")

    @api.route("/")
    async def hello(request):
        return {"hello": "world", "ready": events == ["ready"]}

    app = lifespan_application(api, on_start=startup, on_shutdown=shutdown)

    assert events == []
    incoming, outgoing = asyncio.Queue(), asyncio.Queue()
    async with asyncio.timeout(2), asyncio.TaskGroup() as tasks:
        tasks.create_task(app({"type": "lifespan"}, incoming.get, outgoing.put))
        await incoming.put({"type": "lifespan.startup"})
        assert await outgoing.get() == {"type": "lifespan.startup.complete"}
        assert events == ["ready"]
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as client:
            for _ in range(2):
                response = await client.get("/")
                assert response.status_code == 200
                assert response.json() == {"hello": "world", "ready": True}
        assert events == ["ready"]
        await incoming.put({"type": "lifespan.shutdown"})
        assert await outgoing.get() == {"type": "lifespan.shutdown.complete"}
        assert events == ["ready", "closed"]


async def test_other_scopes_and_downstream_errors_pass_through():
    scope = {"type": "websocket", "state": {"token": "hello"}}
    incoming = asyncio.Queue()
    incoming.put_nowait({"type": "websocket.connect"})
    outgoing = []
    failure = RuntimeError("downstream error")

    async def downstream(scope, receive, send):
        assert await receive() == {"type": "websocket.connect"}
        await send({"type": "websocket.send", "text": scope["state"]["token"]})
        scope["state"]["visited"] = True
        raise failure

    async def unexpected():
        pytest.fail("Lifespan callbacks must not run for WebSocket scopes")

    async def send(message):
        outgoing.append(message)

    app = lifespan_application(downstream, on_start=unexpected, on_shutdown=unexpected)
    async with asyncio.timeout(2):
        with pytest.raises(RuntimeError) as caught:
            await app(scope, incoming.get, send)
    assert caught.value is failure
    assert scope["state"]["visited"] is True
    assert outgoing == [{"type": "websocket.send", "text": "hello"}]


@pytest.mark.parametrize("transport", ["receive", "send"])
async def test_transport_errors_propagate(transport):
    calls = []
    failure = OSError("connection lost")

    async def receive():
        if transport == "receive":
            raise failure
        return {"type": "lifespan.startup"}

    async def send(message):
        calls.append(message)
        raise failure

    with pytest.raises(OSError) as caught:
        await lifespan_application(App())({"type": "lifespan"}, receive, send)
    assert caught.value is failure
    assert calls == (
        [{"type": "lifespan.startup.complete"}] if transport == "send" else []
    )


@pytest.mark.parametrize(
    "events",
    [
        ["lifespan.shutdown"],
        ["lifespan.startup", "lifespan.startup"],
        ["unknown"],
    ],
)
async def test_out_of_order_messages_are_rejected(events):
    with pytest.raises(RuntimeError, match="Expected lifespan\\."):
        await exchange(lifespan_application(App()), events)


@pytest.mark.parametrize("option", ["on_start", "on_shutdown"])
@pytest.mark.parametrize("invalid", [lambda: None, [None], [lambda: None]])
def test_non_async_hooks_are_rejected_at_configuration(option, invalid):
    with pytest.raises(TypeError, match="hooks must be async"):
        lifespan_application(App(), **{option: invalid})


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf"), None, True])
def test_timeouts_must_be_positive_and_finite(timeout):
    with pytest.raises(ValueError, match="positive finite"):
        lifespan_application(App(), hook_timeout=timeout)


@pytest.mark.parametrize("option", [None, "on_start", "on_shutdown"])
async def test_optional_hooks_and_iterators_are_reusable_per_connection(option):
    events = []

    class Hook:
        async def __call__(self):
            events.append("called")

    options = {} if option is None else {option: iter([Hook()])}
    app = lifespan_application(App(), **options)
    for _ in range(2):
        assert await exchange(app) == [
            {"type": "lifespan.startup.complete"},
            {"type": "lifespan.shutdown.complete"},
        ]
    assert events == ([] if option is None else ["called", "called"])


@pytest.mark.parametrize("phase", ["startup", "shutdown"])
@pytest.mark.parametrize("suppress", [False, True])
async def test_timeouts_cancel_hooks_and_allow_remaining_cleanup(phase, suppress):
    events = []

    async def hang():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if not suppress:
                raise
        finally:
            events.append("cancelled")

    async def cleanup():
        events.append("cleaned")

    async def skipped():
        events.append("unexpected")

    app = lifespan_application(
        App(),
        on_start=[hang, skipped] if phase == "startup" else None,
        on_shutdown=cleanup if phase == "startup" else [hang, cleanup],
        hook_timeout=0.01,
    )
    messages = await exchange(app)
    assert events == ["cancelled", "cleaned"]
    assert messages[-1]["type"] == f"lifespan.{phase}.failed"
    assert "TimeoutError" in messages[-1]["message"]
    assert len(messages) == (1 if phase == "startup" else 2)


@pytest.mark.parametrize("phase", ["startup", "running", "shutdown"])
async def test_external_cancellation_cleans_up_then_propagates(phase):
    incoming, outgoing = asyncio.Queue(), asyncio.Queue()
    entered = asyncio.Event()
    events = []
    incoming.put_nowait({"type": "lifespan.startup"})
    if phase == "shutdown":
        incoming.put_nowait({"type": "lifespan.shutdown"})

    async def startup():
        if phase == "startup":
            entered.set()
            await asyncio.Event().wait()

    async def first_cleanup():
        events.append("first")
        if phase == "shutdown":
            entered.set()
            await asyncio.Event().wait()

    async def last_cleanup():
        events.append("last")

    app = lifespan_application(
        App(), on_start=startup, on_shutdown=[first_cleanup, last_cleanup]
    )
    async with asyncio.timeout(2), asyncio.TaskGroup() as tasks:
        task = tasks.create_task(app({"type": "lifespan"}, incoming.get, outgoing.put))
        if phase == "running":
            assert await outgoing.get() == {"type": "lifespan.startup.complete"}
        else:
            await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert events == ["first", "last"]
    remaining = []
    while not outgoing.empty():
        remaining.append(outgoing.get_nowait())
    assert remaining == (
        [{"type": "lifespan.startup.complete"}] if phase == "shutdown" else []
    )


@pytest.mark.parametrize(
    "broken_at", ["startup_ack", "shutdown_receive", "shutdown_ack"]
)
async def test_broken_connection_cleans_up_exactly_once(broken_at):
    events = []
    received = 0
    failure = OSError("connection lost")

    async def cleanup():
        events.append("closed")

    async def receive():
        nonlocal received
        received += 1
        if received == 1:
            return {"type": "lifespan.startup"}
        if broken_at == "shutdown_receive":
            raise failure
        return {"type": "lifespan.shutdown"}

    async def send(message):
        if (
            broken_at == "startup_ack"
            and message["type"] == "lifespan.startup.complete"
        ) or (
            broken_at == "shutdown_ack"
            and message["type"] == "lifespan.shutdown.complete"
        ):
            raise failure

    with pytest.raises(OSError) as caught:
        await lifespan_application(App(), on_shutdown=cleanup)(
            {"type": "lifespan"}, receive, send
        )
    assert caught.value is failure
    assert events == ["closed"]


async def test_shutdown_attempts_remaining_hooks_and_reports_all_errors():
    events = []

    async def first():
        events.append("first")
        raise ValueError("first cleanup failed")

    async def second():
        events.append("second")
        raise RuntimeError("second cleanup failed")

    async def last():
        events.append("last")

    messages = await exchange(
        lifespan_application(App(), on_shutdown=[first, second, last])
    )
    assert events == ["first", "second", "last"]
    assert messages[-1]["type"] == "lifespan.shutdown.failed"
    assert "first cleanup failed" in messages[-1]["message"]
    assert "second cleanup failed" in messages[-1]["message"]


async def test_partial_startup_rolls_back_acquired_resources_and_reports_both_errors():
    resources = []

    async def acquire():
        resources.append("connection")

    async def fail():
        raise RuntimeError("startup failed")

    async def must_not_run():
        resources.append("unexpected")

    async def close():
        resources.clear()
        raise ValueError("rollback failed after closing")

    messages = await exchange(
        lifespan_application(
            App(), on_start=[acquire, fail, must_not_run], on_shutdown=close
        )
    )
    assert resources == []
    assert len(messages) == 1
    assert messages[0]["type"] == "lifespan.startup.failed"
    assert "startup failed" in messages[0]["message"]
    assert "rollback failed after closing" in messages[0]["message"]
