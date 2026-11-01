"""Bounded startup and best-effort cleanup for asyncio ASGI applications."""

import asyncio
import logging
import math
from collections.abc import Awaitable, Callable, Iterable
from inspect import iscoroutinefunction
from traceback import format_exception
from typing import Any, TypeAlias

__version__ = "0.1.1"
__all__ = ["lifespan_application"]

Scope: TypeAlias = dict[str, Any]
Message: TypeAlias = dict[str, Any]
Receive: TypeAlias = Callable[[], Awaitable[Message]]
Send: TypeAlias = Callable[[Message], Awaitable[None]]
ASGIApplication: TypeAlias = Callable[[Scope, Receive, Send], Awaitable[None]]
LifespanHandler: TypeAlias = Callable[[], Awaitable[None]]
Handlers: TypeAlias = LifespanHandler | Iterable[LifespanHandler] | None
_logger = logging.getLogger(__name__)


def _handlers(value: Handlers) -> tuple[LifespanHandler, ...]:
    if value is None:
        return ()
    handlers = (value,) if callable(value) else tuple(value)
    for handler in handlers:
        if not (
            iscoroutinefunction(handler)
            or iscoroutinefunction(getattr(handler, "__call__", None))  # noqa: B004 - async callable objects.
        ):
            raise TypeError(
                "Lifespan hooks must be async functions taking no arguments"
            )
    return handlers


async def _run(handler: LifespanHandler, seconds: float) -> None:
    async with asyncio.timeout(seconds) as deadline:
        await handler()
    # A hook that catches cancellation and returns still missed its deadline.
    if deadline.expired():
        raise TimeoutError("Lifespan hook exceeded its timeout")


async def _cleanup(
    handlers: tuple[LifespanHandler, ...], seconds: float
) -> list[BaseException]:
    errors: list[BaseException] = []
    for handler in handlers:
        try:
            await _run(handler, seconds)
        except (Exception, asyncio.CancelledError) as exc:  # noqa: BLE001 - attempt every cleanup.
            # Each cleanup gets its own deadline; one failure cannot skip the rest.
            errors.append(exc)
    return errors


def _log_errors(errors: list[BaseException]) -> None:
    for error in errors:
        if not isinstance(error, asyncio.CancelledError):
            _logger.error(
                "Lifespan cleanup failed",
                exc_info=(type(error), error, error.__traceback__),
            )


def _result(phase: str, errors: list[BaseException]) -> Message:
    for error in errors:
        if isinstance(error, asyncio.CancelledError):
            _log_errors(errors)
            raise error
    if errors:
        failure = BaseExceptionGroup(f"Lifespan {phase} failed", errors)
        return {
            "type": f"lifespan.{phase}.failed",
            "message": "".join(format_exception(failure)),
        }
    return {"type": f"lifespan.{phase}.complete"}


async def _receive(receive: Receive, phase: str) -> None:
    message = await receive()
    if message["type"] != f"lifespan.{phase}":
        raise RuntimeError(f"Expected lifespan.{phase}, received {message['type']}")


def lifespan_application(
    app: ASGIApplication,
    *,
    on_start: Handlers = None,
    on_shutdown: Handlers = None,
    hook_timeout: float = 30.0,
) -> ASGIApplication:
    """Wrap an ASGI app with async startup and shutdown hooks.

    Hooks run in supplied order, each with its own positive, finite timeout in
    seconds. Shutdown hooks also run after partial startup failure, cancellation,
    or a broken lifespan connection; they must tolerate uninitialized resources.
    All cleanup hooks are attempted even when another fails or times out.

    Timeouts require cooperative asyncio code. Blocking code or suppressed
    cancellation cannot be forcibly stopped. This wrapper owns lifespan scopes;
    all other scopes are forwarded unchanged to app.
    """
    if not callable(app):
        raise TypeError("app must be an ASGI callable")
    if (
        isinstance(hook_timeout, bool)
        or not isinstance(hook_timeout, (int, float))
        or not math.isfinite(hook_timeout)
        or hook_timeout <= 0
    ):
        raise ValueError("hook_timeout must be a positive finite number of seconds")
    startup, shutdown = _handlers(on_start), _handlers(on_shutdown)

    async def application(scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "lifespan":
            await app(scope, receive, send)
            return

        await _receive(receive, "startup")
        cleanup_started = False
        try:
            try:
                for handler in startup:
                    await _run(handler, hook_timeout)
            except Exception as exc:  # noqa: BLE001 - report failures through ASGI.
                cleanup_started = True
                errors = [exc, *await _cleanup(shutdown, hook_timeout)]
                await send(_result("startup", errors))
                return
            await send({"type": "lifespan.startup.complete"})
            await _receive(receive, "shutdown")
            cleanup_started = True
            errors = await _cleanup(shutdown, hook_timeout)
            await send(_result("shutdown", errors))
        finally:
            # Transport errors, protocol errors, and cancellation still release
            # resources. Preserve the original exception and log cleanup errors.
            if not cleanup_started:
                _log_errors(await _cleanup(shutdown, hook_timeout))

    return application
