"""In-process request overhead; excludes network, server, and concurrency costs."""

import argparse
import asyncio
import statistics
import time

from wred import App

from wred_lifespan import lifespan_application


async def main(requests, rounds):
    api = App()

    @api.route("/")
    async def hello(request):
        return {"hello": "world"}

    scope = {"type": "http", "method": "GET", "path": "/"}

    async def receive():
        return {"type": "http.request", "body": b""}

    async def send(message):
        pass

    wrapped = lifespan_application(api)
    samples = {"wred": [], "wrapped": []}
    for turn in range(rounds + 1):
        order = [("wred", api), ("wrapped", wrapped)]
        if turn % 2:
            order.reverse()
        for name, app in order:
            start = time.perf_counter()
            for _ in range(requests):
                await app(scope, receive, send)
            elapsed = time.perf_counter() - start
            if turn:  # Discard warmup; alternate order to reduce timing bias.
                samples[name].append(elapsed / requests * 1_000_000)
    raw, wrapped_time = (statistics.median(samples[name]) for name in samples)
    print(
        f"{requests:,} requests/round; {rounds} measured rounds; median microseconds/request"
    )
    print(
        f"Wred: {raw:.3f}; wrapped: {wrapped_time:.3f}; added: {wrapped_time - raw:.3f}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=int, default=100_000)
    parser.add_argument("--rounds", type=int, default=7)
    args = parser.parse_args()
    if args.requests < 1 or args.rounds < 1:
        parser.error("requests and rounds must be positive")
    asyncio.run(main(args.requests, args.rounds))
