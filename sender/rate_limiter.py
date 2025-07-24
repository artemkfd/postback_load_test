import asyncio
import time


class PreciseRateLimiter:
    """Provides precise rate limiting for async operations"""

    def __init__(self, rps: int):
        self.interval = 1.0 / rps if rps else 0
        self.last_request = time.perf_counter()
        self.lock = asyncio.Lock()

    async def wait(self):
        async with self.lock:
            elapsed = time.perf_counter() - self.last_request
            wait_time = max(0, self.interval - elapsed)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            self.last_request = time.perf_counter()
