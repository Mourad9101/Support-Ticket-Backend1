"""
Task 10: Rate Limiter service.

Client-side rate limiter to enforce the external API's rate limit.
"""

import asyncio
import time


class RateLimiter:
    """
    Token bucket rate limiter.
    """

    def __init__(self, requests_per_minute: int = 60):
        self.max_tokens = max(1, requests_per_minute)
        self.refill_rate = self.max_tokens / 60.0  # tokens per second
        self.tokens = float(self.max_tokens)
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> float:
        """
        Acquire a token, waiting if necessary.

        Returns:
            wait_time: seconds waited before acquiring the token.
        """
        total_wait = 0.0
        while True:
            async with self._lock:
                now = time.time()
                elapsed = now - self.last_refill
                if elapsed > 0:
                    self.tokens = min(self.max_tokens, self.tokens + elapsed * self.refill_rate)
                    self.last_refill = now

                if self.tokens >= 1:
                    self.tokens -= 1
                    return total_wait

                wait_time = (1 - self.tokens) / self.refill_rate if self.refill_rate > 0 else 1.0

            await asyncio.sleep(wait_time)
            total_wait += wait_time

    def get_status(self) -> dict:
        """
        Return limiter status for tests/inspection.
        """
        return {
            "current_requests": int(max(0, min(self.max_tokens, round(self.max_tokens - self.tokens)))),
            "remaining": int(max(0, min(self.max_tokens, round(self.tokens))))
        }


# Global limiter shared across all tenants
_global_rate_limiter = RateLimiter(requests_per_minute=60)


async def global_rate_limiter_acquire() -> float:
    return await _global_rate_limiter.acquire()


global_rate_limiter = _global_rate_limiter
