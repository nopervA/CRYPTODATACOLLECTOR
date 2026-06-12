from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Sequence
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)


class BinanceRestClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        base_url: str,
        max_attempts: int,
        concurrency: int,
        runtime_metrics: Any | None = None,
    ) -> None:
        self._session = session
        self._base_url = base_url
        self._max_attempts = max_attempts
        self._semaphore = asyncio.Semaphore(concurrency)
        self._runtime_metrics = runtime_metrics

    async def get_json(
        self, path: str, params: dict[str, str] | None = None
    ) -> Any:
        url = f"{self._base_url}{path}"
        delay = 1.0
        last_error: Exception | None = None

        for attempt in range(1, self._max_attempts + 1):
            try:
                async with self._semaphore:
                    async with self._session.get(url, params=params) as response:
                        if response.status == 200:
                            return await response.json(content_type=None)

                        body = (await response.text())[:500]
                        if response.status not in {418, 429, 500, 502, 503, 504}:
                            raise RuntimeError(
                                f"Binance REST {response.status}: {body}"
                            )

                        retry_after = response.headers.get("Retry-After")
                        if retry_after:
                            delay = max(delay, float(retry_after))
                        last_error = RuntimeError(
                            f"Binance REST {response.status}: {body}"
                        )
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                last_error = exc

            if attempt == self._max_attempts:
                break

            sleep_seconds = min(delay, 60.0) + random.uniform(0.0, 0.5)
            logger.warning(
                "REST request failed for %s (attempt %d): %s; retrying in %.2fs",
                url,
                attempt,
                last_error,
                sleep_seconds,
            )
            await asyncio.sleep(sleep_seconds)
            delay = min(delay * 2.0, 60.0)

        if self._runtime_metrics is not None:
            self._runtime_metrics.record_rest_failure()
        raise RuntimeError(
            f"Binance REST request failed after {self._max_attempts} attempts: "
            f"{url}: {last_error}"
        )

    async def validate_symbols(self, symbols: Sequence[str]) -> None:
        payload = await self.get_json("/fapi/v1/exchangeInfo")
        available = {
            item["symbol"]
            for item in payload.get("symbols", [])
            if item.get("contractType") == "PERPETUAL"
            and item.get("quoteAsset") == "USDT"
            and item.get("status") == "TRADING"
        }
        invalid = sorted(set(symbols) - available)
        if invalid:
            raise ValueError(
                "Symbols are not active Binance USDT-M perpetuals: "
                + ", ".join(invalid)
            )
