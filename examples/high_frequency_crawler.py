"""High-frequency crawling with CloakBrowser persistent context.

Runs repeated crawl jobs multiple times per minute using:
- one long-lived persistent browser profile
- fixed-interval scheduler
- target queue with overlap protection
- timeout + retry with exponential backoff
- simple circuit breaker per target
- incremental state store (write only when changed)
- basic observability and adaptive slowdown when blocked/error rate rises

Usage:
    pip install cloakbrowser
    python examples/high_frequency_crawler.py

Environment variables:
    TARGETS="https://example.com,https://example.org"
    INTERVAL_SECONDS=20
    BATCH_SIZE=2
    MAX_CONCURRENCY=2
    JOB_TIMEOUT_SECONDS=25
    MAX_RETRIES=2
    CIRCUIT_BREAKER_THRESHOLD=3
    CIRCUIT_BREAKER_SECONDS=120
    MAX_BACKOFF_SECONDS=10
    BACKOFF_JITTER_SECONDS=0.5
    RATE_FAILURE_THRESHOLD=0.35
    RATE_BLOCKED_THRESHOLD=0.20
    RATE_SLOWDOWN_INCREMENT_SECONDS=5
    RATE_RECOVERY_DECREMENT_SECONDS=1
    RATE_MAX_MULTIPLIER=3
    STATE_FLUSH_INTERVAL_SECONDS=5
    QUEUE_SCAN_FACTOR=3
    RUN_SECONDS=300
    PROFILE_DIR=./hf-profile
    STATE_FILE=./hf-state.json
    OUTPUT_FILE=./hf-results.ndjson

Note:
    Validate your crawl cadence against target site terms/robots and local laws.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cloakbrowser import launch_persistent_context_async


class ChallengeDetectedError(RuntimeError):
    """Raised when a challenge/captcha/block page is detected."""


@dataclass
class Metrics:
    runs: int = 0
    success: int = 0
    failed: int = 0
    blocked: int = 0
    total_duration: float = 0.0

    @property
    def success_rate(self) -> float:
        return self.success / self.runs if self.runs else 0.0

    @property
    def blocked_rate(self) -> float:
        return self.blocked / self.runs if self.runs else 0.0

    @property
    def failure_rate(self) -> float:
        return self.failed / self.runs if self.runs else 0.0

    @property
    def avg_duration(self) -> float:
        return self.total_duration / self.runs if self.runs else 0.0


class HighFrequencyCrawler:
    def __init__(
        self,
        targets: list[str],
        profile_dir: Path,
        state_file: Path,
        output_file: Path,
        interval_seconds: float = 20.0,
        batch_size: int = 2,
        max_concurrency: int = 2,
        job_timeout_seconds: float = 25.0,
        max_retries: int = 2,
        circuit_breaker_threshold: int = 3,
        circuit_breaker_seconds: float = 120.0,
        max_backoff_seconds: float = 10.0,
        backoff_jitter_seconds: float = 0.5,
        rate_failure_threshold: float = 0.35,
        rate_blocked_threshold: float = 0.20,
        rate_slowdown_increment_seconds: float = 5.0,
        rate_recovery_decrement_seconds: float = 1.0,
        rate_max_multiplier: float = 3.0,
        state_flush_interval_seconds: float = 5.0,
        queue_scan_factor: int = 3,
        run_seconds: float = 300.0,
    ) -> None:
        if not targets:
            raise ValueError("At least one target URL is required")

        self.targets = targets
        self.queue = deque(targets)
        self.profile_dir = profile_dir
        self.state_file = state_file
        self.output_file = output_file
        self.base_interval = max(1.0, interval_seconds)
        self.current_interval = self.base_interval
        self.batch_size = max(1, batch_size)
        self.max_concurrency = max(1, max_concurrency)
        self.job_timeout_seconds = max(1.0, job_timeout_seconds)
        self.max_retries = max(0, max_retries)
        self.circuit_breaker_threshold = max(1, circuit_breaker_threshold)
        self.circuit_breaker_seconds = max(5.0, circuit_breaker_seconds)
        self.max_backoff_seconds = max(1.0, max_backoff_seconds)
        self.backoff_jitter_seconds = max(0.0, backoff_jitter_seconds)
        self.rate_failure_threshold = min(max(rate_failure_threshold, 0.0), 1.0)
        self.rate_blocked_threshold = min(max(rate_blocked_threshold, 0.0), 1.0)
        self.rate_slowdown_increment_seconds = max(0.1, rate_slowdown_increment_seconds)
        self.rate_recovery_decrement_seconds = max(0.1, rate_recovery_decrement_seconds)
        self.rate_max_multiplier = max(1.0, rate_max_multiplier)
        self.state_flush_interval_seconds = max(1.0, state_flush_interval_seconds)
        self.queue_scan_factor = max(1, queue_scan_factor)
        self.run_seconds = max(5.0, run_seconds)

        self.semaphore: asyncio.Semaphore | None = None
        self.in_flight: set[str] = set()
        self.failures_by_target: dict[str, int] = {t: 0 for t in targets}
        self.circuit_until: dict[str, float] = {t: 0.0 for t in targets}
        self.state = self._load_state()
        self.metrics = Metrics()
        self.recent_outcomes: deque[bool] = deque(maxlen=30)
        self.recent_blocked: deque[bool] = deque(maxlen=30)
        self.state_lock = asyncio.Lock()
        self.state_dirty = False
        self.last_state_flush_at = 0.0
        self.context = None
        self.stop_at = time.monotonic() + self.run_seconds

    def _load_state(self) -> dict[str, Any]:
        if not self.state_file.exists():
            return {}
        try:
            data = json.loads(self.state_file.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _target_id(self, url: str) -> str:
        return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]

    async def _safe_text(self, page: Any, selectors: list[str], timeout_ms: int = 1500) -> str | None:
        """Return first non-empty selector text.

        page: Playwright page object.
        selectors: Ordered CSS selectors to try.
        timeout_ms: Timeout in milliseconds for each selector attempt.
        """
        for selector in selectors:
            try:
                loc = page.locator(selector).first
                txt = await loc.text_content(timeout=timeout_ms)
                if txt:
                    clean = " ".join(txt.split())
                    if clean:
                        return clean
            except Exception:
                continue
        return None

    async def _detect_block(self, page: Any, title: str) -> bool:
        lowered = (title or "").lower()
        title_signals = ("captcha", "just a moment", "access denied", "blocked")
        if any(sig in lowered for sig in title_signals):
            return True

        try:
            body = (await page.content()).lower()
        except Exception:
            return False

        content_signals = (
            "captcha",
            "cf-challenge",
            "cloudflare",
            "verify you are human",
            "unusual traffic",
            "access denied",
        )
        return any(sig in body for sig in content_signals)

    async def _append_output(self, payload: dict[str, Any]) -> None:
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        with self.output_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    async def _save_state_if_changed(self, key: str, payload: dict[str, Any]) -> bool:
        fingerprint_source = {
            "url": payload["url"],
            "title": payload.get("title"),
            "price": payload.get("price"),
            "stock": payload.get("stock"),
        }
        digest = hashlib.sha256(json.dumps(fingerprint_source, sort_keys=True).encode("utf-8")).hexdigest()

        async with self.state_lock:
            prev = self.state.get(key)
            if prev and prev.get("hash") == digest:
                return False

            self.state[key] = {
                "hash": digest,
                "last_seen_at": payload["timestamp"],
                "last_value": fingerprint_source,
            }
            self.state_dirty = True

        await self._append_output(payload)
        return True

    async def _flush_state(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and (now - self.last_state_flush_at) < self.state_flush_interval_seconds:
            return

        async with self.state_lock:
            if not self.state_dirty:
                self.last_state_flush_at = now
                return
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            self.state_file.write_text(json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf-8")
            self.state_dirty = False
            self.last_state_flush_at = now

    async def _crawl_once(self, url: str) -> dict[str, Any]:
        if self.context is None:
            raise RuntimeError("Browser context is not initialized")

        page = await self.context.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded")
            title = await page.title()
            blocked = await self._detect_block(page, title)
            if blocked:
                raise ChallengeDetectedError(f"Blocked/challenge detected for {url}")

            price = await self._safe_text(
                page,
                selectors=[
                    '[data-testid*="price"]',
                    '[class*="price"]',
                    '.price',
                ],
            )
            stock = await self._safe_text(
                page,
                selectors=[
                    '[data-testid*="stock"]',
                    '[class*="stock"]',
                    '.stock',
                ],
            )

            payload = {
                "id": self._target_id(url),
                "url": url,
                "title": title,
                "price": price,
                "stock": stock,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            return payload
        finally:
            await page.close()

    async def _run_target(self, url: str) -> None:
        started = time.monotonic()
        blocked = False
        ok = False
        last_error: Exception | None = None

        try:
            if self.semaphore is None:
                raise RuntimeError("Semaphore is not initialized")
            async with self.semaphore:
                for attempt in range(self.max_retries + 1):
                    try:
                        payload = await asyncio.wait_for(self._crawl_once(url), timeout=self.job_timeout_seconds)
                        changed = await self._save_state_if_changed(self._target_id(url), payload)
                        status = "CHANGED" if changed else "UNCHANGED"
                        print(f"[{status}] {url} title={payload.get('title')!r} price={payload.get('price')!r}")
                        ok = True
                        self.failures_by_target[url] = 0
                        break
                    except ChallengeDetectedError as e:
                        blocked = True
                        last_error = e
                    except Exception as e:
                        last_error = e

                    if attempt < self.max_retries:
                        backoff = min(self.max_backoff_seconds, (2 ** min(attempt, 10)))
                        backoff += random.uniform(0.0, self.backoff_jitter_seconds)
                        await asyncio.sleep(backoff)

                if not ok:
                    self.failures_by_target[url] += 1
                    if self.failures_by_target[url] >= self.circuit_breaker_threshold:
                        self.circuit_until[url] = time.monotonic() + self.circuit_breaker_seconds
                        self.failures_by_target[url] = 0
                        print(f"[CIRCUIT-OPEN] {url} for {self.circuit_breaker_seconds:.0f}s")

                    print(f"[FAILED] {url} error={last_error}")
        finally:
            duration = time.monotonic() - started
            self.metrics.runs += 1
            self.metrics.total_duration += duration
            if ok:
                self.metrics.success += 1
                self.recent_outcomes.append(True)
                self.recent_blocked.append(False)
            else:
                self.metrics.failed += 1
                self.recent_outcomes.append(False)
                self.recent_blocked.append(blocked)
                if blocked:
                    self.metrics.blocked += 1
            self.in_flight.discard(url)

    def _adjust_rate(self) -> None:
        if not self.recent_outcomes:
            return

        recent_failure_rate = 1.0 - (sum(self.recent_outcomes) / len(self.recent_outcomes))
        recent_blocked_rate = sum(self.recent_blocked) / len(self.recent_blocked)

        if recent_failure_rate > self.rate_failure_threshold or recent_blocked_rate > self.rate_blocked_threshold:
            self.current_interval = min(
                self.base_interval * self.rate_max_multiplier,
                self.current_interval + self.rate_slowdown_increment_seconds,
            )
        else:
            self.current_interval = max(
                self.base_interval,
                self.current_interval - self.rate_recovery_decrement_seconds,
            )

    def _report_metrics(self) -> None:
        effective_max_runs_per_minute = (60.0 / self.current_interval) * self.batch_size
        print(
            "[METRICS] "
            f"runs={self.metrics.runs} "
            f"success_rate={self.metrics.success_rate:.1%} "
            f"blocked_rate={self.metrics.blocked_rate:.1%} "
            f"avg_duration={self.metrics.avg_duration:.2f}s "
            f"interval={self.current_interval:.1f}s "
            f"effective_max_runs_per_min={effective_max_runs_per_minute:.2f}"
        )

    async def run(self) -> None:
        self.profile_dir.mkdir(parents=True, exist_ok=True)
        self.semaphore = asyncio.Semaphore(self.max_concurrency)
        self.context = await launch_persistent_context_async(
            user_data_dir=self.profile_dir,
            headless=True,
        )
        print(f"[START] targets={len(self.targets)} run_seconds={self.run_seconds}")

        try:
            while time.monotonic() < self.stop_at:
                tick_started = time.monotonic()
                tasks: list[asyncio.Task[Any]] = []
                scan_limit = min(len(self.queue), self.batch_size * self.queue_scan_factor)

                for _ in range(scan_limit):
                    if len(tasks) >= self.batch_size:
                        break

                    url = self.queue.popleft()
                    self.queue.append(url)

                    if url in self.in_flight:
                        continue

                    if time.monotonic() < self.circuit_until[url]:
                        continue

                    self.in_flight.add(url)
                    tasks.append(asyncio.create_task(self._run_target(url)))

                if tasks:
                    await asyncio.gather(*tasks)

                await self._flush_state()
                self._adjust_rate()
                self._report_metrics()

                elapsed = time.monotonic() - tick_started
                await asyncio.sleep(max(0.0, self.current_interval - elapsed))
        finally:
            await self._flush_state(force=True)
            if self.context is not None:
                await self.context.close()
            print("[STOP] crawler closed cleanly")


def env_float(name: str, default: float) -> float:
    val = os.getenv(name)
    return float(val) if val else default


def env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    return int(val) if val else default


def env_targets() -> list[str]:
    raw = os.getenv("TARGETS", "https://example.com")
    items = [x.strip() for x in raw.split(",")]
    return [x for x in items if x]


async def main() -> None:
    crawler = HighFrequencyCrawler(
        targets=env_targets(),
        profile_dir=Path(os.getenv("PROFILE_DIR", "./hf-profile")),
        state_file=Path(os.getenv("STATE_FILE", "./hf-state.json")),
        output_file=Path(os.getenv("OUTPUT_FILE", "./hf-results.ndjson")),
        interval_seconds=env_float("INTERVAL_SECONDS", 20.0),
        batch_size=env_int("BATCH_SIZE", 2),
        max_concurrency=env_int("MAX_CONCURRENCY", 2),
        job_timeout_seconds=env_float("JOB_TIMEOUT_SECONDS", 25.0),
        max_retries=env_int("MAX_RETRIES", 2),
        circuit_breaker_threshold=env_int("CIRCUIT_BREAKER_THRESHOLD", 3),
        circuit_breaker_seconds=env_float("CIRCUIT_BREAKER_SECONDS", 120.0),
        max_backoff_seconds=env_float("MAX_BACKOFF_SECONDS", 10.0),
        backoff_jitter_seconds=env_float("BACKOFF_JITTER_SECONDS", 0.5),
        rate_failure_threshold=env_float("RATE_FAILURE_THRESHOLD", 0.35),
        rate_blocked_threshold=env_float("RATE_BLOCKED_THRESHOLD", 0.20),
        rate_slowdown_increment_seconds=env_float("RATE_SLOWDOWN_INCREMENT_SECONDS", 5.0),
        rate_recovery_decrement_seconds=env_float("RATE_RECOVERY_DECREMENT_SECONDS", 1.0),
        rate_max_multiplier=env_float("RATE_MAX_MULTIPLIER", 3.0),
        state_flush_interval_seconds=env_float("STATE_FLUSH_INTERVAL_SECONDS", 5.0),
        queue_scan_factor=env_int("QUEUE_SCAN_FACTOR", 3),
        run_seconds=env_float("RUN_SECONDS", 300.0),
    )
    await crawler.run()


if __name__ == "__main__":
    asyncio.run(main())
