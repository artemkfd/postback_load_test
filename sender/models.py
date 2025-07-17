from dataclasses import dataclass
from typing import Any
from pydantic import BaseModel


class TestConfig(BaseModel):
    request_count: int
    parallel_threads_count: int
    max_requests_per_second: int
    max_duration_minutes: int
    target_url: str
    postback_types: list[str]
    event_names: list[str]
    source_ids: list[str]
    mmp: list[str]
    db_name: str
    max_connections: int | None = 100
    http_timeout: float | None = 30.0
    http_retries: int | None = 3


@dataclass
class TestResult:
    test_id: str
    duration: float
    stats: dict[str, Any]
    metrics: dict[str, float]
    verification: dict[str, Any]


@dataclass
class PostbackParams:
    request_id: str
    test_id: str
    postback_type: str
    event_name: str
    source_id: str
    campaign_id: str
    placement_id: str
    adset_id: str
    ad_id: str
    advertising_id: str
    country: str
    click_id: str
    mmp: str


@dataclass
class TestStats:
    verified_success: int
    unverified_success: int
    failed: int
    latencies: list[float]
    sent_count: int


@dataclass
class TestMetrics:
    avg_latency: float
    min_latency: float
    max_latency: float
    p90: float
    p95: float
    p99: float
    verified_rate: float
    rps: float


@dataclass
class VerificationResult:
    total_sent: int
    total_received: int
    missing_count: int
    field_mismatches: dict[str, int]
