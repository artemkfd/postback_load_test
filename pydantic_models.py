from typing import Literal
from pydantic import BaseModel, ConfigDict, Field
from datetime import UTC, datetime


class Config(BaseModel):
    target_url: str
    db_name: str
    request_count: int
    source_ids: list
    postback_types: list
    parallel_threads_count: int
    mmp: list
    event_names: list
    max_duration_minutes: int | None = None
    max_requests_per_second: int | None = None
    delay: int | None = None


class Postback(BaseModel):
    test_id: str | None = None
    request_id: str
    model_config = ConfigDict(extra="allow")
    postback_type: Literal["install", "event"] | None = None
    event_name: str | None = None
    source_id: str | None = None
    campaign_id: str | None = None
    placement_id: str | None = None  # tagId in xiaomi
    adset_id: str | None = None  # group_id xiaomi
    ad_id: str | None = None  #  creative_id in xiaomi
    advertising_id: str | None = None  # gaid in xiaomi
    country: str | None = None  # region in xiaomi
    click_id: str | None = None
    mmp: Literal["appmetrica", "appsflyer"] | None = None
    gaid: str | None = None
    idfa: str | None = None


class Metrics(BaseModel):
    test_datetime: datetime = Field(default_factory=lambda: datetime.now(UTC))
    test_id: str | None
    duration: int | None
    sending_count: int | None
    received_count: int | None
    success_rate: int | None
    average_latency: int | None
    data_loss: int | None
