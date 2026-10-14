"""Doris数据库管理能力"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from .settings import DorisConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class LoadMetrics:
    request_count: int = 0
    row_count: int = 0
    parquet_bytes: int = 0
    encode_seconds: float = 0.0
    request_seconds: float = 0.0


class DorisStreamLoader:
    """通过严格模式Stream Load批量写入Doris"""

    def __init__(self, config: DorisConfig, batch_id: str) -> None:
        self._config = config
        self._batch_id = re.sub(r"[^A-Za-z0-9_-]", "_", batch_id)
        self._sequences: defaultdict[str, int] = defaultdict(int)
        self._client = httpx.Client(timeout=600, trust_env=False)
        self._lock = threading.Lock()
        self._metrics = LoadMetrics()

    @staticmethod
    def _identifier(value: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError(f"Doris标识符无效: {value}")
        return value

    @classmethod
    def _parquet_bytes(cls, rows: Sequence[Mapping[str, Any]]) -> bytes:
        normalized = [
            {
                key: cls._normalize_value(value)
                for key, value in row.items()
            }
            for row in rows
        ]
        table = pa.Table.from_pylist(normalized)
        sink = pa.BufferOutputStream()
        pq.write_table(
            table,
            sink,
            compression="snappy",
            use_dictionary=True,
            write_statistics=False,
        )
        return sink.getvalue().to_pybytes()

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        if isinstance(value, (dict, list)):
            return json.dumps(
                value,
                ensure_ascii=False,
                default=str,
                separators=(",", ":"),
            )
        return value

    def load(self, table_name: str, rows: Sequence[Mapping[str, Any]]) -> None:
        if not rows:
            return
        database = self._identifier(self._config.database)
        table = self._identifier(table_name)
        with self._lock:
            self._sequences[table] += 1
            sequence = self._sequences[table]
        label = f"{self._batch_id}_{table}_{sequence}"
        encode_started = time.perf_counter()
        body = self._parquet_bytes(rows)
        encode_seconds = time.perf_counter() - encode_started
        headers = {
            "Expect": "100-continue",
            "format": "parquet",
            "strict_mode": "true",
            "max_filter_ratio": "0",
            "timezone": "Asia/Shanghai",
            "label": label,
        }
        endpoint = (
            f"http://{self._config.host}:{self._config.http_port}"
            f"/api/{database}/{table}/_stream_load"
        )
        request_started = time.perf_counter()
        response = self._client.put(
            endpoint,
            auth=(self._config.user, self._config.password),
            headers=headers,
            content=body,
        )
        if response.is_redirect:
            location = response.headers.get("location")
            if not location:
                raise RuntimeError(f"Doris Stream Load重定向缺少地址: {table}")
            redirect_url = httpx.URL(location).copy_with(
                username=None,
                password=None,
            )
            response = self._client.put(
                redirect_url,
                auth=(self._config.user, self._config.password),
                headers=headers,
                content=body,
            )
        request_seconds = time.perf_counter() - request_started
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"Doris Stream Load响应不是对象: {table}")
        status = str(payload.get("Status", ""))
        total = int(payload.get("NumberTotalRows", 0))
        loaded = int(payload.get("NumberLoadedRows", 0))
        filtered = int(payload.get("NumberFilteredRows", 0))
        if status != "Success" or total != len(rows) or loaded != len(rows) or filtered:
            raise RuntimeError(
                "Doris Stream Load失败 "
                f"table={table} status={status} total={total} "
                f"loaded={loaded} filtered={filtered} "
                f"message={payload.get('Message')}"
            )
        with self._lock:
            metrics = self._metrics
            self._metrics = LoadMetrics(
                request_count=metrics.request_count + 1,
                row_count=metrics.row_count + len(rows),
                parquet_bytes=metrics.parquet_bytes + len(body),
                encode_seconds=metrics.encode_seconds + encode_seconds,
                request_seconds=metrics.request_seconds + request_seconds,
            )

    def take_metrics(self) -> LoadMetrics:
        with self._lock:
            metrics = self._metrics
            self._metrics = LoadMetrics()
        return metrics

    def close(self) -> None:
        self._client.close()
