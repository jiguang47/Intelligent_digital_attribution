"""Celery 同步任务中的异步运行辅助。"""

import asyncio
import sys
from collections.abc import Coroutine
from typing import Any


def run_async[T](coroutine: Coroutine[Any, Any, T]) -> T:
    """为单个 Celery 任务运行异步领域逻辑。"""
    if sys.platform == "win32":
        # Windows 默认 ProactorEventLoop 不被 psycopg 异步模式支持，显式指定 Selector。
        return asyncio.run(coroutine, loop_factory=asyncio.SelectorEventLoop)
    return asyncio.run(coroutine)
