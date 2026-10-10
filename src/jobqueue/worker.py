"""Worker:并发槽 + 执行超时 + 失败重试。

Worker 的职责边界刻意划得很窄:它只负责"领取 -> 执行 -> 报告结果"。
"失败了该重试还是进死信"由 broker 判定,因为只有 broker 知道任务是不是
还在别人手上,也只有它能保证判定和重新入队之间不被打断。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

from .broker import InMemoryBroker
from .models import Task

logger = logging.getLogger(__name__)

Handler = Callable[[Task], Awaitable[Any]]


class Worker:
    def __init__(
        self,
        broker: InMemoryBroker,
        concurrency: int = 2,
        poll_interval: float = 0.02,
        retry_base_delay: float = 0.05,
    ) -> None:
        self.broker = broker
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        self.retry_base_delay = retry_base_delay
        self._handlers: dict[str, Handler] = {}
        self._slots: list[asyncio.Task[None]] = []
        self._stopping = False

    def task(self, name: str):
        """注册 handler 的装饰器:@worker.task("add")"""

        def decorator(fn: Handler) -> Handler:
            self._handlers[name] = fn
            return fn

        return decorator

    async def start(self) -> None:
        """起 N 个并发槽。每个槽是一个独立的循环,共享同一个 broker。"""
        self._stopping = False
        self._slots = [
            asyncio.create_task(self._slot(i), name=f"worker-slot-{i}")
            for i in range(self.concurrency)
        ]

    async def stop(self) -> None:
        """停止领取新任务,等在手的任务跑完(不打断正在执行的 handler)。

        注意:如果进程在这里被强杀,在手的任务不会被 ack,它们会留在
        broker 的 inflight 里,等可见性超时后被重新投递。这就是设计意图。
        """
        self._stopping = True
        if self._slots:
            await asyncio.gather(*self._slots, return_exceptions=True)
            self._slots = []

    async def _slot(self, index: int) -> None:
        while not self._stopping:
            task = await self.broker.dequeue()
            if task is None:
                await asyncio.sleep(self.poll_interval)
                continue
            await self._execute(task)

    async def _execute(self, task: Task) -> None:
        handler = self._handlers.get(task.name)
        if handler is None:
            await self.broker.fail(task.id, f"没有注册名为 {task.name!r} 的 handler")
            return

        try:
            if task.timeout:
                result = await asyncio.wait_for(handler(task), task.timeout)
            else:
                result = await handler(task)
        except Exception as exc:  # TimeoutError 也走这里
            # 指数退避:第 1 次失败等 base,第 2 次等 2*base ...
            delay = self.retry_base_delay * (2 ** (task.attempts - 1))
            logger.info("任务 %s 第 %d 次失败: %s", task.id[:8], task.attempts, exc)
            await self.broker.fail(task.id, f"{type(exc).__name__}: {exc}", delay)
        else:
            await self.broker.ack(task.id, result)
