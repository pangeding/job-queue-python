"""极简分布式任务队列 MVP。

支持的语义:
- 优先级出队(四档)
- 单次执行超时
- 失败指数退避重试,额度用尽进死信队列
- 可见性超时:worker 崩溃后任务被自动重新投递(至少一次投递)

明确不包含(等你需要时再加,见 docs/):
- Redis 持久化、跨进程/跨机器消费
- 延迟任务 / 定时调度 / cron
- 幂等去重、取消、指标、HTTP 控制面
"""

from .broker import InMemoryBroker
from .models import (
    ALLOWED_TRANSITIONS,
    InvalidTransition,
    Task,
    TaskPriority,
    TaskStatus,
)
from .worker import Worker

__all__ = [
    "ALLOWED_TRANSITIONS",
    "InMemoryBroker",
    "InvalidTransition",
    "Task",
    "TaskPriority",
    "TaskStatus",
    "Worker",
]
