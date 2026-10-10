"""任务模型与状态机。

状态集为什么只有四个,见 docs/2026-09-30-status-decision.md 的取舍。
判据是:一个状态值不值得存在,看它有没有独立的"推进者"。

    QUEUED  --(worker 领取)-->  RUNNING
    RUNNING --(成功)--------->  SUCCESS
    RUNNING --(失败/超时)---->  QUEUED(重试) 或 FAILURE(死信)

你原先列的另外三个,以及它们什么时候该长出来:

- PENDING:等实现"延迟到点执行"或"依赖前置任务"时再加。那时它的推进者是
  定时器而不是 worker,它才携带信息。现在它只是 QUEUED 的同义词。
- RETRY:它是"带延迟的重新入队",推进者仍然是 worker,所以不是状态。
  "这是第几次"用 attempts 表达,展示层要区分随时能算出来。
- CANCELLED:等有取消需求再加。注意它只能从 QUEUED 取消,不能从 RUNNING
  取消 —— 任务已经在跑,你只能请求它自己退出。
"""

from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


class TaskPriority(enum.IntEnum):
    """数值越大越先出队。"""

    LOW = 10
    NORMAL = 20
    HIGH = 30
    CRITICAL = 40


class TaskStatus(str, enum.Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"


TERMINAL_STATUSES = frozenset({TaskStatus.SUCCESS, TaskStatus.FAILURE})

# 合法转移表:一条边 = 一次允许的转移。表里没有的边一律抛 InvalidTransition。
# 把规则写成数据而不是散在 if 里,是为了让"哪些转换是合法的"能一眼看完。
ALLOWED_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.QUEUED: frozenset({TaskStatus.RUNNING}),
    TaskStatus.RUNNING: frozenset(
        {TaskStatus.QUEUED, TaskStatus.SUCCESS, TaskStatus.FAILURE}
    ),
    TaskStatus.SUCCESS: frozenset(),
    TaskStatus.FAILURE: frozenset(),
}


class InvalidTransition(RuntimeError):
    """试图做一次状态机不允许的转移。"""


@dataclass
class Task:
    name: str
    payload: dict[str, Any] = field(default_factory=dict)
    priority: TaskPriority = TaskPriority.NORMAL
    max_retries: int = 2  # 失败后还能重试几次;总执行次数 = 1 + max_retries
    timeout: float = 5.0  # 单次执行超时(秒);0 表示不限
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    status: TaskStatus = TaskStatus.QUEUED
    attempts: int = 0  # 被"投递"出去几次;在 dequeue 时 +1,和最终成败无关
    result: Any = None
    error: str | None = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    def transition_to(self, new_status: TaskStatus) -> Task:
        if new_status not in ALLOWED_TRANSITIONS[self.status]:
            raise InvalidTransition(
                f"{self.id[:8]}: {self.status.value} -> {new_status.value} 不是合法转移"
            )
        self.status = new_status
        self.updated_at = time.time()
        return self

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    def __repr__(self) -> str:  # 让日志和断言失败时看得懂
        return (
            f"<Task {self.id[:8]} {self.name} {self.status.value} "
            f"attempts={self.attempts}>"
        )
