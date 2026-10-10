"""进程内 broker:优先级堆 + 可见性超时 + 至少一次投递。

三个必须先理解的点:

1. 这些方法都是 async,但体内一个 await 都没有 —— 这是故意的。
   接口从第一天就按"未来的 RedisBroker 也必须满足"的形状定下来。
   进程内版本不需要 await,但换实现时调用方不应该改。

2. 正因为没有 await,每个方法在单线程事件循环里就是原子的。
   这是 InMemoryBroker 能站得住的**全部**理由:从堆里 pop 一个任务和写
   inflight 记录之间不会被打断。换成 Redis 后这个前提消失,你必须用 Lua
   脚本或 WATCH/MULTI 把原子性买回来 —— 这就是"分布式"三个字的成本所在。

3. 超时回收是惰性的:没有后台线程扫,而是在每次 dequeue 时顺手回收。
   好处是不引入额外并发;代价是"没有 worker 在 dequeue 时,崩溃的任务
   不会被回收"。这个代价在真实系统里同样存在,不是这里的偷懒。
"""

from __future__ import annotations

import heapq
import itertools
import time

from .models import Task, TaskStatus


class InMemoryBroker:
    def __init__(self, visibility_timeout: float = 5.0) -> None:
        self.visibility_timeout = visibility_timeout
        self._ready: list[tuple[int, int, Task]] = []  # (-priority, seq, task)
        self._delayed: list[tuple[float, int, Task]] = []  # (ready_at, seq, task)
        self._inflight: dict[str, float] = {}  # task_id -> 可见性截止(monotonic)
        self._tasks: dict[str, Task] = {}  # 见过的所有任务
        self._dead_letters: list[Task] = []
        self._seq = itertools.count()

    # ---------------- 生产者侧 ----------------

    async def enqueue(self, task: Task, delay: float = 0.0) -> Task:
        """投递任务。delay > 0 表示多少秒之后才可被领取(重试退避用)。"""
        if task.id in self._tasks:
            raise ValueError(f"任务 {task.id[:8]} 已经在 broker 里了")
        self._tasks[task.id] = task
        self._schedule(task, delay)
        return task

    # ---------------- 消费者侧 ----------------

    async def dequeue(self) -> Task | None:
        """领取一个任务并标记为 RUNNING;没有就绪任务时返回 None。

        领取即"投递一次":attempts 在这里 +1,和任务最终成败无关。
        这样崩溃重投也会消耗重试额度,否则一个每次都能把 worker 搞崩的任务
        会被无限重投。
        """
        self._reclaim()
        while self._ready:
            _, _, task = heapq.heappop(self._ready)
            if task.status is not TaskStatus.QUEUED:
                continue  # 陈旧条目,防御性跳过
            task.attempts += 1
            task.transition_to(TaskStatus.RUNNING)
            self._inflight[task.id] = time.monotonic() + self.visibility_timeout
            return task
        return None

    async def ack(self, task_id: str, result: object = None) -> None:
        """执行成功。"""
        task = self._require(task_id)
        self._inflight.pop(task_id, None)
        task.result = result
        task.transition_to(TaskStatus.SUCCESS)

    async def fail(
        self, task_id: str, error: str, retry_delay: float = 0.0
    ) -> None:
        """执行失败。还有重试额度就带延迟重新入队,否则进死信。

        "判断额度"和"重新入队"必须在同一段无 await 的代码里完成,
        否则两个 worker 可能同时判定"这是最后一次重试"。
        """
        task = self._require(task_id)
        self._inflight.pop(task_id, None)
        task.error = error
        if task.attempts > task.max_retries:
            task.transition_to(TaskStatus.FAILURE)
            self._dead_letters.append(task)
        else:
            task.transition_to(TaskStatus.QUEUED)
            self._schedule(task, retry_delay)

    # ---------------- 查询 ----------------

    async def get(self, task_id: str) -> Task | None:
        return self._tasks.get(task_id)

    @property
    def dead_letters(self) -> list[Task]:
        return list(self._dead_letters)

    def stats(self) -> dict[str, int]:
        self._reclaim()
        counts = {s.value: 0 for s in TaskStatus}
        for task in self._tasks.values():
            counts[task.status.value] += 1
        return {
            "ready": len(self._ready),
            "delayed": len(self._delayed),
            "inflight": len(self._inflight),
            "dead_letters": len(self._dead_letters),
            **counts,
        }

    # ---------------- 内部 ----------------

    def _schedule(self, task: Task, delay: float) -> None:
        seq = next(self._seq)
        if delay > 0:
            heapq.heappush(self._delayed, (time.monotonic() + delay, seq, task))
        else:
            heapq.heappush(self._ready, (-int(task.priority), seq, task))

    def _reclaim(self, now: float | None = None) -> None:
        """把到期的延迟任务搬进就绪堆;把可见性超时的任务重新投递。

        第二件事就是"至少一次投递"的来源:worker 崩了没 ack,任务在这里被
        放回队列。因为 attempts 在 dequeue 时已经计过数,它不会无限循环,
        最终会落到死信里 —— 你随时能查到"哪些任务把 worker 搞死了"。
        """
        now = time.monotonic() if now is None else now

        if self._delayed:
            due = [item for item in self._delayed if item[0] <= now]
            if due:
                self._delayed = [item for item in self._delayed if item[0] > now]
                for _, seq, task in due:
                    if task.status is TaskStatus.QUEUED:
                        heapq.heappush(
                            self._ready, (-int(task.priority), seq, task)
                        )

        if self._inflight:
            expired = [tid for tid, dl in self._inflight.items() if dl <= now]
            for tid in expired:
                del self._inflight[tid]
                task = self._tasks[tid]
                if task.status is not TaskStatus.RUNNING:
                    continue
                task.error = f"可见性超时({self.visibility_timeout}s 内没有 ack)"
                if task.attempts > task.max_retries:
                    task.transition_to(TaskStatus.FAILURE)
                    self._dead_letters.append(task)
                else:
                    task.transition_to(TaskStatus.QUEUED)
                    heapq.heappush(
                        self._ready, (-int(task.priority), next(self._seq), task)
                    )

    def _require(self, task_id: str) -> Task:
        try:
            return self._tasks[task_id]
        except KeyError:
            raise KeyError(f"未知任务 {task_id}") from None
