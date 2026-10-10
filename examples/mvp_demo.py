"""极简 MVP 的可运行示例。

    python3 examples/mvp_demo.py

四个场景,每个对应一条语义。看完这四个,你就知道这个 MVP 到底保证了什么:

1. 优先级出队:数值大的先走
2. 失败重试后成功(指数退避)
3. 重试额度用尽 -> 死信队列
4. worker 崩溃 -> 可见性超时 -> 任务被重新领取(至少一次投递)
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from jobqueue import InMemoryBroker, Task, TaskPriority, Worker


async def wait_until(predicate, timeout: float = 3.0) -> bool:
    """轮询等待一个条件成立。测试和 demo 都需要它,正式代码里不该有。"""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.005)
    return False


async def demo_priority() -> None:
    print("\n[1] 优先级出队")
    broker = InMemoryBroker()
    worker = Worker(broker, concurrency=1)
    order: list[str] = []

    @worker.task("record")
    async def record(task: Task) -> str:
        order.append(task.payload["label"])
        return task.payload["label"]

    # 故意按"低 -> 高"的顺序投递,看它是不是按优先级出队
    for label, prio in [
        ("low", TaskPriority.LOW),
        ("critical", TaskPriority.CRITICAL),
        ("normal", TaskPriority.NORMAL),
    ]:
        await broker.enqueue(Task(name="record", payload={"label": label}, priority=prio))

    await worker.start()
    await wait_until(lambda: len(order) == 3)
    await worker.stop()
    print(f"    投递顺序: low, critical, normal")
    print(f"    出队顺序: {', '.join(order)}")


async def demo_retry_then_success() -> None:
    print("\n[2] 失败重试后成功")
    broker = InMemoryBroker()
    worker = Worker(broker, concurrency=1, retry_base_delay=0.02)
    calls = {"n": 0}

    @worker.task("flaky")
    async def flaky(task: Task) -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            print(f"    第 {calls['n']} 次执行 -> 失败")
            raise RuntimeError("暂时性故障")
        print(f"    第 {calls['n']} 次执行 -> 成功")
        return "终于成功"

    task = await broker.enqueue(Task(name="flaky", max_retries=3))
    await worker.start()
    await wait_until(lambda: task.is_terminal)
    await worker.stop()
    print(f"    最终: status={task.status.value} attempts={task.attempts} result={task.result!r}")


async def demo_dead_letter() -> None:
    print("\n[3] 重试额度用尽 -> 死信队列")
    broker = InMemoryBroker()
    worker = Worker(broker, concurrency=1, retry_base_delay=0.01)

    @worker.task("always_broken")
    async def always_broken(task: Task) -> None:
        raise ValueError("这个 handler 就是坏的")

    task = await broker.enqueue(Task(name="always_broken", max_retries=2))
    await worker.start()
    await wait_until(lambda: task.is_terminal)
    await worker.stop()
    print(f"    最终: status={task.status.value} attempts={task.attempts}")
    print(f"    错误: {task.error}")
    print(f"    死信队列: {broker.dead_letters}")


async def demo_visibility_timeout() -> None:
    print("\n[4] worker 崩溃 -> 可见性超时 -> 重新投递")
    broker = InMemoryBroker(visibility_timeout=0.2)
    task = await broker.enqueue(Task(name="important", max_retries=2))

    first = await broker.dequeue()
    print(f"    worker A 领走 {first.id[:8]},此刻它还没 ack")
    print(f"    worker A 崩了(永远不会调用 ack)")
    print(f"    现在队列里有就绪任务吗? {await broker.dequeue()}")

    await asyncio.sleep(0.25)  # 等可见性超时

    second = await broker.dequeue()
    print(f"    可见性超时后:worker B 领到 {second.id[:8]}")
    print(f"    同一个任务吗? {second.id == task.id};attempts = {second.attempts}")
    await broker.ack(second.id, "B 干完了")
    print(f"    最终: status={task.status.value} result={task.result!r}")


async def main() -> None:
    await demo_priority()
    await demo_retry_then_success()
    await demo_dead_letter()
    await demo_visibility_timeout()
    print("\n全部场景结束。")


if __name__ == "__main__":
    asyncio.run(main())
