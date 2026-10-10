"""MVP 的行为测试。只用标准库,所以不需要装任何东西:

    python3 -m unittest discover -s tests -t tests -v

每个测试都对应一条"系统必须保证的语义",而不是对应一个函数。
这些断言就是你后面改实现时的护栏 —— 尤其是可见性超时那条,
它是"至少一次投递"的唯一守卫。
"""

from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from jobqueue import (
    InMemoryBroker,
    InvalidTransition,
    Task,
    TaskPriority,
    TaskStatus,
    Worker,
)


class StatusMachineTests(unittest.TestCase):
    def test_illegal_transition_is_rejected(self) -> None:
        task = Task(name="x")
        self.assertIs(task.status, TaskStatus.QUEUED)
        # QUEUED 不能直接跳到 SUCCESS,必须先经过 RUNNING
        with self.assertRaises(InvalidTransition):
            task.transition_to(TaskStatus.SUCCESS)

    def test_terminal_states_have_no_exit(self) -> None:
        task = Task(name="x")
        task.transition_to(TaskStatus.RUNNING)
        task.transition_to(TaskStatus.SUCCESS)
        self.assertTrue(task.is_terminal)
        with self.assertRaises(InvalidTransition):
            task.transition_to(TaskStatus.QUEUED)


class BrokerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.broker = InMemoryBroker(visibility_timeout=0.05)

    async def test_priority_order(self) -> None:
        for label, prio in [
            ("low", TaskPriority.LOW),
            ("critical", TaskPriority.CRITICAL),
            ("normal", TaskPriority.NORMAL),
            ("high", TaskPriority.HIGH),
        ]:
            await self.broker.enqueue(
                Task(name="x", payload={"label": label}, priority=prio)
            )

        order = []
        while (task := await self.broker.dequeue()) is not None:
            order.append(task.payload["label"])
            await self.broker.ack(task.id)

        self.assertEqual(order, ["critical", "high", "normal", "low"])

    async def test_visibility_timeout_redelivers_same_task(self) -> None:
        task = await self.broker.enqueue(Task(name="x", max_retries=2))

        first = await self.broker.dequeue()
        self.assertEqual(first.attempts, 1)

        # 没 ack:任务在 inflight 里,不该被第二次领走
        self.assertIsNone(await self.broker.dequeue())

        await asyncio.sleep(0.08)  # 超过可见性超时

        second = await self.broker.dequeue()
        self.assertIsNotNone(second)
        self.assertEqual(second.id, task.id)
        self.assertEqual(second.attempts, 2)

    async def test_retry_requeues_then_succeeds(self) -> None:
        task = await self.broker.enqueue(Task(name="x", max_retries=2))

        await self.broker.dequeue()
        await self.broker.fail(task.id, "boom")
        self.assertIs(task.status, TaskStatus.QUEUED)

        redelivered = await self.broker.dequeue()
        self.assertEqual(redelivered.attempts, 2)
        await self.broker.ack(redelivered.id, "ok")
        self.assertIs(task.status, TaskStatus.SUCCESS)
        self.assertEqual(task.result, "ok")

    async def test_exhausted_retries_land_in_dead_letter(self) -> None:
        task = await self.broker.enqueue(Task(name="x", max_retries=1))

        await self.broker.dequeue()  # attempts = 1
        await self.broker.fail(task.id, "boom")
        self.assertIs(task.status, TaskStatus.QUEUED)  # 1 > 1 为假,还能重试

        await self.broker.dequeue()  # attempts = 2
        await self.broker.fail(task.id, "boom")  # 2 > 1 为真
        self.assertIs(task.status, TaskStatus.FAILURE)
        self.assertEqual([t.id for t in self.broker.dead_letters], [task.id])

    async def test_crash_loop_ends_in_dead_letter_not_infinite(self) -> None:
        """一个每次都能把 worker 搞崩的任务,必须最终停下。"""
        task = await self.broker.enqueue(Task(name="x", max_retries=2))

        for _ in range(10):  # 反复"领取后崩溃"
            got = await self.broker.dequeue()
            if got is None:
                await asyncio.sleep(0.08)
                got = await self.broker.dequeue()
            if got is None:
                break

        self.assertIs(task.status, TaskStatus.FAILURE)
        self.assertEqual(len(self.broker.dead_letters), 1)

    async def test_delayed_enqueue_is_not_ready_immediately(self) -> None:
        await self.broker.enqueue(Task(name="x"), delay=0.05)
        self.assertIsNone(await self.broker.dequeue())
        await asyncio.sleep(0.08)
        self.assertIsNotNone(await self.broker.dequeue())


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def wait_until(self, predicate, timeout: float = 3.0) -> bool:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if predicate():
                return True
            await asyncio.sleep(0.005)
        return False

    async def test_end_to_end_retry_then_success(self) -> None:
        broker = InMemoryBroker()
        worker = Worker(broker, concurrency=2, retry_base_delay=0.01)
        calls = {"n": 0}

        @worker.task("flaky")
        async def flaky(task: Task) -> int:
            calls["n"] += 1
            if calls["n"] < 3:
                raise RuntimeError("暂时性故障")
            return 42

        task = await broker.enqueue(Task(name="flaky", max_retries=3))
        await worker.start()
        self.assertTrue(await self.wait_until(lambda: task.is_terminal))
        await worker.stop()

        self.assertIs(task.status, TaskStatus.SUCCESS)
        self.assertEqual(task.result, 42)
        self.assertEqual(task.attempts, 3)

    async def test_handler_timeout_counts_as_failure(self) -> None:
        broker = InMemoryBroker()
        worker = Worker(broker, concurrency=1)

        @worker.task("slow")
        async def slow(task: Task) -> None:
            await asyncio.sleep(1)

        task = await broker.enqueue(Task(name="slow", timeout=0.05, max_retries=0))
        await worker.start()
        self.assertTrue(await self.wait_until(lambda: task.is_terminal))
        await worker.stop()

        self.assertIs(task.status, TaskStatus.FAILURE)
        self.assertIn("TimeoutError", task.error or "")

    async def test_unknown_handler_goes_to_dead_letter(self) -> None:
        broker = InMemoryBroker()
        worker = Worker(broker, concurrency=1)

        @worker.task("known")
        async def known(task: Task) -> None:
            return None

        task = await broker.enqueue(Task(name="typo_name", max_retries=0))
        await worker.start()
        self.assertTrue(await self.wait_until(lambda: task.is_terminal))
        await worker.stop()

        self.assertIs(task.status, TaskStatus.FAILURE)
        self.assertIn("没有注册", task.error or "")

    async def test_graceful_stop_finishes_inflight_task(self) -> None:
        """stop() 不该打断正在执行的任务。"""
        broker = InMemoryBroker()
        worker = Worker(broker, concurrency=1)
        finished = {"ok": False}

        @worker.task("slowish")
        async def slowish(task: Task) -> str:
            await asyncio.sleep(0.05)
            finished["ok"] = True
            return "done"

        task = await broker.enqueue(Task(name="slowish"))
        await worker.start()
        # 等它真正进入 RUNNING 再停,否则可能在"还没领到"的时候就把 worker 停了
        self.assertTrue(
            await self.wait_until(lambda: task.status is TaskStatus.RUNNING)
        )
        await worker.stop()

        self.assertTrue(finished["ok"])
        self.assertIs(task.status, TaskStatus.SUCCESS)


if __name__ == "__main__":
    unittest.main()
