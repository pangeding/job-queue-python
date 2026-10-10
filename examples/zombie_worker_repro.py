"""复现:僵尸 worker 的迟到上报会覆盖新一次投递的结果。

    python3 examples/zombie_worker_repro.py

场景:worker A 领走任务后卡住(GC、长暂停、网络分区都行)
  -> 可见性超时,任务被重新投递
  -> worker B 领走并开始执行
  -> A 突然醒来,上报它那份结果

现在它实际怎么反应?跑一下看。然后回答文件末尾那三个问题。
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from jobqueue import InMemoryBroker, Task


async def main() -> None:
    broker = InMemoryBroker(visibility_timeout=0.05)
    task = await broker.enqueue(Task(name="transfer_money", max_retries=5))

    a = await broker.dequeue()
    print(f"worker A 领走   attempts={a.attempts}  status={task.status.value}")

    await asyncio.sleep(0.08)  # A 卡住,超过可见性超时
    b = await broker.dequeue()
    print(
        f"worker B 领走   attempts={b.attempts}  status={task.status.value}"
        f"  同一任务? {b.id == task.id}"
    )

    # A 醒来上报。注意:它手里那张"凭证"和 B 手里的没有任何区别。
    await broker.ack(a.id, "A 的结果")
    print(
        f"A 上报之后      status={task.status.value}  result={task.result!r}"
        f"   <- 此刻 B 还在执行"
    )

    try:
        await broker.ack(b.id, "B 的结果")
        print(f"B 上报之后      status={task.status.value}  result={task.result!r}")
    except Exception as exc:
        print(f"B 上报时崩了    {type(exc).__name__}: {exc}")

    print(f"最终           status={task.status.value}  result={task.result!r}")
    print()
    print("问自己三个问题:")
    print("  1. 同一个任务被两个 worker 同时执行、各自都以为自己成功了 ——")
    print("     这在'转账'这种业务上意味着什么?谁该为重复副作用负责?")
    print("  2. B 的 ack 抛出的异常,在 Worker._execute 里会传播到哪里?")
    print("     那个异常之后,worker 槽还活着吗?这是不是让故障放大了?")
    print("  3. 要让 A 的迟到上报被拒绝,broker 必须多记住什么?")
    print("     (关键词:delivery token / fencing —— 先别查,自己想 10 分钟)")


if __name__ == "__main__":
    asyncio.run(main())
