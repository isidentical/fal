from __future__ import annotations

import argparse
from concurrent.futures import (
    FIRST_COMPLETED,
    Executor,
    Future,
    ThreadPoolExecutor,
    wait,
)
from dataclasses import dataclass, field
from typing import List

from fal.planner.schedule import SUCCESS, NodeQueue
from fal.planner.tasks import FalHookTask, Node, Task
from faldbt.project import FalDbt

N_THREADS = 5


def serial_executor(queue: NodeQueue) -> None:
    while node := next(queue.iter_available_nodes(), None):

        status = node.task.execute()
        for hook in node.post_hooks:
            status |= hook.execute()

        queue.finish(node, status=status)


@dataclass
class FutureGroup:
    args: argparse.Namespace
    fal_dbt: FalDbt
    node: Node
    executor: Executor
    futures: list[Future] = field(default_factory=list)
    status: int = SUCCESS

    def __post_init__(self) -> None:
        # In the case of us having pre-hooks, this is
        # where we'll trigger them (and handle the node.task)
        # below.
        self._add_tasks(self.node.task)

    def process(self, future: Future) -> None:
        assert future in self.futures

        self.futures.remove(future)
        self.status |= future.result()
        if not isinstance(future.task, FalHookTask):
            # Once the main task gets completed, we'll populate
            # the task queue with the post-hooks.
            self._add_tasks(*self.node.post_hooks)

    def _add_tasks(self, *tasks: Task) -> None:
        for task in tasks:
            future = self.executor.submit(
                task.execute,
                args=self.args,
                fal_dbt=self.fal_dbt,
            )
            future.task, future.group = task, self
            self.futures.append(future)

    @property
    def is_done(self) -> int:
        return len(self.futures) == 0


def parallel_executor(
    args: argparse.Namespace, fal_dbt: FalDbt, queue: NodeQueue
) -> None:
    def get_futures(future_groups):
        return {
            # Unpack all running futures into a single set
            # to be consumed by wait().
            future
            for future_group in future_groups
            for future in future_group.futures
        }

    def create_futures(executor: ThreadPoolExecutor) -> List[FutureGroup]:
        return [
            # FutureGroup's are the secondary layer of the executor,
            # managing the parallelization of tasks.
            FutureGroup(
                args,
                fal_dbt,
                node=node,
                executor=executor,
            )
            for node in queue.iter_available_nodes()
        ]

    with ThreadPoolExecutor(max_workers=N_THREADS) as executor:
        future_groups = create_futures(executor)

        while futures := get_futures(future_groups):
            # Get the first completed futures, mark them done.
            completed_futures, _ = wait(futures, return_when=FIRST_COMPLETED)
            for future in completed_futures:
                group = future.group
                group.process(future)
                if group.is_done:
                    queue.finish(group.node, status=group.status)

            # And load all the tasks that were blocked by those futures.
            future_groups.extend(create_futures(executor))
