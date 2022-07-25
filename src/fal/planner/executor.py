import argparse
from email.headerregistry import Group
import warnings
from enum import Enum, auto
from concurrent.futures import (
    FIRST_COMPLETED,
    Executor,
    Future,
    ThreadPoolExecutor,
    wait,
)
from dataclasses import dataclass, field
from typing import Iterator, List, Optional

from fal.planner.schedule import SUCCESS, Scheduler
from fal.planner.tasks import TaskGroup, Task, Status, DBTTask
from faldbt.project import FalDbt

from dbt.logger import GLOBAL_LOGGER as logger


class State(Enum):
    PRE_HOOKS = auto()
    MAIN_TASK = auto()
    POST_HOOKS = auto()


# HACK: Since we construct multiprocessing pools for each DBT run, it leaves a trace
# of shared memory warnings behind. In reality, there isn't anything we can do to
# get rid of them since everything is closed properly and gets destroyed at the end.
# As of now, it is just a known problem of using multiprocessing like this, and for
# not spamming the users with these unrelated warnings we'll filter them out.
#
# See for more: https://stackoverflow.com/a/63004750
warnings.filterwarnings("ignore", category=UserWarning, module="multiprocessing.*")


def _collect_models(groups: List[TaskGroup]) -> Iterator[str]:
    for group in groups:
        if isinstance(group.task, DBTTask):
            yield from group.task.model_ids


def _show_failed_groups(scheduler: Scheduler) -> None:
    failed_models = list(_collect_models(scheduler.filter_groups(Status.FAILURE)))
    if failed_models:
        message = ", ".join(failed_models)
        logger.info("Failed calculating the following DBT models: {}", message)

    skipped_models = list(_collect_models(scheduler.filter_groups(Status.SKIPPED)))
    if skipped_models:
        message = ", ".join(skipped_models)
        logger.info("Skipped calculating the following DBT models: {}", message)


@dataclass
class FutureGroup:
    args: argparse.Namespace
    fal_dbt: FalDbt
    task_group: TaskGroup
    executor: Executor
    futures: List[Future] = field(default_factory=list)
    status: int = SUCCESS
    state: Optional[State] = None

    def __post_init__(self) -> None:
        if self.task_group.pre_hooks:
            self.switch_to(State.PRE_HOOKS)
        else:
            self.switch_to(State.MAIN_TASK)

    def switch_to(self, group_status: State) -> None:
        self.state = group_status
        if group_status is State.PRE_HOOKS:
            self._add_tasks(*self.task_group.pre_hooks)
        elif group_status is State.MAIN_TASK:
            self._add_tasks(self.task_group.task)
        else:
            assert group_status is State.POST_HOOKS
            self._add_tasks(*self.task_group.post_hooks)

    def process(self, future: Future) -> None:
        assert future in self.futures
        self.futures.remove(future)
        self.status |= future.result()

        if self.futures:
            return None

        if self.state is State.PRE_HOOKS:
            # If there are no tasks left and the previous group was pre-hooks,
            # we'll decide based on the exit status (success => move to the main task,
            # failure => run the post-hooks and then exit).
            if self.status == SUCCESS:
                self.switch_to(State.MAIN_TASK)
            else:
                self.switch_to(State.POST_HOOKS)
        elif self.state is State.MAIN_TASK:
            # If we just executed the main task, then we'll proceed to the post-hooks
            self.switch_to(State.POST_HOOKS)
        else:
            # If we just executed post-hooks and there are no more tasks left,
            # we'll just exit ¯\_(ツ)_/¯
            assert self.state is State.POST_HOOKS
            return None

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
    args: argparse.Namespace,
    fal_dbt: FalDbt,
    scheduler: Scheduler,
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
                task_group=task_group,
                executor=executor,
            )
            for task_group in scheduler.iter_available_groups()
        ]

    with ThreadPoolExecutor(fal_dbt.threads) as executor:
        future_groups = create_futures(executor)
        futures = get_futures(future_groups)
        while futures:
            # Get the first completed futures, mark them done.
            completed_futures, _ = wait(futures, return_when=FIRST_COMPLETED)
            for future in completed_futures:
                group: FutureGroup = future.group
                group.process(future)
                if group.is_done:
                    scheduler.finish(group.task_group, status=group.status)

            # And load all the tasks that were blocked by those futures.
            future_groups.extend(create_futures(executor))
            futures = get_futures(future_groups)

    _show_failed_groups(scheduler)
