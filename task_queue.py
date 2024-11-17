import asyncio
import time
import random
from typing import Coroutine, Callable, Literal
from logging import getLogger
from signal import SIGINT, signal

logger = getLogger(__name__)


class QueueItem:
    must_complete: bool

    def __init__(self, task_item: Callable | Coroutine, *args, **kwargs):
        self.task_item = task_item
        self.args = args
        self.kwargs = kwargs
        self.must_complete = False
        self.time = int(time.monotonic_ns())

    def __hash__(self):
        return id(self)

    def __lt__(self, other):
        return self.time < other.time

    async def run(self):
        try:
            if asyncio.iscoroutinefunction(self.task_item):
                await self.task_item(*self.args, **self.kwargs)
            else:
                await asyncio.to_thread(self.task_item, *self.args, **self.kwargs)
        except Exception as err:
            logger.error("Error %s occurred in %s with args %s and %s",
                         err, self.task_item.__name__, self.args, self.kwargs)


class TaskQueue:
    def __init__(self, size: int = 0, workers: int = 500, timeout: int = None, queue: asyncio.Queue = None,
                 on_exit: Literal['cancel', 'complete_priority'] = 'complete_priority',
                 mode: Literal['finite', 'infinite'] = 'finite', worker_timeout: int = 60):

        self.queue = queue or asyncio.PriorityQueue(maxsize=size)
        self.workers = workers
        self.tasks = {}
        self.priority_tasks = set()  # tasks that must complete
        self.timeout = timeout
        self.stop = False
        self.on_exit = on_exit
        self.mode = mode
        self.worker_timeout = worker_timeout
        signal(SIGINT, self.sigint_handle)

    def add(self, *, item: QueueItem, priority=3, must_complete=False):
        try:
            if self.stop:
                return
            item.must_complete = must_complete
            self.priority_tasks.add(item) if item.must_complete else ...
            if isinstance(self.queue, asyncio.PriorityQueue):
                item = (priority, item)
            self.queue.put_nowait(item)
        except asyncio.QueueFull:
            logger.error("Queue is full")

    def spin_workers(self, workers: int = 0):
        try:
            if workers <= 0:
                extra_workers = len([task for task in self.tasks.values() if task.done() is False]) - 1
                qsize =  self.queue.qsize()
                workers = extra_workers - qsize
                if workers <= 100:
                    return

            for i in range(workers):
                wid = random.getrandbits(32)
                worker = asyncio.create_task(self.worker(wid=f"worker_{wid}"))
                self.tasks[f"worker_{wid}"] = worker
        except Exception as err:
            logger.error("%s: Error occurred in spin_workers", err)

    async def worker(self, wid: str = ''):
        while True:
            try:
                if isinstance(self.queue, asyncio.PriorityQueue):
                    _, item = self.queue.get_nowait()

                else:
                    item = self.queue.get_nowait()

                if self.stop is False or item.must_complete:
                    await item.run()

                self.queue.task_done()
                self.priority_tasks.discard(item)

                if self.stop and (self.on_exit == 'cancel' or len(self.priority_tasks) == 0):
                    self.cancel()
                    break

                if self.mode == 'infinite':
                    self.spin_workers()

            except asyncio.QueueEmpty:
                if self.stop:
                    break

                if self.mode == 'finite':
                    break

                sleep = QueueItem(asyncio.sleep, 1)
                self.add(item=sleep)
                await asyncio.sleep(self.worker_timeout)
            except Exception as err:
                logger.error("%s: Error occurred in worker", err)
                break

    async def run(self, timeout: int = 0):
        start = time.perf_counter()
        try:
            workers = {f"worker_{i}": asyncio.create_task(self.worker(wid=f"worker_{i}")) for i in range(self.workers)}
            self.tasks |= workers
            queue_task = asyncio.create_task(self.queue.join())
            self.tasks['queue_task'] = queue_task
            timeout = timeout or self.timeout
            if timeout:
                await asyncio.wait_for(queue_task, timeout=timeout)
                self.stop = True
            else:
                await queue_task

        except TimeoutError:
            print("dfdfdfdfdf erkldfld  klklsdkld  kwkldl  kkldl")
            logger.warning("Timed out after %d seconds, %d tasks remaining",
                           time.perf_counter() - start, self.queue.qsize())
            self.stop = True

        except asyncio.CancelledError:
            logger.warning("Task was cancelled? what happened?")
            self.stop = True

        except Exception as err:
            logger.warning("%s: An error occurred in %s.run", err, self.__class__.__name__)
            self.stop = True

        finally:
            await self.clean_up()

    async def clean_up(self):
        try:
            print('cleaning up tasks...')
            if self.on_exit == 'complete_priority' and (pt := len(self.priority_tasks)) > 0:
                logger.info(f'Completing {pt} priority tasks...')
                self.spin_workers(workers=pt)
                queue_task = asyncio.create_task(self.queue.join())
                self.tasks['queue_task'] = queue_task
                await queue_task

            logger.info('Cleaning up tasks done...')
            self.cancel()

        except asyncio.CancelledError:
            logger.warning("Task was cancelled? why though?")
            self.stop = True

        except Exception as err:
            # logger.warning("Task was cancelled? why though? an exception occurred")
            logger.error(f"%s: Error occurred in %s", err, self.__class__.__name__)

        finally:
            self.cancel()

    def cancel(self):
        try:
            if (size := len(self.tasks)) == 0:
                return
            print(f'canceling all {size} tasks...')
            queue_task = self.tasks.pop('queue_task', None)
            queue_task.cancel() if queue_task is not None else ...
            for task in self.tasks.values():
                try:
                    task.cancel() if task is not None and task.done() is False else ...
                except asyncio.CancelledError:
                    logger.warning("Task was cancelled? why though? in canceling task")
                except Exception as err:
                    logger.error("%s: occurred in canceling task", err)
        except Exception as err:
            logger.error("%s: occurred in canceling all tasks", err)

        finally:
            self.tasks.clear()

    def sigint_handle(self, sig, frame):
        logger.info('SIGINT received, cleaning up...')
        if self.on_exit == 'complete_priority':
            self.stop = True
        else:
            self.cancel()
        self.on_exit = 'cancel'  # force cancel on exit if SIGINT is received again
