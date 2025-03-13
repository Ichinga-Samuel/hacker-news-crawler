import asyncio
import time
import random
from typing import Coroutine, Callable, Literal
from logging import getLogger
from signal import SIGINT, signal

# from check import worker

logger = getLogger(__name__)


class QueueItem:
    def __init__(self, task_item: Callable | Coroutine, *args, **kwargs):
        self.task_item = task_item
        self.args = args
        self.kwargs = kwargs
        self.must_complete = False
        self.time = time.time_ns()
        self.timeout = None

    def __repr__(self):
        name = getattr(self.task_item, "__name__", str(self.task_item))
        return f"{self.__class__.__name__}({name})"

    def __hash__(self):
        return id(self)

    def __lt__(self, other):
        return self.time < other.time

    async def __call__(self, timeout: float = None):
        """Run the task asynchronously"""
        start = time.perf_counter()
        try:
            async with asyncio.timeout(timeout or self.timeout):
                if asyncio.iscoroutinefunction(self.task_item):
                    return await self.task_item(*self.args, **self.kwargs)
                else:
                    return await asyncio.to_thread(self.task_item, *self.args, **self.kwargs)

        except asyncio.TimeoutError:
            logger.error("Task %s timed out after %d seconds", self, time.perf_counter() - start)

        except asyncio.CancelledError:
            logger.debug("Task %s was cancelled",self)

        except Exception as err:
            logger.error("Error %s occurred in %s",err, self)

    def set_attributes(self, **kwargs):
        [setattr(self, k, v) for k, v in kwargs.items()]


class TaskQueue:
    queue_task: asyncio.Task
    start_time: float
    _loop: asyncio.AbstractEventLoop

    def __init__(self, *, size: int = 0, workers: int = 10, queue: asyncio.Queue = None, queue_timeout: int = None,
                 on_exit: Literal['cancel', 'complete_priority'] = 'complete_priority', absolute_timeout: int = None,
                 mode: Literal['finite', 'infinite'] = 'finite', task_timeout: int = None):
        self.timed_out = False
        self.queue = queue or asyncio.PriorityQueue(maxsize=size)
        self.workers = workers
        self.worker_tasks = {}
        self.queue_timeout = queue_timeout
        self.absolute_timeout = absolute_timeout
        self.stop = False
        self.on_exit = on_exit
        self.mode = mode
        self.queue_task_cancelled = False
        self.start_time = time.perf_counter()
        self.task_timeout: float | None = None
        signal(SIGINT, self.sigint_handle)

    def add(self, *, item: QueueItem, priority=1, must_complete=False, timeout=0):
        """Add a task to the queue.

        Args:
            item (QueueItem): The task to add to the queue.
            priority (int): The priority of the task. Default is 3.
            must_complete (bool): A flag to indicate if the task must complete before the queue stops. Default is False.
            timeout (int): The maximum time to run the task. Default is None.
        """
        try:
            if self.stop:
                return
            attrs = {'must_complete': must_complete, "timeout": timeout or self.task_timeout}
            item.set_attributes(**attrs)
            if isinstance(self.queue, asyncio.PriorityQueue):
                item = (priority, item)
            self.queue.put_nowait(item)
        except asyncio.QueueFull:
            logger.error("Queue is full")

    async def worker(self, wid: int = None):
        """Worker function to run tasks in the queue."""
        while True:
            try:
                if not await self.check_timeout():
                    break
                #
                # if self.stop and self.queue.qsize() == 0:
                #     logger.warning('calling cancel')
                #     break

                if self.mode == 'infinite' and self.queue.qsize() <= 1 and self.stop is False:
                    dummy = QueueItem(self.dummy_task)
                    self.add(item=dummy)

                if isinstance(self.queue, asyncio.PriorityQueue):
                    _, item = self.queue.get_nowait()

                else:
                    item = self.queue.get_nowait()

                if self.stop is False or (item.must_complete and self.on_exit == 'complete_priority'):
                    await item()

                self.queue.task_done()
                    # self.cancel()

                await self.add_workers()

            except asyncio.QueueEmpty:
                if self.stop or self.mode == "finite":
                    break

            # except asyncio.CancelledError:
            #     break

            except Exception as err:
                logger.error("%s: Error occurred in worker", err)
                break

    def start_timer(self, *, queue_timeout: int = None, absolute_timeout: int = None, start=False):
        self.queue_timeout = queue_timeout or self.queue_timeout
        self.absolute_timeout = absolute_timeout or self.absolute_timeout
        print('start timer', self.absolute_timeout, self.queue_timeout, self.task_timeout)
        if start:
            self.start_time = time.perf_counter()

    async def check_timeout(self):
        res = True
        if self.absolute_timeout is None and self.queue_timeout is None:
            return True
        if self.timed_out:
            return False
        if self.queue_timeout and (time.perf_counter() - self.start_time) > self.queue_timeout:
            self.stop = True
            res = True
            self.queue_timeout = False
        if self.absolute_timeout and (time.perf_counter() - self.start_time) > self.absolute_timeout:
            print(self.absolute_timeout, 'absolute timeout')
            self.stop = True
            self.timed_out = True
            await self.cancel()
            res = False
        return res

    async def dummy_task(self):
        await asyncio.sleep(1)

    def remove_worker(self, wid: int):
        try:
            print('removing worker', wid)
            task = self.worker_tasks.pop(wid, None)
            if task is not None:
                print('removing worker', wid)
                task.cancel()
        except asyncio.CancelledError as _:
            print("error canceling worker", wid)
        except Exception as err:
            logger.debug("%s: Error occurred in removing worker %d", err, wid)

    async def add_workers(self, no_of_workers: int = None):
        """Create workers for running queue tasks."""
        if no_of_workers is None:
            queue_size = self.queue.qsize()
            # if size of queue greater than number of workers add more workers
            req_workers = queue_size - len(self.worker_tasks)
            if req_workers > 1:
                no_of_workers = req_workers
            else:
                return

        ri = lambda : random.randint(999, 999_999_999) # random id
        ct = lambda ti: asyncio.create_task(self.worker(wid=ti), name=ti) # create task
        wr = range(no_of_workers)
        [self.worker_tasks.setdefault(wi:=ri(), ct(wi)) for _ in wr]

    async def run(self, queue_timeout: int = None, absolute_timeout: int = None):
        """Run the queue until all tasks are completed or the timeout is reached.

        Args:
            queue_timeout (int): The maximum time to wait for the queue to complete. Default is 0.
            absolute_timeout (int): The maximum time to run the queue.
            This timeout overrides the timeout attribute of the queue instance.
            The queue stops when the timeout is reached, and the remaining tasks are handled based on the
            `on_exit` attribute. If the timeout is 0, the queue will run until all tasks are completed or the queue
            is stopped.
        """
        try:
            self._loop = asyncio.get_running_loop()
            await self.add_workers(no_of_workers=self.workers)
            self.start_timer(queue_timeout=queue_timeout, absolute_timeout=absolute_timeout, start=True)
            # async with asyncio.timeout(self.absolute_timeout):
            #     await self.queue.join()

            # async with asyncio.timeout_at(self.task_timeout):
            # await self.queue.join()

            self.queue_task = asyncio.create_task(self.queue.join())
            # self.worker_tasks[0] = self.queue_task
            await self.queue_task
        except asyncio.TimeoutError:
            logger.warning("Timeout occurred after %d seconds, %d tasks remaining",
                           time.perf_counter() - self.start_time, self.queue.qsize())
            # self.cancel()

        except asyncio.CancelledError:
            logger.warning("Task Queue Cancelled after %d seconds, %d tasks remaining",
                           time.perf_counter() - self.start_time, self.queue.qsize())
            print(len(self.worker_tasks), 'workers remaining after cancellation of queue')
            await self.cancel()

        except Exception as err:
            logger.warning("%s occurred after %d seconds, %d tasks remaining",
                           err, time.perf_counter() - self.start_time, self.queue.qsize())
        finally:
            logger.info("Tasks completed after %d seconds, %d tasks remaining",
                           time.perf_counter() - self.start_time, self.queue.qsize())

    async def cancel_all_workers(self):
        try:
            print('canceling all workers')
            wids = list(self.worker_tasks.keys())
            for task in self.worker_tasks.values():
                task.cancel()

            # await asyncio.gather(*self.worker_tasks.values(), return_exceptions=True)
            self.worker_tasks.clear()
            # [self.remove_worker(wid) for wid in wids]
        # except asyncio.CancelledError as err:
        #     print('error cancelling all workers', err)
        except Exception as err:
            print(err, 'in cancel all workers')
            logger.error("%s: Error occurred in cancelling workers", err)

    async def cancel(self):
        try:
            # if self.queue_task_cancelled:
            #     return
            print("calling cancel")
            self.stop = True
            await self.cancel_all_workers()
            self.queue_task.cancel()
            # await self.queue_task
            print("task cancelled")
            self.queue_task_cancelled = True
            # await self.cancel_all_workers()
            print('cancelled complete')
        # except asyncio.CancelledError as err:
        #     print('cancelled error in cancel', err)
        except Exception as err:
            print('Error occurred in cancelling queue')
            logger.error("%s: Error occurred in cancelling queue", err)

    def sigint_handle(self, sig, frame):
        logger.warning("Canceling all tasks")
        self.stop = True
        self.cancel()
