import asyncio
import threading
from typing import Any, Coroutine, Dict, List

__all__ = ["run_async"]


class RunThread(threading.Thread):
    def __init__(self, func, args, kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.result = None
        super().__init__()

    def run(self):
        asyncio.run(self.func(*self.args, **self.kwargs))


def run_async(func: Coroutine[Any], *args: List[Any], **kwargs: Dict[Any, Any]):
    """不同线程运行异步任务

    Args:
        func (Coroutine): _description_
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        thread = RunThread(func, args, kwargs)
        thread.start()
        thread.join()
    else:
        asyncio.run(func(*args, **kwargs))
