import asyncio
import inspect
from contextlib import suppress
from threading import Timer


class PeriodicAsync:
    '''
    For executing some function periodically.
    Note! If you're planning to call some long-running func, you possibly would need to run it in executor

    eg. usage:

        async def main():
            p = Periodic(lambda: print('test'), 1)
            try:
                print('Start')
                await p.start()
                await asyncio.sleep(3.1)

                print('Stop')
                await p.stop()
                await asyncio.sleep(3.1)

                print('Start')
                await p.start()
                await asyncio.sleep(3.1)
            finally:
                await p.stop()  # we should stop task finally


        if __name__ == '__main__':
            loop = asyncio.get_event_loop()
            loop.run_until_complete(main())


        Output:

            Start
            test
            test
            test

            Stop

            Start
            test
            test
            test

            [Finished in 9.5s]
    '''

    def __init__(self, interval, func):
        self.func = func
        self.interval = interval
        self.is_started = False
        self._task = None

    async def start(self):
        if not self.is_started:
            self.is_started = True
            # Start task to call func periodically:
            self._task = asyncio.ensure_future(self._run())

    async def stop(self):
        if self.is_started:
            self.is_started = False
            # Stop task and await it stopped:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def _run(self):
        while True:
            # Supporting normal and async functions
            res = self.func()
            if inspect.isawaitable(res):
                await res
            await asyncio.sleep(self.interval)


class PeriodicNormal(object):
    def __init__(self, interval, func, *args, **kwargs):
        self._timer = None
        self.interval = interval
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.is_started = False
        self.start()  # It starts itself!

    def start(self):
        if not self.is_started:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False

    def _run(self):
        self.is_started = False
        self.start()
        self.func(*self.args, **self.kwargs)