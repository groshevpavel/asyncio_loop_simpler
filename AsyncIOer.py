import asyncio

from inspect import iscoroutinefunction


from time import time

class AsyncIOer():
    def __init__(self, num_of_parallel_runs=3):
        self.sem = asyncio.BoundedSemaphore(num_of_parallel_runs)
        self.futures = []
        self.tasklist = []
        self.loops = []


    def func2async(self, funcname):
        """convert function to coroutine"""
        if isinstance(funcname, str):
            if funcname in globals():
                funcname = globals()[funcname]

        if callable(funcname):
            return asyncio.coroutine(funcname)


    def addtask(self, func, *args, **kwargs):
        """add task to _main_task; func is function(will turn into coro) or coroutine;
        args, kwargs is args, kwargs for coroutine
        """
        if not iscoroutinefunction(func):
            async_f = self.func2async(func)
        else:
            async_f = func

        _t = tuple([async_f, args, kwargs])
        self.tasklist.append(_t)


    async def _main_task(self, *main_args, **main_kwargs):
        for index, _t in enumerate(self.tasklist):
            if len(_t) == 1:
                task, args, kwargs = _t, None, None
            else:
                task, args, kwargs = _t

            if index == 0: # for first coro call it with main_args, main_kwargs
                # print(index, main_args, main_kwargs)
                res = await task(*main_args, **main_kwargs)
            else:
                if res is not None:
                    if not isinstance(res, (list, tuple, set,)):
                        res = [res]

                    if args:
                        if isinstance(res, tuple):
                            res = list(res)
                        if isinstance(args, tuple):
                            args = list(args)

                        if kwargs:
                            res = await task(*res + args, **kwargs)
                        else:
                            res = await task(*res + args)
                    else: # if no args
                        res = await task(*res)

                else: # if res is None
                    if args or kwargs:
                        res = await task(*args, **kwargs)
        return res

    async def _sem_main_task(self, *main_args, **main_kwargs):
        """wraps main_task with semaphore restriction"""
        async with self.sem:
            return await self._main_task(*main_args, **main_kwargs)

    async def main(self, *args_for_first_coro):
        """main will start from start_event_loop as heart of asyncio loop"""
        print("main started: ", int(time()))

        self.futures = [asyncio.ensure_future(self._sem_main_task(*a)) for a in args_for_first_coro]

        for i, future in enumerate(asyncio.as_completed(self.futures), 1):
            try:
                result = await future # здесь в result кортеж (orderno, guid)
                print(f"coro #{i}: result:{result}")
            except Exception as e:
                print("future exception:", str(e))


    def start_event_loop(self, *args_for_first_coro):
        if self.loops:
            loop = self.loops[-1]
        else:
            loop = asyncio.get_event_loop()

        if loop.is_closed() or loop.is_running():
            loop =  asyncio.new_event_loop()

        if loop not in self.loops:
            self.loops.append(loop)

        loop.run_until_complete(self.main(*args_for_first_coro))
        # try:
        #     loop.run_until_complete(self.main(*args_for_first_coro))
        # finally:
        #     loop.run_until_complete(loop.shutdown_asyncgens())
        #     loop.close()



def add(x, y):
    return x + y

def mul(x, y):
    return x * y

def div(x, y):
    return x // y

from random import randint
async def wait(a):
    s = randint(1, 3)
    print (f'res:{a}, sleep for {s} secs')
    await asyncio.sleep(s)
    return a

def pr(a):
    print(a)
    return a

if __name__ == "__main__":
    a = AsyncIOer()
    # первая коро без параметров, потому что они придут при вызове _main_task
    a.addtask(add)
    # вторая берет результат из первой
    a.addtask(mul, 30)
    a.addtask(div, 3)
    a.addtask(wait)
    # a.addtask(pr)

    a.start_event_loop(*[(i, i) for i in range(3)])
    print("new loop: ", int(time()))
    a.start_event_loop(*[(i, i) for i in range(55, 59)])



# main started:  1570602162
# res:0, sleep for 1 secs
# res:20, sleep for 1 secs
# res:40, sleep for 3 secs
# coro #1: result:0
# coro #2: result:20
# coro #3: result:40
# new loop:  1570602165
# main started:  1570602165
# res:1100, sleep for 1 secs
# res:1120, sleep for 2 secs
# res:1140, sleep for 1 secs
# res:1160, sleep for 2 secs
# coro #1: result:1100
# coro #2: result:1140
# coro #3: result:1120
# coro #4: result:1160
