# проверка концепта
# в экземпляр класса AsyncioLS добавляем задания (функции или корутины)
# при создании экземпляра указываем количество паралельных выполнений

# Принцип указания заданий
#   - каждому заданию можно задать параметры args, kwargs
#   - результат предыдущего задания подается на вход следующему заданию (если результат был, если None то след задание без параметров)
#   - если результат возвр одно значение а на вход след. нужно два, то второй параметр можно задать при добавлении задания (пример mul)

import asyncio

from inspect import iscoroutinefunction

class AsyncioLS():
    def __init__(self, num_of_parallel_runs=3):
        self.sem = asyncio.BoundedSemaphore(num_of_parallel_runs)
        self.futures = []
        self.tasklist = []


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
        self.futures = [asyncio.ensure_future(self._sem_main_task(*a)) for a in args_for_first_coro]

        for i, future in enumerate(asyncio.as_completed(self.futures), 1):
            try:
                result = await future # здесь в result кортеж (orderno, guid)
                print(f"coro #{i}: result:{result}")
            except Exception as e:
                print("future exception:", str(e))


    def start_event_loop(self, *args_for_first_coro):
        self.loop = asyncio.get_event_loop()
        try:
            self.loop.run_until_complete(self.main(*args_for_first_coro))
        finally:
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            self.loop.close()



def add(x, y):
    return x + y

def mul(x, y):
    return x * y

def div(x, y):
    return x // y

from random import randint
async def wait(a):
    s = randint(2,5)
    print (f'res:{a}, sleep for {s} secs')
    await asyncio.sleep(s)
    return a

def pr(a):
    print(a)
    return a

if __name__ == "__main__":
    a = AsyncioLS()
    # первая коро без параметров, потому что они придут при вызове _main_task
    a.addtask(add)
    # вторая берет результат из первой
    a.addtask(mul, 30)
    a.addtask(div, 3)
    a.addtask(wait)
    # a.addtask(pr)

    # здесь *[(i, i) for i in range(10)] это СПИСОК параметров для первого задания
    # итого будет 10 футур(запусков main_task) c параметрами от 0 до 9
    a.start_event_loop(*[(i, i) for i in range(10)])
