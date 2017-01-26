import asyncio

async def download(item):
    await asyncio.sleep(0.25)
    print(f'download: {item}')
    return item * 2

async def resize(item):
    await asyncio.sleep(0.2)
    print(f'resize: {item}')
    return item + 1


async def upload(item):
    await asyncio.sleep(0.25)
    print(f'upload: {item}')
    return item / 2


class ClosableAsyncQueue(asyncio.Queue):
    SENTINEL = object()
    async def close(self):
        await self.put(ClosableAsyncQueue.SENTINEL)

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            item = await self.get()
            if item is self.SENTINEL:
                raise StopAsyncIteration
            return item

    def __iter__(self):
        return self

    def __next__(self):
            item = self.get_nowait()
            if item is self.SENTINEL:
                raise StopIteration
            return item

class StoppableWorker():
    def __init__(self, func, in_queue, out_queue):
        self.func = func
        self.in_queue = in_queue
        self.out_queue = out_queue

    async def run(self):
        
        async for item in self.in_queue:
            try:
                result = await self.func(item)
                await self.out_queue.put(result)
            finally:
                self.in_queue.task_done()
        try:
            await self.out_queue.close()
        finally:
            self.in_queue.task_done()

download_queue = ClosableAsyncQueue(20)
resize_queue = ClosableAsyncQueue(10)
upload_queue = ClosableAsyncQueue(5)
done_queue = ClosableAsyncQueue(1000)

async def put_queue2(q):
    for i in range(20):
        print(f'put: {i}')
        await q.put(i)
    await q.close()

async def dump_queue(q):
    return list(q) 

async def wait_join():
    await download_queue.join(),
    await resize_queue.join(),
    await upload_queue.join(),
    r = [i async for i in done_queue]
    await asyncio.sleep(20)
    print(r)

    return r


tasks = asyncio.wait([
    put_queue2(download_queue),
    StoppableWorker(download, download_queue, resize_queue).run(),
    StoppableWorker(resize, resize_queue, upload_queue).run(),
    StoppableWorker(upload, upload_queue, done_queue).run(),
])

loop = asyncio.get_event_loop()
print(loop.get_debug())
loop.run_until_complete(tasks)


#if __name__ == '__main__':
#    loop = asyncio.get_event_loop()
#    q = asyncio.Queue()
#    import time
#    try:
#        asyncio.ensure_future(put_queue(q))
#        asyncio.ensure_future(stop_queue(q))
#        loop.run_forever()
#        print(f'end: {asyncio.get_event_loop().time()}')
#    finally:
#        loop.close()
